---
description: Signing providers (ENV/KMS), channel account lifecycle, encryption, and transaction signing flows
type: reference
subsystem: signing
areas: [signing, channel-accounts, kms, encryption]
status: current
updated: 2026-02-23
vault: docs/knowledge
---

# Signing & Channel Accounts Architecture

## High-Level Overview

The signing subsystem is split into two independent signing identities that serve different purposes:

- **Distribution account** — sponsors channel account creation, pays fee-bump fees, holds the operator's main Stellar account. Uses ENV or KMS provider.
- **Channel accounts** — serve as source accounts for built transactions, enabling concurrent submission. Always uses the CHANNEL_ACCOUNT provider backed by DB-encrypted keypairs.

```mermaid
flowchart TD
    subgraph "Configuration"
        Env["DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER\n(ENV or KMS)"]
        Passphrase["CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE"]
    end

    subgraph "Signing Clients at Startup"
        DistClient["DistributionAccountSignatureClient\n(ENV or KMS)"]
        ChAccClient["ChannelAccountSignatureClient\n(CHANNEL_ACCOUNT / DB)"]
    end

    subgraph "Transaction Paths"
        BuildTx["buildTransaction mutation\nBuildAndSignTransactionWithChannelAccount()"]
        FeeBump["createFeeBumpTransaction mutation\nWrapTransaction()"]
    end

    Env --> DistClient
    Passphrase --> ChAccClient

    DistClient -->|signs fee-bump outer tx| FeeBump
    DistClient -->|signs channel creation tx| Lifecycle["Channel Account Lifecycle\n(ensure/delete)"]
    ChAccClient -->|acquires + signs source account| BuildTx
    ChAccClient -->|signs channel creation tx| Lifecycle
```

The two clients are constructed at `cmd/serve.go` startup and injected into every service that needs them. The distribution client type is resolved by `cmd/utils/utils.go:SignatureClientResolver` based on `DISTRIBUTION_ACCOUNT_SIGNATURE_PROVIDER`. The channel account client is always `NewChannelAccountDBSignatureClient`.

---

## SignatureClient Interface

All three providers implement the same 4-method interface:

```go
type SignatureClient interface {
    NetworkPassphrase() string
    GetAccountPublicKey(ctx context.Context, opts ...int) (string, error)
    SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error)
    SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error)
}
```

The `opts ...int` variadic in `GetAccountPublicKey` is a channel-account-specific extension: when called with an integer, it sets `locked_until` to that many seconds from now (default: 60s). The ENV and KMS providers ignore this parameter.

```mermaid
flowchart TD
    I["SignatureClient\ninterface"]

    I --> ENV["envSignatureClient\nenv_signature_client.go"]
    I --> KMS["kmsSignatureClient\nkms_signature_client.go"]
    I --> ChAcc["channelAccountDBSignatureClient\nchannel_account_db_signature_client.go"]

    subgraph "ENV internals"
        ENV --> KP["In-memory keypair.Full\n(parsed once at startup)"]
    end

    subgraph "KMS internals"
        KMS --> DB1["keypairs table\n(DB-stored encrypted key)"]
        DB1 --> KMSAPI["AWS KMS Decrypt API\n(called every sign)"]
        KMSAPI --> KP2["keypair.Full\n(reconstructed per call)"]
    end

    subgraph "CHANNEL_ACCOUNT internals"
        ChAcc --> DB2["channel_accounts table\n(FOR UPDATE SKIP LOCKED)"]
        DB2 --> Enc["AES-128-GCM Decrypt\n(passphrase-based)"]
        Enc --> KP3["keypair.Full\n(reconstructed per sign)"]
    end
```

### Provider Comparison

| Aspect | ENV | KMS | CHANNEL_ACCOUNT |
|---|---|---|---|
| Key storage | Environment variable | AWS KMS + `keypairs` DB table | `channel_accounts` DB table |
| Key material in memory | Yes (at startup) | No (decrypted per call) | No (decrypted per sign) |
| Caching | Permanent (process lifetime) | None | None |
| `SignStellarFeeBumpTransaction` | Supported | Supported | `ErrNotImplemented` |
| `GetAccountPublicKey` behavior | Returns fixed public key | Returns fixed public key | Acquires a lock on an idle account |
| Intended for | Distribution account (simple ops) | Distribution account (production) | Channel accounts only |

### Sentinel Errors (`signature_client.go`)

| Error | When |
|---|---|
| `ErrInvalidTransaction` | `nil` transaction passed to any Sign method |
| `ErrNotImplemented` | `SignStellarFeeBumpTransaction` called on channel account client |
| `ErrInvalidSignatureClientType` | `SignatureClientResolver` receives unknown type string |

### `SignatureClientType` Enum

```go
EnvSignatureClientType            SignatureClientType = "ENV"
KMSSignatureClientType            SignatureClientType = "KMS"
ChannelAccountSignatureClientType SignatureClientType = "CHANNEL_ACCOUNT"
```

**Source files:** `internal/signing/signature_client.go`

---

## ENV Provider

The simplest provider. The private key is parsed once at startup from `DISTRIBUTION_ACCOUNT_PRIVATE_KEY` into a `keypair.Full` stored in struct memory.

```mermaid
flowchart LR
    Start["NewEnvSignatureClient(privateKey, networkPassphrase)"]
    Start --> Parse["keypair.ParseFull(privateKey)\n→ keypair.Full stored in struct"]

    Sign["SignStellarTransaction()"]
    Sign --> Validate["Verify stellarAccounts == distributionAccount"]
    Validate --> TxSign["tx.Sign(networkPassphrase, keypairFull)"]
    TxSign --> Return["Return signed *txnbuild.Transaction"]
```

Key behaviors:
- `SignStellarTransaction` rejects any `stellarAccounts` that aren't the distribution account — prevents misuse as a general-purpose signer.
- No DB access, no network calls — all signing is purely in-memory.
- `GetAccountPublicKey` returns `distributionAccountFull.Address()` — trivial, always succeeds.

**Source file:** `internal/signing/env_signature_client.go`

---

## KMS Provider

Production-grade provider. The private key is stored encrypted in the `keypairs` table; AWS KMS decrypts it on every signing call. There is **no caching** — each `SignStellarTransaction` or `SignStellarFeeBumpTransaction` call reads from DB and calls the KMS API.

```mermaid
sequenceDiagram
    participant Caller
    participant kmsSignatureClient
    participant keypairsDB as keypairs table
    participant KMSAPI as AWS KMS

    Caller->>kmsSignatureClient: SignStellarTransaction(tx, accounts)
    kmsSignatureClient->>kmsSignatureClient: validate accounts == distributionAccount
    kmsSignatureClient->>keypairsDB: GetByPublicKey(distributionAccountPublicKey)
    keypairsDB-->>kmsSignatureClient: Keypair{EncryptedPrivateKey: []byte}
    kmsSignatureClient->>KMSAPI: Decrypt(ciphertextBlob, encryptionContext, keyId)
    Note over KMSAPI: EncryptionContext: {"pubkey": publicKey}
    KMSAPI-->>kmsSignatureClient: plaintext private key
    kmsSignatureClient->>kmsSignatureClient: keypair.ParseFull(plaintext)
    kmsSignatureClient->>kmsSignatureClient: tx.Sign(networkPassphrase, keypairFull)
    kmsSignatureClient-->>Caller: signed transaction
```

### Encryption Context

The KMS `DecryptInput` always includes an encryption context: `{"pubkey": <distributionAccountPublicKey>}`. This context was set during the original KMS `Encrypt` call. If the public key doesn't match, KMS will reject the decryption — binding the ciphertext to a specific Stellar account.

```go
// awskms/utils.go
const PrivateKeyEncryptionContext = "pubkey"

func GetPrivateKeyEncryptionContext(publicKey string) map[string]*string {
    return map[string]*string{
        PrivateKeyEncryptionContext: aws.String(publicKey),
    }
}
```

### KMS Client Construction

`awskms.GetKMSClient(awsRegion)` creates an AWS session with the specified region and returns a `*kms.KMS`. The `kmsSignatureClient` takes a `kmsiface.KMSAPI` interface, making it testable without real AWS credentials.

**Source files:** `internal/signing/kms_signature_client.go`, `internal/signing/awskms/utils.go`

---

## Channel Account Lifecycle

Channel accounts are Stellar accounts whose keypairs are stored encrypted in the `channel_accounts` table. They serve as source accounts for built transactions, allowing many transactions to be submitted concurrently (one channel account per in-flight transaction).

### Provisioning Flow

Run once via `go run main.go channel-account ensure <N>`:

```mermaid
sequenceDiagram
    participant CLI as CLI: channel-account ensure N
    participant ChAccSvc as channelAccountService
    participant RPC as Stellar RPC
    participant DB as channel_accounts table

    CLI->>ChAccSvc: EnsureChannelAccounts(ctx, N)
    ChAccSvc->>DB: Count() → currentCount

    alt currentCount < N
        loop batches of ≤19 accounts
            ChAccSvc->>ChAccSvc: Generate keypair.Random() × batchSize
            ChAccSvc->>ChAccSvc: AES-GCM Encrypt(seed, passphrase) per keypair
            ChAccSvc->>RPC: Wait for RPC health (up to 5 min)
            ChAccSvc->>RPC: GetAccountLedgerSequence(distributionAccount)
            Note over ChAccSvc: Build tx with BeginSponsoringFutureReserves +\nCreateAccount(amount=0) +\nEndSponsoringFutureReserves per keypair
            ChAccSvc->>RPC: SendTransaction (signed by distAcc + all new chAcc keypairs)
            ChAccSvc->>RPC: Poll GetTransaction until SUCCESS
            ChAccSvc->>DB: BatchInsert(publicKey, encryptedPrivateKey)
        end
    else currentCount > N
        loop batches of ≤19 accounts
            ChAccSvc->>DB: GetAll(batchSize) FOR UPDATE SKIP LOCKED
            ChAccSvc->>RPC: Build AccountMerge tx → distAcc
            ChAccSvc->>RPC: Sign + Submit
            ChAccSvc->>DB: Delete(publicKeys)
        end
    end
```

**Key constant:** `MaximumCreateAccountOperationsPerStellarTx = 19` — each sponsored account creation requires 3 operations (`BeginSponsoringFutureReserves`, `CreateAccount`, `EndSponsoringFutureReserves`), so 19 is the practical limit before hitting Stellar's ~100-operation payload cap; it's also driven by signature count limits.

Channel accounts are created with `amount = "0"` — the reserve is sponsored by the distribution account via `BeginSponsoringFutureReserves`.

### Transaction Submission Flow (Acquire → Build → Sign → Release)

```mermaid
sequenceDiagram
    participant Caller as GraphQL Resolver
    participant TxSvc as transactionService
    participant ChAccClient as ChannelAccountSignatureClient
    participant DB as channel_accounts table
    participant RPC as Stellar RPC

    Caller->>TxSvc: BuildAndSignTransactionWithChannelAccount(genericTx, simulation)
    TxSvc->>ChAccClient: GetAccountPublicKey(ctx, 30s timeout)
    ChAccClient->>DB: FOR UPDATE SKIP LOCKED → lock idle account 30s
    alt no idle account
        ChAccClient->>ChAccClient: sleep 1s, retry up to 6 times
        ChAccClient-->>TxSvc: ErrUnavailableChannelAccounts
    end
    DB-->>ChAccClient: channelAccount.PublicKey
    ChAccClient-->>TxSvc: channelAccountPublicKey

    TxSvc->>TxSvc: Validate operations (no chAcc as source)
    TxSvc->>RPC: GetAccountLedgerSequence(channelAccountPublicKey)
    TxSvc->>TxSvc: txnbuild.NewTransaction(channelAccount as source)
    TxSvc->>TxSvc: tx.HashHex(networkPassphrase)
    TxSvc->>DB: AssignTxToChannelAccount(publicKey, txHash)
    TxSvc->>ChAccClient: SignStellarTransaction(tx, channelAccountPublicKey)
    ChAccClient->>DB: GetAllByPublicKey(channelAccountPublicKey)
    ChAccClient->>ChAccClient: AES-GCM Decrypt(encryptedPrivateKey, passphrase)
    ChAccClient->>ChAccClient: tx.Sign(networkPassphrase, keypairFull)
    ChAccClient-->>TxSvc: signed transaction
    TxSvc-->>Caller: signed *txnbuild.Transaction

    Note over Caller,DB: Caller submits the transaction externally
    Note over DB: channel account remains locked until...\n(1) ingestion sees the tx confirmed, OR\n(2) locked_until timestamp expires
```

### Locking SQL

Channel account acquisition is a single atomic SQL statement using `FOR UPDATE SKIP LOCKED`:

```sql
UPDATE channel_accounts
SET
    locked_tx_hash = NULL,
    locked_at = NOW(),
    locked_until = NOW() + INTERVAL '<N> seconds'
WHERE public_key = (
    SELECT
        public_key
    FROM channel_accounts
    WHERE
        locked_until IS NULL
        OR locked_until < NOW()
    ORDER BY random()
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

`ORDER BY random()` distributes load across channel accounts rather than always hitting the same one. `SKIP LOCKED` ensures concurrent callers never block each other — they simply skip rows held by another transaction.

### Release Mechanisms

Two independent release paths ensure accounts aren't permanently stuck:

| Mechanism | Trigger | SQL |
|---|---|---|
| **Ingestion-based (primary)** | Ledger is ingested; tx confirmed on-chain | `UnassignTxAndUnlockChannelAccounts()` — sets `locked_tx_hash=NULL`, `locked_at=NULL`, `locked_until=NULL` WHERE `locked_tx_hash = ANY(txHashes)` |
| **Time expiry (fallback)** | `locked_until < NOW()` at next acquisition attempt | The `GetAndLockIdleChannelAccount` WHERE clause naturally reclaims expired locks |

The ingestion path runs inside `PersistLedgerData` → `unlockChannelAccounts()` within the same database transaction that writes the ledger data. This guarantees that channel accounts are released exactly when the confirming ledger is committed to the DB.

Unlock is keyed on **inner transaction hashes** — for fee-bumped transactions, the channel account's `locked_tx_hash` matches the inner transaction hash, not the outer fee-bump hash.

### Retry Configuration (`channelAccountDBSignatureClient`)

| Constant | Value | Description |
|---|---|---|
| `DefaultRetryCount` | 6 | Max acquisition attempts before returning error |
| `DefaultRetryInterval` | 1s | Sleep between attempts |
| `lockedUntil` default | 60s | Lock duration when `GetAccountPublicKey` called without opts |
| `timeoutInSecs` in BuildAndSign | 30s | Lock duration passed from `transactionService` |

After 6 retries (6 seconds total), returns `ErrUnavailableChannelAccounts`. The distinction between `ErrNoIdleChannelAccountAvailable` (accounts exist but are all locked) and `ErrNoChannelAccountConfigured` (table is empty) guides operators in diagnosing the cause.

**Source files:** `internal/signing/channel_account_db_signature_client.go`, `internal/signing/store/channel_accounts_model.go`, `internal/services/channel_account_service.go`, `internal/services/ingest_live.go`

---

## Encryption

### AES-128-GCM (`internal/signing/utils/encrypter.go`)

Channel account private keys are encrypted with AES-128-GCM before being stored in the database.

```mermaid
flowchart TD
    subgraph "Encrypt(message, passphrase)"
        P1["passphrase"] --> H1["SHA-256 hash"]
        H1 --> K1["key = hash[:16]\n(first 16 bytes = 128-bit key)"]
        K1 --> AES1["aes.NewCipher(key)"]
        AES1 --> GCM1["cipher.NewGCM(block)"]
        GCM1 --> Nonce1["rand.Read(nonce)\nnonce size from GCM"]
        Nonce1 --> Seal["gcm.Seal(nonce, nonce, message, nil)\nnonce prepended to ciphertext"]
        Seal --> B64["base64.StdEncoding.Encode\n→ stored string"]
    end

    subgraph "Decrypt(encryptedMessage, passphrase)"
        P2["passphrase"] --> H2["SHA-256 hash"]
        H2 --> K2["key = hash[:16]"]
        K2 --> AES2["aes.NewCipher(key)"]
        AES2 --> GCM2["cipher.NewGCM(block)"]
        B641["base64.Decode(encryptedMessage)"] --> Split["Split: nonce = decoded[:nonceSize]\nciphertext = decoded[nonceSize:]"]
        GCM2 --> Open["gcm.Open(nil, nonce, ciphertext, nil)"]
        Split --> Open
        Open --> Plain["plaintext private key (string)"]
    end
```

**Key derivation:** `SHA-256(passphrase)[:16]` — takes the first 16 bytes of the SHA-256 hash of the passphrase to get a 128-bit AES key. This is why the constant is `keyBytes = 16`.

**Nonce:** Random per encryption call, prepended to the ciphertext blob before base64 encoding. GCM nonce size is always 12 bytes.

**Output format:** `base64(nonce || gcm_ciphertext_with_tag)` — the GCM authentication tag is embedded in the ciphertext by `Seal`.

### `PrivateKeyEncrypter` Interface

```go
type PrivateKeyEncrypter interface {
    Encrypt(ctx context.Context, message, passphrase string) (string, error)
    Decrypt(ctx context.Context, encryptedMessage, passphrase string) (string, error)
}
```

`DefaultPrivateKeyEncrypter{}` is the sole implementation. The interface exists for testability — mocks can return controlled plaintexts without real encryption.

### Channel Account vs KMS Encryption

| | Channel Account keys | KMS distribution key |
|---|---|---|
| Algorithm | AES-128-GCM (Go stdlib) | AWS KMS Encrypt/Decrypt |
| Key derivation | `SHA-256(passphrase)[:16]` | AWS manages the key material |
| Stored in | `channel_accounts.encrypted_private_key` (text) | `keypairs.encrypted_private_key` (bytes) |
| Encryption triggered by | `EnsureChannelAccounts` at provisioning time | External AWS tooling before storing in DB |
| Decryption triggered by | Every `SignStellarTransaction` call on channel client | Every `SignStellarTransaction` / `SignStellarFeeBumpTransaction` call on KMS client |
| Passphrase source | `CHANNEL_ACCOUNT_ENCRYPTION_PASSPHRASE` | AWS KMS key ARN + encryption context |

**Source file:** `internal/signing/utils/encrypter.go`

---

## Transaction Signing Flows

### `buildTransaction` — Channel Account Path

End-to-end flow from GraphQL mutation to signed transaction envelope:

```mermaid
flowchart TD
    GQL["GraphQL: buildTransaction mutation"]
    GQL --> Resolver["resolver/transaction.go\nbuildTransaction()"]
    Resolver --> TxSvc["transactionService\n.BuildAndSignTransactionWithChannelAccount()"]

    TxSvc --> GetKey["ChannelAccountSignatureClient\n.GetAccountPublicKey(ctx, 30s)\n→ acquires DB lock"]
    GetKey --> Validate["Validate operations\n(no chAcc as source)"]
    Validate --> SeqNum["RPCService\n.GetAccountLedgerSequence(chAccPubKey)"]
    SeqNum --> Build["txnbuild.NewTransaction\n(chAcc as source, IncrementSequenceNum=true)"]
    Build --> Hash["tx.HashHex(networkPassphrase)"]
    Hash --> Assign["ChannelAccountStore\n.AssignTxToChannelAccount(pubKey, txHash)"]
    Assign --> Sign["ChannelAccountSignatureClient\n.SignStellarTransaction(tx, chAccPubKey)"]
    Sign --> Return["Return signed *txnbuild.Transaction\n(caller submits via RPC)"]

    style GetKey fill:#fff3e0
    style Assign fill:#fff3e0
```

The `MaxTimeoutInSeconds = 300` constant caps the transaction's time bounds to 5 minutes, regardless of what the caller requests. This prevents channel accounts from being locked indefinitely if a client submits a transaction with a far-future timeout.

### `createFeeBumpTransaction` — Distribution Account Path

```mermaid
flowchart TD
    GQL["GraphQL: createFeeBumpTransaction mutation"]
    GQL --> Resolver["resolver/transaction.go\ncreateFeeBumpTransaction()"]
    Resolver --> FeeBumpSvc["feeBumpService\n.WrapTransaction(tx)"]

    FeeBumpSvc --> Check["Account.IsAccountFeeBumpEligible\n(checks account is registered)"]
    Check --> FeeCheck["tx.BaseFee() <= s.BaseFee"]
    FeeCheck --> SigCheck["len(tx.Signatures()) > 0"]
    SigCheck --> GetDistKey["DistributionAccountSignatureClient\n.GetAccountPublicKey(ctx)"]
    GetDistKey --> NewFeeBump["txnbuild.NewFeeBumpTransaction\n(FeeAccount = distributionAccount)"]
    NewFeeBump --> SignFB["DistributionAccountSignatureClient\n.SignStellarFeeBumpTransaction(ctx, feeBumpTx)"]
    SignFB --> B64["signedFeeBumpTx.Base64()\n→ XDR envelope string"]
    B64 --> Return["Return (txXDR, networkPassphrase)"]
```

The inner transaction must already be signed by the caller — `feeBumpService` only adds the distribution account's fee-bump signature. `ErrNoSignaturesProvided` is returned if the inner transaction has no signatures.

### Dependency Wiring at Startup (`cmd/serve.go` → `internal/serve/serve.go`)

```mermaid
flowchart TD
    CMD["cmd/serve.go\nPersistentPreRunE"]
    CMD --> Resolver["utils.SignatureClientResolver\n(ENV or KMS based on config)"]
    CMD --> NewCh["signing.NewChannelAccountDBSignatureClient\n(always CHANNEL_ACCOUNT type)"]

    Resolver --> DistClient["cfg.DistributionAccountSignatureClient"]
    NewCh --> ChClient["cfg.ChannelAccountSignatureClient"]

    DistClient --> FB["NewFeeBumpService\n(DistributionAccountSignatureClient)"]
    DistClient --> TX["NewTransactionService\n(DistributionAccountSignatureClient\n+ ChannelAccountSignatureClient)"]
    DistClient --> CA["NewChannelAccountService\n(DistributionAccountSignatureClient\n+ ChannelAccountSignatureClient)"]
    ChClient --> TX
    ChClient --> CA

    FB --> HandlerDeps["handlerDeps{}"]
    TX --> HandlerDeps
    CA --> Goroutine["go ensureChannelAccounts(ctx, N)\n(runs async at startup)"]
```

`ensureChannelAccounts` runs in a goroutine at startup — it doesn't block the HTTP server from starting, so the API is immediately available even while channel accounts are being provisioned.

**Source files:**
- `cmd/serve.go` — serve command DI wiring
- `cmd/utils/utils.go` — `SignatureClientResolver` factory
- `internal/serve/serve.go` — `initHandlerDeps` service construction
- `internal/services/transaction_service.go` — `BuildAndSignTransactionWithChannelAccount`
- `internal/services/fee_bump_service.go` — `WrapTransaction`

---

**Topics:** [[entries/index]] | [[entries/signing]]
