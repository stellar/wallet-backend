# Webhooks for the GraphQL API — design space

Status: exploration, no decision made.

The wallet-backend exposes a read-only GraphQL API (`internal/serve/graphql`) over data
written by the live ingestion loop (`internal/services/ingest_live.go`). Clients that want
to know "did anything happen to my account" have to poll it. This document explores what a
push mechanism should look like, given that the query surface is GraphQL rather than REST.

## 1. The three decisions

Every webhook design answers three questions, and they are largely independent:

1. **Trigger** — what makes a delivery happen?
2. **Payload** — what goes in the body?
3. **Transport** — signing, retries, ordering, replay, backpressure.

GraphQL only complicates (1) and (2). Transport is the same engineering problem it is for a
REST webhook, and is the part most likely to be underestimated; §6 covers it.

The interesting axis is the cross-product of trigger and payload:

|                          | **Fixed payload**                              | **Client-supplied selection set**                        |
| ------------------------ | ---------------------------------------------- | -------------------------------------------------------- |
| **Predefined trigger**   | **C.** Stripe, GitHub, Slack, Helius           | **B.** Apollo callback protocol, AppSync, Shopify, Hasura |
| **Trigger from query**   | (degenerate — nobody ships this)               | **A.** Salesforce PushTopic, Alchemy Custom Webhooks      |

Option A splits further, and the split matters more than the A/B/C distinction:

- **A1 — static analysis.** Parse the registered query, derive a predicate over the write
  set, index the predicate, match writes against it. No re-execution.
- **A2 — re-execution.** Register the query, re-run it per ledger against the new data, and
  deliver the result (optionally suppressing empty results). No analysis.

A1 is the "dynamically determine the trigger conditions" idea. A2 is what the products that
look like A actually do.

## 2. Do other APIs let clients register custom queries?

Yes, but it is rare, always constrained, and in the one large-scale case the vendor moved
away from it.

**Salesforce PushTopic** is the purest instance of A1: the client stores a SOQL query and
Salesforce publishes an event whenever a record change matches it. The restrictions are
instructive — the query must select `Id`, and sub-selects and aggregate queries are
unsupported, with an entire documentation page devoted to
[unsupported SOQL statements](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/unsupported_soql_statements.htm).
Salesforce now describes PushTopics as
[legacy](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/pushtopic_events_intro.htm)
and directs new work to Change Data Capture, which has **no query definition at all** — a
fixed per-object change event (option C). The stated tradeoff is exactly the one we are
weighing: CDC gives up query-based filtering in exchange for not having to interpret a query.

**Alchemy Custom Webhooks** is the closest thing to what we would build, and it is A2: you
compose a [GraphQL query](https://www.alchemy.com/docs/reference/custom-webhook) in a
playground, Alchemy pins the root `block` filter to `latest`, and re-evaluates the query on
every canonical block. Two details are worth stealing or avoiding:

- The query root **is the event** (`block`), not a global `Query` type. The input is bounded,
  so no static analysis is needed and cost per subscription is bounded.
- Alchemy delivers on every block *even when the filter matches nothing*, treating the null
  result as a heartbeat/receipt. That is a defensible choice, but at Stellar's ~5s close time
  it means a delivery every 5 seconds per subscription forever.

**QuickNode Streams** takes A2 to its limit: the filter is a sandboxed JavaScript `main()`
function that the client validates against sample block data. Maximum expressiveness,
maximum operational surface (sandbox, CPU/time limits, versioning of user code).

Everyone else lands in B or C:

- **Apollo Router's [HTTP callback protocol](https://www.apollographql.com/docs/graphos/routing/operations/subscriptions/callback-protocol)**
  is the GraphQL-native answer: the client registers a *subscription operation* and results
  arrive as HTTP callbacks instead of over a persistent connection. Trigger = the subscription
  root field (predefined by the schema); payload = the client's selection set. This is B, and
  it is the closest thing to a standard we have.
- **AWS AppSync** allows client filter arguments on a subscription plus
  [enhanced server-side filters](https://docs.aws.amazon.com/appsync/latest/devguide/aws-appsync-real-time-enhanced-filtering.html),
  and caps them hard: basic arguments are limited to 5, equality-only, AND-only; enhanced
  filters are limited to 5 unique `fieldName`s and the limit is not raised on request. Even
  *filter* expressiveness gets rationed once it has to be evaluated per event.
- **Shopify** is the strongest signal. The entire Admin API is GraphQL and webhook
  subscriptions are created *via a GraphQL mutation*
  ([`webhookSubscriptionCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/webhookSubscriptionCreate)) —
  yet a subscription is a predefined **topic**, plus a `filter` in Shopify's search syntax,
  plus `includeFields`/`metafieldNamespaces` for projection. A GraphQL vendor with every
  incentive to accept a GraphQL document chose a topic, a filter DSL, and a field mask.
- **Hasura event triggers** are table + operation + (optionally) which columns must change,
  with payload column selection and request transforms — B/C hybrid with no query.
- **Stripe, GitHub, Slack, Twilio, Helius, Moralis** are all C: predefined event types, fixed
  payloads, frequently thin (an ID plus a type, expecting a follow-up read).

The GraphQL ecosystem's own attempt at A1 is also informative. `@live` queries
([graphql-live-query](https://github.com/n1ru4l/graphql-live-query)) do not statically derive
triggers; the store records the resource identifiers *resolved during the last execution*
(`User:1`, `Query.users`) and re-executes when one is invalidated. **Convex** does the same
thing with a read set captured at execution time. Both are A2 with a memoized dependency set,
and both require the write path to emit invalidation keys that line up with what queries read.

**Takeaway:** the "server analyzes the query to derive triggers" design (A1) has one notable
production instance, it is hedged with a list of unsupported query shapes, and its vendor
replaced it with predefined events. Products that appear to accept custom queries are
re-evaluating them per event (A2) over a **bounded, event-rooted input**.

## 3. Why A1 is worse here than it looks

Concretely, against our schema:

- **The payload is state; the trigger is a delta.** Our root queries return current state
  (`accountByAddress`, `transactionByHash`, `operationById`). Nothing in a query says "when
  this changes". A query for `accountByAddress(address: X) { balances { ... } }` gives us the
  account, so the derived trigger is "any write touching X" — account-level, which is exactly
  the predefined trigger we would have written by hand. The analysis buys nothing.
- **Precision collapses to the coarsest thing in the query.** Any selection that traverses
  `Account.transactions` or `Account.stateChanges` fires on every state change for that
  account. Clients who wanted "credits over 100 XLM" have no way to say so, because a *query*
  has no vocabulary for it beyond the existing `AccountStateChangeFilterInput`. So we would
  end up promoting filter inputs into trigger predicates — reinventing B, inside A.
- **Pagination and time bounds are meaningless as triggers.** `first`/`after`/`since`/`until`
  exist on every connection. What does a webhook registered with `since: <fixed time>` mean
  three months later? Every such argument becomes a special case in the analyzer.
- **Undebuggable failures.** The trigger is invisible to the client. "Why didn't my webhook
  fire" becomes a question only we can answer, and the answer depends on analyzer internals.
- **Silent breakage on schema evolution.** #672 encoded state-change variants as 19 concrete
  types. Any similar reshaping changes the derived predicate for already-registered queries.
  Predefined triggers break loudly at registration time; derived ones drift.
- **We would have to reject queries we cannot analyze.** That is the PushTopic
  unsupported-statements page, and it is a permanent tax on both docs and support.

A2 avoids all of the analysis problems and creates one big cost problem: re-executing a
client query per ledger means running the full resolver stack — DataLoaders, TimescaleDB
reads — once per subscription per ~5s close. With 10k subscriptions that is 2k query
executions/second against the same database that serves interactive traffic, whether or not
anything relevant happened. Alchemy makes this work because the query is rooted at a single
block that is already in memory. The equivalent restriction for us is §5.

## 4. What our ingestion pipeline hands us for free

The trigger side is nearly free if we take triggers from where the data already is.
`persistLedgerData` (`internal/services/ingest_live.go`) commits one ledger in a single DB
transaction from an in-memory `IndexerBuffer` that already exposes:

- `GetStateChanges()` — every state change, with `category`, `reason`, `account_id`,
  `token_id`, `amount`, and the counterparty columns (`destination_account_id`,
  `spender_account_id`, `signer_account_id`, …)
- `GetTransactions()` / `GetOperations()` and their participant maps
- balance/trustline/allowance/LP change sets

That is a complete trigger vocabulary, in memory, already paid for. Matching subscriptions
against it costs a map lookup per state change and zero extra database reads. `StateChangeCategory`
(10 values) × `StateChangeReason` (11 values), plus the 19 concrete state-change types, is an
event catalogue we already ship and document.

Two pipeline facts constrain any design:

- **Backfill and protocol data migrations also write rows** (`ingest_backfill.go`,
  `protocol_migrate_history.go`). Emitting webhooks from those paths would replay millions of
  events. Only the live path should emit; this needs to be explicit, not incidental.
- **Whole-network ingestion, no account registry.** There is no `accounts` table — accounts
  exist implicitly in `transactions_accounts` / `operations_accounts`. So a subscription's
  account set is the *only* thing bounding fan-out, and "subscribe to everything" has to be
  either forbidden or priced.

Deliveries must not run inside the ingestion transaction. The natural shape is a
transactional outbox: match subscriptions against the buffer, insert delivery rows in the
same transaction as the ledger data, and dispatch from a separate worker. Ingestion stays
as fast as it is now, and at-least-once delivery survives a crash.

## 5. Sketch of the recommended shape (B, with a C-shaped default)

Define the events once, in the schema, as subscription root fields; make the webhook a
*durable transport* for the same operation, per Apollo's callback protocol. Then "client
registers a custom query" is true in the sense clients care about — they choose the fields —
without us deriving anything.

```graphql
type Subscription {
  """Fires once per matching state change on the live ingestion path."""
  stateChanges(filter: StateChangeTriggerInput!): StateChangeEvent!

  """Fires once per transaction involving any of `accounts`."""
  accountTransactions(accounts: [String!]!): TransactionEvent!
}

input StateChangeTriggerInput {
  accounts:   [String!]!          # required; bounds fan-out
  categories: [StateChangeCategory!]
  reasons:    [StateChangeReason!]
  tokenIds:   [String!]
  role:       ParticipantRole      # SUBJECT | COUNTERPARTY | ANY
}

type StateChangeEvent {
  """Stable dedupe key: toId:operationId:stateChangeId."""
  eventId:     String!
  ledgerNumber: UInt32!
  stateChange: BaseStateChange!    # client selects concrete-type fields via inline fragments
}
```

Registration is a mutation that stores a **persisted operation**, validated at registration
time against the same gates as interactive traffic (`FixedComplexityLimit`, depth limit) but
with a tighter budget, since the cost is paid every ledger rather than once per client
request. Rejecting an over-budget payload query at registration is a good error; discovering
it at ledger 6,000,000 is not.

Cost control that falls out of this shape:

- **Every trigger predicate is a cheap conjunction over columns we already have in the
  buffer.** Index subscriptions by account; a ledger's match step is O(state changes).
- **Amortize execution by `(trigger, queryHash)`.** Subscriptions that registered the same
  selection set for the same event execute once and fan the result out to N endpoints.
- **A fixed default payload** (the C option) for clients who don't supply a query: the event
  envelope plus scalar state-change fields, requiring no resolver execution at all. This is
  the thin-event/follow-up-read pattern, and it should be the documented default because it
  is the only shape whose cost is independent of client behaviour.
- **Selection sets stay inside the event subtree.** `BaseStateChange.account`,
  `.transaction`, `.operation` are `forceResolver` fields that hit the database; allow them,
  but count them heavily in the registration budget. Do not let a payload query reach
  `Account.transactions` or any connection — that is unbounded work per event.

## 6. Transport, which is where the real work is

Independent of A/B/C, and roughly the same list regardless:

- **Signing.** HMAC-SHA256 over `timestamp.body` with a per-subscription secret
  ([Standard Webhooks](https://www.standardwebhooks.com/)-shaped), or Ed25519 to mirror the
  existing Stellar-key client auth (`CLIENT_AUTH_PUBLIC_KEYS`). Include the timestamp in the
  signed material to prevent replay.
- **At-least-once + dedupe key.** `eventId = toId:operationId:stateChangeId` matches the
  `state_changes` primary key, so clients get an idempotency key with no invention required.
- **Ordering.** Per-account, per-ledger ordering is achievable and worth promising; global
  ordering is not.
- **Retries and death.** Exponential backoff, capped attempts, then auto-disable with a
  visible status. Failure-rate metrics per subscription.
- **Catch-up without a replay API.** Deliveries carry `ledgerNumber` and an opaque cursor;
  a client that missed a window reconciles with a normal GraphQL query using the existing
  `since`/`until` bounds and Relay cursors. This is a genuine advantage of hanging webhooks
  off a query API — the gap-filling endpoint already exists and is already paginated.
- **Backfill/migration suppression** (§4), stated in the docs as a guarantee.
- **Per-tenant limits** on subscription count, accounts per subscription, and payload
  complexity.

## 7. Recommendation

1. **Do not build A1.** The one production example is deprecated by its vendor, the derived
   predicate for our schema degenerates to "any write touching this account", and it makes
   trigger behaviour invisible to clients and fragile across schema changes.
2. **Build C first, as the transport-hardening milestone**: predefined triggers, fixed thin
   payload, outbox + dispatcher + signing + retries + dedupe. This is the part every option
   needs, and it is most of the work.
3. **Then add B**: a registered, persisted, complexity-bounded selection set over the event
   subtree, amortized by query hash. Ship the trigger vocabulary as `Subscription` root
   fields so a future WebSocket/SSE transport reuses the same definitions.
4. **Revisit A2 only if** clients demonstrably need predicates our trigger inputs cannot
   express *and* we are willing to fund per-ledger re-execution. If we get there, copy
   Alchemy's constraint rather than its ergonomics: root the query at the event, keep the
   input in memory, and never let it touch a connection field.

## 8. Open questions

- Who owns a subscription? Auth today is a shared client JWT signed by one of
  `CLIENT_AUTH_PUBLIC_KEYS` — there is no tenant identity to attach subscriptions to, or to
  scope limits and secrets by. This likely has to be solved first.
- Is "subscribe to all accounts" a supported use case (exchange/analytics), or explicitly out
  of scope? It changes the fan-out model completely.
- Do we deliver on liveness gaps — a heartbeat when nothing matched (Alchemy's choice), or
  silence plus a status endpoint?
- Should `Subscription` ship as a real WebSocket/SSE transport too, or only as the schema
  vocabulary for webhooks initially?
- Ingestion reorg/retry semantics: `ingestProcessedDataWithRetry` can re-run a ledger. The
  outbox must be written inside the same transaction so a retried ledger cannot double-emit.
