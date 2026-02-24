---
context: Classic operations have explicit source/destination accounts; Soroban contract calls can involve any number of contracts at arbitrary depths in the auth tree
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# ParticipantsProcessor extracts Soroban contract addresses as participants by recursively walking authorization entries and subinvocations

`ParticipantsProcessor` builds the set of Stellar addresses (accounts and contracts) that participated in a transaction. For classic operations, participants are the operation source account plus explicit counterparties (destination, trader, etc.). For Soroban operations, the processor must additionally walk the authorization entry tree to find all contract addresses involved.

The recursive walk visits `SorobanAuthorizedInvocation.SubInvocations` at each depth level, extracting the contract address from each invocation. A single Soroban transaction can involve dozens of contracts in a cross-contract call chain — all of them need to appear in the participant set for the transaction to be discoverable by those contract addresses.

Classic and Soroban participant extraction use different code paths that are unified in the output: both produce `string` Stellar addresses (G-addresses for classic accounts, C-addresses for Soroban contracts) added to the same participant set for the transaction.

---

Relevant Notes:
- [[ContractDeployProcessor walks Soroban authorization invocations recursively to detect all contract deployments including subinvocations]] — same recursive pattern used for a different purpose
- [[nine specialized processors run per transaction in the indexer fan-out]] — ParticipantsProcessor is the only ParticipantsProcessorInterface in the table

Areas:
- [[entries/ingestion]]
