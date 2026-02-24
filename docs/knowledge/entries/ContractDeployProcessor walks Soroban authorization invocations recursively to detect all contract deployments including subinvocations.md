---
context: Soroban contract deploys can appear at any depth in the auth tree; a flat operation-level scan would miss contracts deployed by other contracts
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# ContractDeployProcessor walks Soroban authorization invocations recursively to detect all contract deployments including subinvocations

`ContractDeployProcessor.ProcessOperation()` extracts all contract deployments from a Soroban operation by recursively traversing the authorization invocation tree. A single operation can deploy multiple contracts if deployer contracts invoke further deploy calls as subinvocations.

The traversal walks `SorobanAuthorizedInvocation.SubInvocations` at each level, checking whether each invocation matches the `create_contract` host function pattern. For each match, it records a `StateChange` with `Category=CONTRACT_DEPLOY` and the newly deployed contract ID.

The recursive walk is necessary because Soroban's execution model allows any contract to deploy other contracts. A factory contract deploying multiple instances in a single transaction would generate one auth invocation per deployment, potentially nested multiple levels deep. A flat scan of the top-level operation body would only find directly invoked deploys, missing factory-pattern deployments.

---

Relevant Notes:
- [[nine specialized processors run per transaction in the indexer fan-out]] — ContractDeployProcessor is one of three OperationProcessorInterface processors
- [[ParticipantsProcessor extracts Soroban contract addresses as participants by recursively walking authorization entries and subinvocations]] — uses same recursive traversal pattern for a different purpose

Areas:
- [[entries/ingestion]]
