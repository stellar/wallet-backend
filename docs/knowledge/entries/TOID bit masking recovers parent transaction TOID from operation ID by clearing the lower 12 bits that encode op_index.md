---
context: The operation dataloader uses &^ 0xFFF to group operations by their parent transaction without an extra JOIN
type: insight
created: 2026-02-24
---

TOIDs (Transaction Order IDs) encode both ledger position and operation index in a single int64. The lower 12 bits encode the operation's index within the transaction; the upper bits encode the transaction's position. To recover the parent transaction TOID from an operation TOID, the dataloader applies a bitwise AND-NOT with `0xFFF` — this clears the 12 low bits, yielding the transaction TOID. This trick allows the operation dataloader to group a batch of operation TOIDs by their parent transaction TOID without joining against the transactions table, making the dataloader query self-contained.
