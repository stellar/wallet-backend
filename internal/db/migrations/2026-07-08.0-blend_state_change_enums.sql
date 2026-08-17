-- +migrate Up

-- Extend state_changes CHECK constraints with the Blend v2 categories and reasons.
-- Categories name the on-chain object an account position changes against
-- (category = object, reason = action, matching the core convention); the
-- amount-bearing categories reuse the generic CREDIT/DEBIT/ADD/REMOVE/BURN
-- reasons, so only Blend-specific verbs are added to the reason list.
-- The base lists restate 2025-06-10.4-statechanges.sql exactly.
-- NOT VALID skips a full hypertable scan; constraint changes propagate to chunks automatically.
ALTER TABLE state_changes DROP CONSTRAINT IF EXISTS state_changes_state_change_category_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_category_check CHECK (
    state_change_category IN (
        'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
        'DATA_ENTRY', 'HOME_DOMAIN', 'ALLOWANCE', 'FLAGS', 'TRUSTLINE',
        'BALANCE_AUTHORIZATION',
        'BLEND_SUPPLY', 'BLEND_COLLATERAL', 'BLEND_DEBT', 'BLEND_AUCTION',
        'BLEND_EMISSIONS', 'BLEND_BACKSTOP_EMISSIONS', 'BLEND_BACKSTOP',
        'BLEND_BACKSTOP_QUEUE'
    )
) NOT VALID;

ALTER TABLE state_changes DROP CONSTRAINT IF EXISTS state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check CHECK (
    state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'SET', 'CLEAR',
        'BORROW', 'REPAY', 'FLASH_LOAN', 'BAD_DEBT', 'FILL', 'CLAIM'
    )
) NOT VALID;

-- +migrate Down

-- Rollback requires no BLEND_* rows: the re-narrowed CHECKs below are NOT VALID
-- (cheap, no hypertable scan), so they cannot reject pre-existing rows. Fail
-- loudly here rather than leaving rows the restored constraints forbid. The
-- EXISTS probes are served by the columnstore's bloom sparse indexes on
-- state_change_category/state_change_reason, so conforming chunks are skipped
-- without decompression.
-- +migrate StatementBegin
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM state_changes
    WHERE state_change_category IN (
      'BLEND_SUPPLY', 'BLEND_COLLATERAL', 'BLEND_DEBT', 'BLEND_AUCTION',
      'BLEND_EMISSIONS', 'BLEND_BACKSTOP_EMISSIONS', 'BLEND_BACKSTOP',
      'BLEND_BACKSTOP_QUEUE')
  ) OR EXISTS (
    SELECT 1 FROM state_changes
    WHERE state_change_reason IN ('BORROW', 'REPAY', 'FLASH_LOAN', 'BAD_DEBT', 'FILL', 'CLAIM')
  ) THEN
    RAISE EXCEPTION 'rollback requires no BLEND_* state_changes rows; delete them first';
  END IF;
END $$;
-- +migrate StatementEnd

ALTER TABLE state_changes DROP CONSTRAINT IF EXISTS state_changes_state_change_category_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_category_check CHECK (
    state_change_category IN (
        'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
        'DATA_ENTRY', 'HOME_DOMAIN', 'ALLOWANCE', 'FLAGS', 'TRUSTLINE',
        'BALANCE_AUTHORIZATION'
    )
) NOT VALID;

ALTER TABLE state_changes DROP CONSTRAINT IF EXISTS state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check CHECK (
    state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'SET', 'CLEAR'
    )
) NOT VALID;
