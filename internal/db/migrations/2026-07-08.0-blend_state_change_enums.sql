-- +migrate Up

-- Extend state_changes CHECK constraints with the Blend v2 categories and reasons.
-- Categories name the on-chain object an account position changes against
-- (category = object, reason = action, matching the core convention); the
-- amount-bearing categories reuse the generic CREDIT/DEBIT/ADD/REMOVE/BURN
-- reasons, so only Blend-specific verbs are added to the reason list.
-- The base lists restate 2025-06-10.4-statechanges.sql exactly.
-- NOT VALID skips a full hypertable scan; constraint changes propagate to chunks automatically.
ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_category_check;
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

ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check CHECK (
    state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'SET', 'CLEAR',
        'BORROW', 'REPAY', 'FLASH_LOAN', 'BAD_DEBT', 'FILL', 'CLAIM'
    )
) NOT VALID;

-- +migrate Down

-- Requires no BLEND_* rows to exist (delete them first in dev).
ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_category_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_category_check CHECK (
    state_change_category IN (
        'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
        'DATA_ENTRY', 'HOME_DOMAIN', 'ALLOWANCE', 'FLAGS', 'TRUSTLINE',
        'BALANCE_AUTHORIZATION'
    )
) NOT VALID;

ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check CHECK (
    state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'SET', 'CLEAR'
    )
) NOT VALID;
