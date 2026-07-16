-- +migrate Up

-- Extend state_changes CHECK constraints with the LENDING category and its reasons (Blend v2).
-- NOT VALID skips a full hypertable scan; constraint changes propagate to chunks automatically.
ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_category_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_category_check CHECK (
    state_change_category IN (
        'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
        'METADATA', 'FLAGS', 'TRUSTLINE', 'RESERVES',
        'BALANCE_AUTHORIZATION', 'AUTHORIZATION', 'LENDING'
    )
) NOT VALID;

ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check CHECK (
    state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'LOW', 'MEDIUM', 'HIGH',
        'HOME_DOMAIN', 'SET', 'CLEAR', 'DATA_ENTRY', 'SPONSOR', 'UNSPONSOR',
        'SUPPLY', 'WITHDRAW', 'SUPPLY_COLLATERAL', 'WITHDRAW_COLLATERAL',
        'BORROW', 'REPAY', 'FLASH_LOAN', 'CLAIM', 'LIQUIDATION',
        'BAD_DEBT', 'DEFAULTED_DEBT', 'BACKSTOP_DEPOSIT',
        'BACKSTOP_WITHDRAW_QUEUE', 'BACKSTOP_WITHDRAW_CANCEL', 'BACKSTOP_WITHDRAW'
    )
) NOT VALID;

-- +migrate Down

-- Requires no LENDING rows to exist (delete them first in dev).
ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_category_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_category_check CHECK (
    state_change_category IN (
        'BALANCE', 'ACCOUNT', 'SIGNER', 'SIGNATURE_THRESHOLD',
        'METADATA', 'FLAGS', 'TRUSTLINE', 'RESERVES',
        'BALANCE_AUTHORIZATION', 'AUTHORIZATION'
    )
) NOT VALID;

ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check CHECK (
    state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'LOW', 'MEDIUM', 'HIGH',
        'HOME_DOMAIN', 'SET', 'CLEAR', 'DATA_ENTRY', 'SPONSOR', 'UNSPONSOR'
    )
) NOT VALID;
