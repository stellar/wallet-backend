-- +migrate Up
ALTER TABLE state_changes ALTER COLUMN state_change_reason SET NOT NULL;
ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check
    CHECK (state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'LOW', 'MEDIUM', 'HIGH',
        'HOME_DOMAIN', 'SET', 'CLEAR', 'DATA_ENTRY', 'SPONSOR', 'UNSPONSOR'
    ));

-- +migrate Down
ALTER TABLE state_changes ALTER COLUMN state_change_reason DROP NOT NULL;
ALTER TABLE state_changes DROP CONSTRAINT state_changes_state_change_reason_check;
ALTER TABLE state_changes ADD CONSTRAINT state_changes_state_change_reason_check
    CHECK (state_change_reason IS NULL OR state_change_reason IN (
        'CREATE', 'MERGE', 'DEBIT', 'CREDIT', 'MINT', 'BURN',
        'ADD', 'REMOVE', 'UPDATE', 'LOW', 'MEDIUM', 'HIGH',
        'HOME_DOMAIN', 'SET', 'CLEAR', 'DATA_ENTRY', 'SPONSOR', 'UNSPONSOR'
    ));
