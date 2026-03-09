-- +migrate Up
ALTER TABLE protocol_wasms
    ADD CONSTRAINT protocol_wasms_protocol_id_fkey
    FOREIGN KEY (protocol_id) REFERENCES protocols(id);
-- +migrate Down
ALTER TABLE protocol_wasms DROP CONSTRAINT IF EXISTS protocol_wasms_protocol_id_fkey;
