package data

const InsertPaymentsQuery = `INSERT INTO ingest_payments (operation_id, operation_type, transaction_id, transaction_hash, from_address, to_address, src_asset_code, src_asset_issuer, src_amount, dest_asset_code, dest_asset_issuer, dest_amount, created_at, memo) VALUES (:operation_id, :operation_type, :transaction_id, :transaction_hash, :from_address, :to_address, :src_asset_code, :src_asset_issuer, :src_amount, :dest_asset_code, :dest_asset_issuer, :dest_amount, :created_at, :memo);`
