-- Seed data for loadtest schema containing trustline_assets and contract_tokens.
-- This file is loaded before ingestion starts to populate prerequisite reference data.

-- Native XLM SAC contract for mainnet passphrase
-- Contract ID computed from: xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID("Public Global Stellar Network ; September 2015")
-- UUID computed using DeterministicContractID("CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA")
INSERT INTO contract_tokens (id, contract_id, type, code, issuer, name, symbol, decimals)
VALUES (
    '2163faf8-5c69-5e93-984d-5c94dd051976',
    'CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA',
    'SAC',
    'XLM',
    NULL,
    'Stellar Lumens',
    'XLM',
    7
) ON CONFLICT (contract_id) DO NOTHING;
