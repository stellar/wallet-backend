package services

import (
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Minimal XDR helpers shared by tests in the services package. The richer
// versions live in internal/services/sep41 alongside the validator — this file
// keeps a small surface for callers outside that subpackage.

func createScSpecFunctionEntry(name string, inputs []xdr.ScSpecFunctionInputV0, outputs []xdr.ScSpecTypeDef) xdr.ScSpecEntry {
	return xdr.ScSpecEntry{
		Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0,
		FunctionV0: &xdr.ScSpecFunctionV0{
			Name:    xdr.ScSymbol(name),
			Inputs:  inputs,
			Outputs: outputs,
		},
	}
}

func createFunctionInput(name string, typeDef xdr.ScSpecTypeDef) xdr.ScSpecFunctionInputV0 {
	return xdr.ScSpecFunctionInputV0{Name: name, Type: typeDef}
}

func createScSpecTypeDef(scType xdr.ScSpecType) xdr.ScSpecTypeDef {
	return xdr.ScSpecTypeDef{Type: scType}
}

func createSEP41ContractSpec() []xdr.ScSpecEntry {
	addressType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeAddress)
	i128Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeI128)
	u32Type := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeU32)
	stringType := createScSpecTypeDef(xdr.ScSpecTypeScSpecTypeString)

	return []xdr.ScSpecEntry{
		createScSpecFunctionEntry("balance",
			[]xdr.ScSpecFunctionInputV0{createFunctionInput("id", addressType)},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("allowance",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
			},
			[]xdr.ScSpecTypeDef{i128Type},
		),
		createScSpecFunctionEntry("decimals",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{u32Type},
		),
		createScSpecFunctionEntry("name",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{stringType},
		),
		createScSpecFunctionEntry("symbol",
			[]xdr.ScSpecFunctionInputV0{},
			[]xdr.ScSpecTypeDef{stringType},
		),
		createScSpecFunctionEntry("approve",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("spender", addressType),
				createFunctionInput("amount", i128Type),
				createFunctionInput("expiration_ledger", u32Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("transfer",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("transfer_from",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("spender", addressType),
				createFunctionInput("from", addressType),
				createFunctionInput("to", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("burn",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("from", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
		createScSpecFunctionEntry("burn_from",
			[]xdr.ScSpecFunctionInputV0{
				createFunctionInput("spender", addressType),
				createFunctionInput("from", addressType),
				createFunctionInput("amount", i128Type),
			},
			[]xdr.ScSpecTypeDef{},
		),
	}
}
