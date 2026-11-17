package middleware

import (
	"context"
	"encoding/json"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// Fixed label values for the argument usage counter. With schema names and enum values,
// these are the only values the counter can carry, so its cardinality is bounded.
const (
	argValueSet       = "set"
	argValueOpenStart = "open_start"
	argValueInvalid   = "invalid"
)

// GraphQLUsageMetrics records which fields and arguments each operation uses.
//
// It walks the operation AST once per request, inside MutateOperationContext. gqlgen calls
// that hook only after the operation has parsed, validated, and had its variables coerced, so
// rejected operations are never counted. Walking the AST (instead of hooking field resolution)
// counts each field path once per operation, however many items a list field returns.
type GraphQLUsageMetrics struct {
	metrics *metrics.GraphQLMetrics
	schema  *ast.Schema
	now     func() time.Time
}

var (
	_ graphql.HandlerExtension        = (*GraphQLUsageMetrics)(nil)
	_ graphql.OperationContextMutator = (*GraphQLUsageMetrics)(nil)
)

// NewGraphQLUsageMetrics creates the field and argument usage extension.
func NewGraphQLUsageMetrics(m *metrics.GraphQLMetrics) *GraphQLUsageMetrics {
	return &GraphQLUsageMetrics{metrics: m, now: time.Now}
}

// ExtensionName implements graphql.HandlerExtension.
func (u *GraphQLUsageMetrics) ExtensionName() string {
	return "GraphQLUsageMetrics"
}

// Validate implements graphql.HandlerExtension. It keeps the schema so the walker can look up
// argument types.
func (u *GraphQLUsageMetrics) Validate(schema graphql.ExecutableSchema) error {
	u.schema = schema.Schema()
	return nil
}

// MutateOperationContext implements graphql.OperationContextMutator. It never changes the
// operation and never returns an error.
func (u *GraphQLUsageMetrics) MutateOperationContext(_ context.Context, oc *graphql.OperationContext) *gqlerror.Error {
	if oc == nil || oc.Operation == nil {
		return nil
	}

	usage := u.collectUsage(oc.Operation, oc.Variables)
	for path := range usage.fields {
		u.metrics.FieldUsageTotal.WithLabelValues(path).Inc()
	}
	for a := range usage.args {
		u.metrics.ArgumentUsageTotal.WithLabelValues(a.fieldPath, a.argument, a.value).Inc()
	}
	return nil
}

type argUsage struct {
	fieldPath string
	argument  string
	value     string
}

// operationUsage holds the distinct field paths and argument labels of one operation.
// Sets make every entry count once per operation.
type operationUsage struct {
	fields map[string]struct{}
	args   map[argUsage]struct{}
}

func (u *GraphQLUsageMetrics) collectUsage(op *ast.OperationDefinition, vars map[string]any) operationUsage {
	usage := operationUsage{
		fields: map[string]struct{}{},
		args:   map[argUsage]struct{}{},
	}
	u.walkSelections(op.SelectionSet, "", vars, &usage)
	return usage
}

func (u *GraphQLUsageMetrics) walkSelections(set ast.SelectionSet, parentPath string, vars map[string]any, usage *operationUsage) {
	for _, sel := range set {
		switch s := sel.(type) {
		case *ast.Field:
			if !shouldInclude(s.Directives, vars) {
				continue
			}
			path := s.Name
			if parentPath != "" {
				path = parentPath + "." + s.Name
			}
			usage.fields[path] = struct{}{}
			u.collectArguments(s, path, vars, usage)
			u.walkSelections(s.SelectionSet, path, vars, usage)
		case *ast.FragmentSpread:
			if !shouldInclude(s.Directives, vars) || s.Definition == nil {
				continue
			}
			u.walkSelections(s.Definition.SelectionSet, parentPath, vars, usage)
		case *ast.InlineFragment:
			if !shouldInclude(s.Directives, vars) {
				continue
			}
			u.walkSelections(s.SelectionSet, parentPath, vars, usage)
		}
	}
}

// shouldInclude applies @skip and @include to a selection.
func shouldInclude(directives ast.DirectiveList, vars map[string]any) bool {
	if d := directives.ForName("skip"); d != nil {
		if v, _ := d.ArgumentMap(vars)["if"].(bool); v {
			return false
		}
	}
	if d := directives.ForName("include"); d != nil {
		if v, _ := d.ArgumentMap(vars)["if"].(bool); !v {
			return false
		}
	}
	return true
}

// collectArguments records the arguments the request wrote on one field. field.Arguments holds
// only arguments present in the query, so schema defaults are never counted.
func (u *GraphQLUsageMetrics) collectArguments(field *ast.Field, path string, vars map[string]any, usage *operationUsage) {
	if len(field.Arguments) == 0 || field.Definition == nil {
		return
	}

	var since, until any
	for _, arg := range field.Arguments {
		value, err := arg.Value.Value(vars)
		if err != nil || value == nil {
			continue
		}

		switch arg.Name {
		case "since":
			since = value
			continue
		case "until":
			until = value
			continue
		}

		var argType *ast.Type
		if def := field.Definition.Arguments.ForName(arg.Name); def != nil {
			argType = def.Type
		}
		u.recordValue(path, arg.Name, argType, value, usage)
	}

	switch {
	case since != nil:
		usage.args[argUsage{path, "since", u.timeRangeBucket(since, until)}] = struct{}{}
	case until != nil:
		usage.args[argUsage{path, "until", argValueOpenStart}] = struct{}{}
	}
}

// recordValue records one argument, or each set sub-field of an input object.
func (u *GraphQLUsageMetrics) recordValue(path, name string, typ *ast.Type, value any, usage *operationUsage) {
	var def *ast.Definition
	if typ != nil && u.schema != nil {
		def = u.schema.Types[typ.Name()]
	}

	// Lists of input objects are not in the schema today; count them as a plain "set".
	if def != nil && def.Kind == ast.InputObject && typ.Elem == nil {
		obj, ok := value.(map[string]any)
		if !ok {
			usage.args[argUsage{path, name, argValueSet}] = struct{}{}
			return
		}
		for _, f := range def.Fields {
			sub, present := obj[f.Name]
			if !present || sub == nil {
				continue
			}
			u.recordValue(path, name+"."+f.Name, f.Type, sub, usage)
		}
		return
	}

	usage.args[argUsage{path, name, argumentValueLabel(name, def, value)}] = struct{}{}
}

// argumentValueLabel maps a scalar or enum argument to its label value.
func argumentValueLabel(name string, def *ast.Definition, value any) string {
	if def != nil && def.Kind == ast.Enum {
		if s, ok := value.(string); ok && def.EnumValues.ForName(s) != nil {
			return s
		}
		return argValueSet
	}

	if name == "first" || name == "last" {
		if n, ok := toInt64(value); ok {
			return pageSizeBucket(n)
		}
	}
	return argValueSet
}

func pageSizeBucket(n int64) string {
	switch {
	case n <= 10:
		return "<=10"
	case n <= 25:
		return "<=25"
	case n <= 50:
		return "<=50"
	case n <= 100:
		return "<=100"
	default:
		return ">100"
	}
}

// timeRangeBucket returns the width of [since, until] as a bucket. A missing until means
// "up to now". A negative width falls into the smallest bucket; the resolver rejects it.
func (u *GraphQLUsageMetrics) timeRangeBucket(since, until any) string {
	start, err := graphql.UnmarshalTime(since)
	if err != nil {
		return argValueInvalid
	}
	end := u.now()
	if until != nil {
		end, err = graphql.UnmarshalTime(until)
		if err != nil {
			return argValueInvalid
		}
	}

	const day = 24 * time.Hour
	switch width := end.Sub(start); {
	case width <= day:
		return "<=1d"
	case width <= 7*day:
		return "<=7d"
	case width <= 30*day:
		return "<=30d"
	case width <= 365*day:
		return "<=365d"
	default:
		return ">365d"
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
