// Command gqldocs renders the GraphQL schema as a Markdown reference page.
//
// Usage: go run ./tools/gqldocs -out docs/api/schema.md
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

const schemaGlob = "internal/serve/graphql/schema/*.graphqls"

func main() {
	out := flag.String("out", "docs/api/schema.md", "path of the Markdown file to write")
	flag.Parse()

	if err := run(*out); err != nil {
		log.Fatal(err)
	}
}

func run(out string) error {
	schema, err := loadSchema(schemaGlob)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	if err := os.WriteFile(out, []byte(render(schema)), 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", out, err)
	}
	return nil
}

func loadSchema(glob string) (*ast.Schema, error) {
	paths, err := filepath.Glob(glob)
	if err != nil {
		return nil, fmt.Errorf("globbing %s: %w", glob, err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no schema files match %s", glob)
	}
	sort.Strings(paths)

	sources := make([]*ast.Source, 0, len(paths))
	for _, p := range paths {
		body, readErr := os.ReadFile(p)
		if readErr != nil {
			return nil, fmt.Errorf("reading %s: %w", p, readErr)
		}
		sources = append(sources, &ast.Source{Name: p, Input: string(body)})
	}

	schema, gqlErr := gqlparser.LoadSchema(sources...)
	if gqlErr != nil {
		return nil, fmt.Errorf("parsing schema: %w", gqlErr)
	}
	return schema, nil
}

// render returns the Markdown reference for every user-defined part of schema.
func render(schema *ast.Schema) string {
	var b strings.Builder
	b.WriteString("# GraphQL schema reference\n\n")
	b.WriteString("Every query, type, and field served at `POST /graphql/query`. ")
	b.WriteString("For auth, pagination, and examples, see [GraphQL API](graphql.md). ")
	b.WriteString("Generated from the schema files by `make gql-docs`; do not edit by hand.\n")

	if schema.Query != nil {
		b.WriteString("\n## Queries\n")
		if desc := paragraphs(schema.Query.Description); desc != "" {
			b.WriteString("\n" + desc + "\n")
		}
		for _, f := range schema.Query.Fields {
			if strings.HasPrefix(f.Name, "__") {
				continue
			}
			b.WriteString("\n### " + f.Name + "\n")
			if desc := paragraphs(f.Description); desc != "" {
				b.WriteString("\n" + desc + "\n")
			}
			if dep := deprecation(f.Directives); dep != "" {
				b.WriteString("\n" + dep + "\n")
			}
			b.WriteString("\nReturns: " + typeRef(schema, f.Type) + "\n")
			if len(f.Arguments) > 0 {
				b.WriteString("\n")
				writeArgs(&b, schema, f.Arguments)
			}
		}
	}

	sections := []struct {
		title string
		kind  ast.DefinitionKind
	}{
		{"Objects", ast.Object},
		{"Interfaces", ast.Interface},
		{"Unions", ast.Union},
		{"Enums", ast.Enum},
		{"Input objects", ast.InputObject},
		{"Scalars", ast.Scalar},
	}
	for _, s := range sections {
		defs := definitions(schema, s.kind)
		if len(defs) == 0 {
			continue
		}
		b.WriteString("\n## " + s.title + "\n")
		for _, def := range defs {
			writeDefinition(&b, schema, def)
		}
	}

	writeDirectives(&b, schema)
	return b.String()
}

// definitions returns user-defined types of one kind, sorted by name.
func definitions(schema *ast.Schema, kind ast.DefinitionKind) []*ast.Definition {
	var defs []*ast.Definition
	for _, def := range schema.Types {
		if def.Kind != kind || def.BuiltIn || isRoot(schema, def) {
			continue
		}
		defs = append(defs, def)
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	return defs
}

func isRoot(schema *ast.Schema, def *ast.Definition) bool {
	return def == schema.Query || def == schema.Mutation || def == schema.Subscription
}

func writeDefinition(b *strings.Builder, schema *ast.Schema, def *ast.Definition) {
	b.WriteString("\n### " + def.Name + "\n")
	if desc := paragraphs(def.Description); desc != "" {
		b.WriteString("\n" + desc + "\n")
	}

	switch def.Kind {
	case ast.Object:
		if len(def.Interfaces) > 0 {
			b.WriteString("\nImplements: " + typeList(schema, def.Interfaces) + "\n")
		}
		writeFields(b, schema, def)
	case ast.Interface:
		var impls []string
		for _, impl := range schema.PossibleTypes[def.Name] {
			impls = append(impls, impl.Name)
		}
		sort.Strings(impls)
		if len(impls) > 0 {
			b.WriteString("\nImplemented by: " + typeList(schema, impls) + "\n")
		}
		writeFields(b, schema, def)
	case ast.Union:
		members := append([]string(nil), def.Types...)
		sort.Strings(members)
		b.WriteString("\nMembers: " + typeList(schema, members) + "\n")
	case ast.InputObject:
		writeFields(b, schema, def)
	case ast.Enum:
		b.WriteString("\n| Value | Description |\n|---|---|\n")
		for _, v := range def.EnumValues {
			b.WriteString("| `" + v.Name + "` | " + withDeprecation(v.Description, v.Directives) + " |\n")
		}
	default:
	}
}

func writeFields(b *strings.Builder, schema *ast.Schema, def *ast.Definition) {
	if len(def.Fields) == 0 {
		return
	}
	b.WriteString("\n| Field | Type | Description |\n|---|---|---|\n")
	for _, f := range def.Fields {
		desc := withDeprecation(f.Description, f.Directives)
		if f.DefaultValue != nil {
			desc = strings.TrimSpace(desc + " Default: `" + f.DefaultValue.String() + "`.")
		}
		b.WriteString("| `" + f.Name + "` | " + typeRef(schema, f.Type) + " | " + desc + " |\n")
	}
	for _, f := range def.Fields {
		if len(f.Arguments) == 0 {
			continue
		}
		b.WriteString("\nArguments of `" + f.Name + "`:\n\n")
		writeArgs(b, schema, f.Arguments)
	}
}

func writeArgs(b *strings.Builder, schema *ast.Schema, args ast.ArgumentDefinitionList) {
	b.WriteString("| Name | Type | Default | Description |\n|---|---|---|---|\n")
	for _, a := range args {
		def := ""
		if a.DefaultValue != nil {
			def = "`" + cell(a.DefaultValue.String()) + "`"
		}
		b.WriteString("| `" + a.Name + "` | " + typeRef(schema, a.Type) + " | " + def + " | " +
			withDeprecation(a.Description, a.Directives) + " |\n")
	}
}

// writeDirectives lists user-defined directives, skipping gqlgen's code-generation ones.
func writeDirectives(b *strings.Builder, schema *ast.Schema) {
	var names []string
	for name, d := range schema.Directives {
		if d.Position != nil && d.Position.Src != nil && d.Position.Src.BuiltIn {
			continue
		}
		if strings.HasPrefix(name, "go") {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return
	}
	sort.Strings(names)
	b.WriteString("\n## Directives\n")
	for _, name := range names {
		d := schema.Directives[name]
		b.WriteString("\n### @" + name + "\n")
		if desc := paragraphs(d.Description); desc != "" {
			b.WriteString("\n" + desc + "\n")
		}
		locs := make([]string, 0, len(d.Locations))
		for _, l := range d.Locations {
			locs = append(locs, "`"+string(l)+"`")
		}
		b.WriteString("\nLocations: " + strings.Join(locs, ", ") + "\n")
		if len(d.Arguments) > 0 {
			b.WriteString("\n")
			writeArgs(b, schema, d.Arguments)
		}
	}
}

// typeRef renders a type in GraphQL syntax with named types linked to their headings.
func typeRef(schema *ast.Schema, t *ast.Type) string {
	var s string
	if t.Elem != nil {
		s = `\[` + typeRef(schema, t.Elem) + `\]`
	} else {
		s = typeName(schema, t.NamedType)
	}
	if t.NonNull {
		s += "!"
	}
	return s
}

func typeName(schema *ast.Schema, name string) string {
	def := schema.Types[name]
	if def == nil || def.BuiltIn {
		return "`" + name + "`"
	}
	return "[" + name + "](#" + strings.ToLower(name) + ")"
}

func typeList(schema *ast.Schema, names []string) string {
	links := make([]string, 0, len(names))
	for _, n := range names {
		links = append(links, typeName(schema, n))
	}
	return strings.Join(links, ", ")
}

func withDeprecation(desc string, dirs ast.DirectiveList) string {
	s := cell(desc)
	if dep := deprecation(dirs); dep != "" {
		s = strings.TrimSpace(s + " " + cell(dep))
	}
	return s
}

func deprecation(dirs ast.DirectiveList) string {
	d := dirs.ForName("deprecated")
	if d == nil {
		return ""
	}
	reason := "No longer supported."
	if arg := d.Arguments.ForName("reason"); arg != nil && arg.Value != nil {
		reason = arg.Value.Raw
	}
	return "Deprecated: " + reason
}

// cell makes text safe for one Markdown table cell.
func cell(s string) string {
	return strings.ReplaceAll(strings.Join(strings.Fields(s), " "), "|", `\|`)
}

// paragraphs collapses whitespace inside each paragraph and keeps blank-line breaks.
func paragraphs(s string) string {
	var out []string
	for _, p := range strings.Split(strings.TrimSpace(s), "\n\n") {
		if p = strings.Join(strings.Fields(p), " "); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "\n\n")
}
