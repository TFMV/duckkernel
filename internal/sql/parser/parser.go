package parser

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	sqlparser "vitess.io/vitess/go/vt/sqlparser"
)

type TableReference struct {
	Name    string
	Alias   string
	Version int
	IsCTE   bool
}

var versionSyntax = regexp.MustCompile(`\b([a-zA-Z_][a-zA-Z0-9_]*)@v([0-9]+)\b`)

func ExtractTableReferences(query string) ([]TableReference, error) {
	cleanSQL, versions, err := stripVersionSyntax(query)
	if err != nil {
		return nil, err
	}

	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, err
	}
	stmt, err := parser.Parse(cleanSQL)
	if err != nil {
		return nil, err
	}

	cteNames := collectCTENames(stmt)

	refs := make([]TableReference, 0)
	walkFn := func(node sqlparser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *sqlparser.AliasedTableExpr:
			switch expr := n.Expr.(type) {
			case sqlparser.TableName:
				name := strings.ToLower(expr.Name.String())
				if name == "" {
					return true, nil
				}
				if _, isCTE := cteNames[name]; isCTE {
					return true, nil
				}
				refs = append(refs, TableReference{
					Name:    name,
					Alias:   strings.ToLower(n.As.String()),
					Version: versions[name],
					IsCTE:   false,
				})
			}
		}
		return true, nil
	}

	if err := sqlparser.Walk(walkFn, stmt); err != nil {
		return nil, err
	}

	return normalizeReferences(refs)
}

func stripVersionSyntax(query string) (string, map[string]int, error) {
	versions := make(map[string]int)
	replaced := versionSyntax.ReplaceAllStringFunc(query, func(token string) string {
		parts := versionSyntax.FindStringSubmatch(token)
		if len(parts) != 3 {
			return token
		}
		name := strings.ToLower(parts[1])
		version, err := strconv.Atoi(parts[2])
		if err != nil {
			return token
		}
		if existing, ok := versions[name]; ok && existing != version {
			return token
		}
		versions[name] = version
		return name
	})

	for token := range versions {
		if _, ok := versions[token]; !ok {
			return "", nil, fmt.Errorf("ambiguous version syntax for %s", token)
		}
	}

	return replaced, versions, nil
}

func collectCTENames(stmt sqlparser.Statement) map[string]struct{} {
	ctes := make(map[string]struct{})
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		if stmt.With != nil {
			for _, expr := range stmt.With.CTEs {
				ctes[strings.ToLower(expr.ID.String())] = struct{}{}
			}
		}
	case *sqlparser.Union:
		if stmt.With != nil {
			for _, expr := range stmt.With.CTEs {
				ctes[strings.ToLower(expr.ID.String())] = struct{}{}
			}
		}
	}
	return ctes
}

func normalizeReferences(refs []TableReference) ([]TableReference, error) {
	seen := make(map[string]TableReference, len(refs))
	out := make([]TableReference, 0, len(refs))
	for _, ref := range refs {
		if existing, ok := seen[ref.Name]; ok {
			if existing.Version != ref.Version {
				return nil, fmt.Errorf("ambiguous version reference for dataset %s", ref.Name)
			}
			continue
		}
		seen[ref.Name] = ref
		out = append(out, ref)
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Version < out[j].Version
	})
	return out, nil
}
