package sql

import (
	"sort"
	"strings"

	parser "vitess.io/vitess/go/vt/sqlparser"
)

func ExtractTableNames(query string) ([]string, error) {
	p, err := parser.New(parser.Options{})
	if err != nil {
		return nil, err
	}
	stmt, err := p.Parse(query)
	if err != nil {
		return nil, err
	}

	tables := make(map[string]struct{})
	ctes := make(map[string]struct{})

	// First pass: collect CTE names
	if sel, ok := stmt.(*parser.Select); ok && sel.With != nil {
		for _, cte := range sel.With.CTEs {
			ctes[strings.ToLower(cte.ID.String())] = struct{}{}
		}
	}

	// Walk AST
	err = parser.Walk(func(node parser.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *parser.AliasedTableExpr:
			switch expr := n.Expr.(type) {
			case parser.TableName:
				name := strings.ToLower(expr.Name.String())

				// skip CTE references
				if _, isCTE := ctes[name]; isCTE {
					return true, nil
				}

				if name != "" {
					tables[name] = struct{}{}
				}
			}
		}

		return true, nil
	}, stmt)

	if err != nil {
		return nil, err
	}

	// Convert to sorted slice (deterministic)
	result := make([]string, 0, len(tables))
	for t := range tables {
		result = append(result, t)
	}

	sort.Strings(result)
	return result, nil
}

func ExtractDependencies(query string, candidates []string) []string {
	refs, err := ExtractTableNames(query)
	if err != nil {
		return nil
	}

	candidateSet := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidateSet[strings.ToLower(candidate)] = struct{}{}
	}

	resolved := make([]string, 0, len(refs))
	for _, ref := range refs {
		if _, ok := candidateSet[ref]; ok {
			resolved = append(resolved, ref)
		}
	}

	sort.Strings(resolved)
	return resolved
}

func IsValidName(name string) bool {
	if strings.TrimSpace(name) == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')) {
				return false
			}
			continue
		}
		if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}
