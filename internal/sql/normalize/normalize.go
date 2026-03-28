package normalize

import (
	"strings"

	sqlparser "vitess.io/vitess/go/vt/sqlparser"
)

func NormalizeSQL(query string) (string, error) {
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return "", err
	}
	stmt, err := parser.Parse(query)
	if err != nil {
		return "", err
	}

	normalized := sqlparser.String(stmt)
	normalized = strings.TrimSpace(normalized)
	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = strings.ToLower(normalized)
	return normalized, nil
}
