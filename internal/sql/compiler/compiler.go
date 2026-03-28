package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/TFMV/duckkernel/internal/graph/node"
	"github.com/TFMV/duckkernel/internal/sql/normalize"
	"github.com/TFMV/duckkernel/internal/sql/parser"
	"github.com/TFMV/duckkernel/internal/sql/resolver"
)

type Edge struct {
	From string
	To   string
}

type CompileResult struct {
	Node         node.Node
	Dependencies []string
	Edges        []Edge
}

type Compiler struct {
	resolver resolver.DatasetResolver
}

func New(resolver resolver.DatasetResolver) *Compiler {
	return &Compiler{resolver: resolver}
}

func (c *Compiler) Compile(nodeName, query string) (*CompileResult, error) {
	if strings.TrimSpace(nodeName) == "" {
		return nil, fmt.Errorf("node name must not be empty")
	}

	references, err := parser.ExtractTableReferences(query)
	if err != nil {
		return nil, err
	}

	if len(references) == 0 {
		normalizedSQL, err := normalize.NormalizeSQL(query)
		if err != nil {
			return nil, err
		}
		nodeID := computeNodeID(normalizedSQL, nil)
		compiledNode := node.Node{
			ID:   nodeID,
			Name: nodeName,
			Versions: []*node.NodeVersion{
				{
					Version:    1,
					SQL:        normalizedSQL,
					Mode:       node.ModeCached,
					Status:     node.StatusDirty,
					CreatedAt:  time.Now().UTC(),
					UpdatedAt:  time.Now().UTC(),
					CacheValid: false,
				},
			},
		}
		return &CompileResult{Node: compiledNode}, nil
	}

	sortedRefs, err := dedupeReferences(references)
	if err != nil {
		return nil, err
	}

	dependencies := make([]string, 0, len(sortedRefs))
	for _, ref := range sortedRefs {
		resolved, err := c.resolver.ResolveDataset(ref.Name, ref.Version)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, resolved)
	}

	sort.Strings(dependencies)

	normalizedSQL, err := normalize.NormalizeSQL(query)
	if err != nil {
		return nil, err
	}

	nodeID := computeNodeID(normalizedSQL, dependencies)
	edges := make([]Edge, len(dependencies))
	for i, dep := range dependencies {
		edges[i] = Edge{From: dep, To: nodeID}
	}

	compiledNode := node.Node{
		ID:   nodeID,
		Name: nodeName,
		Versions: []*node.NodeVersion{
			{
				Version:      1,
				SQL:          normalizedSQL,
				Mode:         node.ModeCached,
				Status:       node.StatusDirty,
				Dependencies: dependencies,
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
				CacheValid:   false,
			},
		},
	}

	return &CompileResult{
		Node:         compiledNode,
		Dependencies: dependencies,
		Edges:        edges,
	}, nil
}

func dedupeReferences(refs []parser.TableReference) ([]parser.TableReference, error) {
	seen := make(map[string]parser.TableReference, len(refs))
	out := make([]parser.TableReference, 0, len(refs))
	for _, ref := range refs {
		if existing, ok := seen[ref.Name]; ok {
			if existing.Version != ref.Version {
				return nil, fmt.Errorf("ambiguous version references for dataset %s", ref.Name)
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

func computeNodeID(normalizedSQL string, dependencies []string) string {
	hash := sha256.New()
	hash.Write([]byte(normalizedSQL))
	hash.Write([]byte("|"))
	hash.Write([]byte(strings.Join(dependencies, ",")))
	return hex.EncodeToString(hash.Sum(nil))
}
