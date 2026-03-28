package resolver

import (
	"fmt"
	"strings"
)

type DatasetResolver interface {
	ResolveDataset(name string, version int) (string, error)
}

type MapResolver struct {
	knownVersions map[string]map[int]struct{}
	latestVersion map[string]int
}

func NewMapResolver(datasetVersions map[string][]int) *MapResolver {
	known := make(map[string]map[int]struct{}, len(datasetVersions))
	latest := make(map[string]int, len(datasetVersions))
	for rawName, versions := range datasetVersions {
		name := strings.ToLower(strings.TrimSpace(rawName))
		if name == "" {
			continue
		}
		set := make(map[int]struct{}, len(versions))
		max := 0
		for _, version := range versions {
			if version <= 0 {
				continue
			}
			set[version] = struct{}{}
			if version > max {
				max = version
			}
		}
		if max > 0 {
			known[name] = set
			latest[name] = max
		}
	}
	return &MapResolver{knownVersions: known, latestVersion: latest}
}

func (r *MapResolver) ResolveDataset(name string, version int) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("empty dataset name")
	}
	name = strings.ToLower(strings.TrimSpace(name))
	versions, ok := r.knownVersions[name]
	if !ok {
		return "", fmt.Errorf("dataset not found: %s", name)
	}
	if version == 0 {
		return name, nil
	}
	if _, ok := versions[version]; !ok {
		return "", fmt.Errorf("dataset %s does not have version %d", name, version)
	}
	return name, nil
}
