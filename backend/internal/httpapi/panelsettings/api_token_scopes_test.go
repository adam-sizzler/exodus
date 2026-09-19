package panelsettings

import (
	"testing"

	"exodus/internal/httpapi/scopecatalog"
)

func TestScopeCatalogCounts(t *testing.T) {
	resources := buildAPITokenScopes(nil)
	if len(resources) != 21 {
		t.Fatalf("expected 21 resources, got %d", len(resources))
	}

	endpointsCount := 0
	scopeSet := make(map[string]struct{})
	scopeSet["*"] = struct{}{}
	for _, res := range resources {
		for _, s := range res.ResourceScopes {
			scopeSet[s] = struct{}{}
		}
		for _, ep := range res.Endpoints {
			endpointsCount++
			if ep.Key != "" {
				scopeSet[ep.Key] = struct{}{}
			}
		}
	}

	if endpointsCount != 196 {
		t.Fatalf("expected 196 endpoints, got %d", endpointsCount)
	}

	if len(scopeSet) != 260 {
		t.Fatalf("expected 260 grantable scopes, got %d", len(scopeSet))
	}
}

func TestValidateAPITokenScopesAndExpiration(t *testing.T) {
	// 1. Wildcard scope is valid
	wildcardScopes := normalizeAPITokenScopes([]string{"*"})
	if invalid := scopecatalog.FindInvalidScopes(wildcardScopes); len(invalid) > 0 {
		t.Fatalf("expected wildcard to be valid, got invalid: %v", invalid)
	}

	// 2. Resource-level scopes are valid
	validScopes := normalizeAPITokenScopes([]string{"users:*", "nodes:read", "config-profiles:write"})
	if invalid := scopecatalog.FindInvalidScopes(validScopes); len(invalid) > 0 {
		t.Fatalf("expected valid scopes, got invalid: %v", invalid)
	}

	// 3. Unknown/invalid scopes are detected
	invalidScopes := normalizeAPITokenScopes([]string{"users:*", "hacker:exploit", "unknown:scope"})
	invalid := scopecatalog.FindInvalidScopes(invalidScopes)
	if len(invalid) != 2 {
		t.Fatalf("expected 2 invalid scopes, got: %v", invalid)
	}
	if invalid[0] != "hacker:exploit" || invalid[1] != "unknown:scope" {
		t.Fatalf("unexpected invalid scopes: %v", invalid)
	}

	// 4. Empty scopes normalize to wildcard [*]
	emptyScopes := normalizeAPITokenScopes([]string{})
	if len(emptyScopes) != 1 || emptyScopes[0] != "*" {
		t.Fatalf("expected empty scopes to default to [*], got: %v", emptyScopes)
	}
}

