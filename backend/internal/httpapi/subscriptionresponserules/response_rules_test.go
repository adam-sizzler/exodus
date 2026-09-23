package subscriptionresponserules

import (
"net/http"
"testing"
)

func TestMatchRules_CaseInsensitiveAndRegex(t *testing.T) {
rules := Config{
Version: "1.0",
Rules: []Rule{
{
Name:         "Match Clash CaseFold",
Enabled:      true,
ResponseType: "CLASH",
Conditions: []Condition{
{
HeaderName:    "User-Agent",
Operator:      "EQUALS",
Value:         "clash-verge",
CaseSensitive: false,
},
},
},
{
Name:         "Match Regex V2Ray",
Enabled:      true,
ResponseType: "V2RAY",
Conditions: []Condition{
{
HeaderName:    "User-Agent",
Operator:      "REGEX",
Value:         "^v2rayN/[0-9.]+$",
CaseSensitive: false,
},
},
},
},
}

h1 := http.Header{}
h1.Set("User-Agent", "CLASH-VERGE")
res1 := MatchRulesDetailed(&rules, h1, "", func(s string) string { return s }, "DEFAULT")
if !res1.Matched || res1.ResponseType != "CLASH" {
t.Fatalf("expected CLASH match, got %+v", res1)
}

h2 := http.Header{}
h2.Set("User-Agent", "v2rayn/6.23")
res2 := MatchRulesDetailed(&rules, h2, "", func(s string) string { return s }, "DEFAULT")
if !res2.Matched || res2.ResponseType != "V2RAY" {
t.Fatalf("expected V2RAY match, got %+v", res2)
}

// Repeated match should use cached regex without error
res2Cached := MatchRulesDetailed(&rules, h2, "", func(s string) string { return s }, "DEFAULT")
if !res2Cached.Matched || res2Cached.ResponseType != "V2RAY" {
t.Fatalf("expected V2RAY match from cache, got %+v", res2Cached)
}
}
