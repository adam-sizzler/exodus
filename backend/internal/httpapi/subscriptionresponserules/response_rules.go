package subscriptionresponserules

import (
	"net/http"
	"regexp"
	"strings"
	"sync"
)

var regexCache sync.Map // map[string]*regexp.Regexp

func getCompiledRegex(pattern string) (*regexp.Regexp, error) {
	if val, ok := regexCache.Load(pattern); ok {
		return val.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexCache.Store(pattern, re)
	return re, nil
}

type Config struct {
	Version  string    `json:"version"`
	Rules    []Rule    `json:"rules"`
	Settings *Settings `json:"settings,omitempty"`
}

type Rule struct {
	Name                  string             `json:"name"`
	Enabled               bool               `json:"enabled"`
	Operator              string             `json:"operator"`
	Conditions            []Condition        `json:"conditions"`
	ResponseType          string             `json:"responseType"`
	ResponseModifications *RuleModifications `json:"responseModifications,omitempty"`
	Description           *string            `json:"description,omitempty"`
}

type Condition struct {
	HeaderName    string `json:"headerName"`
	Operator      string `json:"operator"`
	Value         string `json:"value"`
	CaseSensitive bool   `json:"caseSensitive"`
}

type Settings struct {
	DisableSubscriptionAccessByPath bool `json:"disableSubscriptionAccessByPath"`
}

type RuleModifications struct {
	Headers                           []RuleHeader    `json:"headers,omitempty"`
	ApplyHeadersToEnd                 bool            `json:"applyHeadersToEnd,omitempty"`
	SubscriptionTemplate              *string         `json:"subscriptionTemplate,omitempty"`
	IgnoreHostXrayJsonTemplate        bool            `json:"ignoreHostXrayJsonTemplate,omitempty"`
	IgnoreServeJsonAtBaseSubscription bool            `json:"ignoreServeJsonAtBaseSubscription,omitempty"`
	AdditionalExtendedClientsRegex    []string        `json:"additionalExtendedClientsRegex,omitempty"`
	DisableHwidCheck                  bool            `json:"disableHwidCheck,omitempty"`
	ExcludeHostsByTags                []string        `json:"excludeHostsByTags,omitempty"`
	RespondWithRemarks                []string        `json:"respondWithRemarks,omitempty"`
	Encryption                        *RuleEncryption `json:"encryption,omitempty"`
}

type RuleEncryption struct {
	Method string `json:"method"` // "age1" | "age1pq1"
	Key    string `json:"key"`
}

type RuleHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MatchResult struct {
	Matched      bool
	ResponseType string
	MatchedRule  *Rule
}

func MatchRulesDetailed(rules *Config, headers http.Header, overrideClientType string, mapClientType func(string) string, defaultResponse string) MatchResult {
	if overrideClientType != "" {
		if rules == nil {
			return MatchResult{Matched: false}
		}
		if rules.Settings != nil && rules.Settings.DisableSubscriptionAccessByPath {
			return MatchResult{Matched: true, ResponseType: "BLOCK"}
		}
		return MatchResult{Matched: true, ResponseType: mapClientType(overrideClientType)}
	}

	if rules == nil || len(rules.Rules) == 0 {
		return MatchResult{Matched: false}
	}

	normalizedHeaders := map[string]string{}
	for key, values := range headers {
		if len(values) > 0 {
			normalizedHeaders[strings.ToLower(key)] = strings.Join(values, ",")
		}
	}

	for _, rule := range rules.Rules {
		if !rule.Enabled {
			continue
		}

		if len(rule.Conditions) == 0 {
			return MatchResult{Matched: true, ResponseType: strings.ToUpper(rule.ResponseType), MatchedRule: &rule}
		}

		op := strings.ToUpper(strings.TrimSpace(rule.Operator))
		if op == "" {
			op = "AND"
		}

		matched := op == "AND"
		for _, cond := range rule.Conditions {
			if op == "AND" {
				if !evalCondition(cond, normalizedHeaders) {
					matched = false
					break
				}
			} else {
				if evalCondition(cond, normalizedHeaders) {
					matched = true
					break
				}
				matched = false
			}
		}

		if matched {
			return MatchResult{Matched: true, ResponseType: strings.ToUpper(rule.ResponseType), MatchedRule: &rule}
		}
	}

	return MatchResult{Matched: false}
}

func evalCondition(cond Condition, headers map[string]string) bool {
	headerName := strings.ToLower(strings.TrimSpace(cond.HeaderName))
	value, ok := headers[headerName]
	if !ok {
		return false
	}
	operator := strings.ToUpper(strings.TrimSpace(cond.Operator))

	checkValue := value
	checkTarget := cond.Value

	switch operator {
	case "EQUALS":
		if !cond.CaseSensitive {
			return strings.EqualFold(value, cond.Value)
		}
		return value == cond.Value
	case "NOT_EQUALS":
		if !cond.CaseSensitive {
			return !strings.EqualFold(value, cond.Value)
		}
		return value != cond.Value
	case "CONTAINS":
		if !cond.CaseSensitive {
			checkValue = strings.ToLower(checkValue)
			checkTarget = strings.ToLower(checkTarget)
		}
		return strings.Contains(checkValue, checkTarget)
	case "NOT_CONTAINS":
		if !cond.CaseSensitive {
			checkValue = strings.ToLower(checkValue)
			checkTarget = strings.ToLower(checkTarget)
		}
		return !strings.Contains(checkValue, checkTarget)
	case "STARTS_WITH":
		if !cond.CaseSensitive {
			checkValue = strings.ToLower(checkValue)
			checkTarget = strings.ToLower(checkTarget)
		}
		return strings.HasPrefix(checkValue, checkTarget)
	case "NOT_STARTS_WITH":
		if !cond.CaseSensitive {
			checkValue = strings.ToLower(checkValue)
			checkTarget = strings.ToLower(checkTarget)
		}
		return !strings.HasPrefix(checkValue, checkTarget)
	case "ENDS_WITH":
		if !cond.CaseSensitive {
			checkValue = strings.ToLower(checkValue)
			checkTarget = strings.ToLower(checkTarget)
		}
		return strings.HasSuffix(checkValue, checkTarget)
	case "NOT_ENDS_WITH":
		if !cond.CaseSensitive {
			checkValue = strings.ToLower(checkValue)
			checkTarget = strings.ToLower(checkTarget)
		}
		return !strings.HasSuffix(checkValue, checkTarget)
	case "REGEX":
		pattern := checkTarget
		if !cond.CaseSensitive {
			pattern = "(?i)" + pattern
		}
		re, err := getCompiledRegex(pattern)
		if err != nil {
			return false
		}
		return re.MatchString(checkValue)
	case "NOT_REGEX":
		pattern := checkTarget
		if !cond.CaseSensitive {
			pattern = "(?i)" + pattern
		}
		re, err := getCompiledRegex(pattern)
		if err != nil {
			return false
		}
		return !re.MatchString(checkValue)
	case "EXISTS", "NOT_EMPTY":
		return strings.TrimSpace(value) != ""
	case "NOT_EXISTS", "EMPTY":
		return strings.TrimSpace(value) == ""
	default:
		return false
	}
}
