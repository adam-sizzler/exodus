package squads

import (
	"testing"
	"time"
)

type mockSquadRowScanner struct {
	values []any
}

func (m *mockSquadRowScanner) Scan(dest ...any) error {
	for i, d := range dest {
		if i >= len(m.values) {
			break
		}
		val := m.values[i]
		if val == nil {
			continue
		}
		switch target := d.(type) {
		case *string:
			if s, ok := val.(string); ok {
				*target = s
			}
		case **int:
			if v, ok := val.(int); ok {
				*target = &v
			}
		case *[]string:
			if s, ok := val.([]string); ok {
				*target = s
			}
		case *time.Time:
			if t, ok := val.(time.Time); ok {
				*target = t
			}
		}
	}
	return nil
}

func TestScanInternalSquad(t *testing.T) {
	now := time.Now().UTC()
	mock := &mockSquadRowScanner{
		values: []any{
			"squad-uuid-1",
			15,
			"VIP Squad",
			[]string{"vip", "fast"},
			now,
			now,
		},
	}

	squad, err := scanInternalSquad(mock)
	if err != nil {
		t.Fatalf("unexpected error scanning squad: %v", err)
	}
	if squad.UUID != "squad-uuid-1" {
		t.Errorf("UUID = %q, want 'squad-uuid-1'", squad.UUID)
	}
	if squad.ViewPosition != 15 {
		t.Errorf("ViewPosition = %d, want 15", squad.ViewPosition)
	}
	if squad.Name != "VIP Squad" {
		t.Errorf("Name = %q, want 'VIP Squad'", squad.Name)
	}
	if len(squad.Tags) != 2 || squad.Tags[0] != "vip" {
		t.Errorf("Tags = %v, want ['vip', 'fast']", squad.Tags)
	}
}

func TestBuildInternalSquadResponse(t *testing.T) {
	squad := InternalSquad{
		UUID:         "uuid-1",
		ViewPosition: 1,
		Name:         "Squad 1",
		Tags:         nil,
	}
	res := buildInternalSquadResponse(squad, 5, nil)
	if res.Info.MembersCount != 5 {
		t.Errorf("MembersCount = %d, want 5", res.Info.MembersCount)
	}
	if res.Info.InboundsCount != 0 {
		t.Errorf("InboundsCount = %d, want 0", res.Info.InboundsCount)
	}
	if res.Tags == nil {
		t.Error("Tags should be initialized slice, got nil")
	}
	if res.Inbounds == nil {
		t.Error("Inbounds should be initialized slice, got nil")
	}
}
