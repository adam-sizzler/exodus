package squads

import (
	"exodus/internal/httpapi/shared"
)

func scanInternalSquad(scanner shared.RowScanner) (InternalSquad, error) {
	var squad InternalSquad
	var viewPosition *int
	var tags []string

	err := scanner.Scan(
		&squad.UUID,
		&viewPosition,
		&squad.Name,
		&tags,
		&squad.CreatedAt,
		&squad.UpdatedAt,
	)
	if err != nil {
		return squad, err
	}

	if viewPosition != nil {
		squad.ViewPosition = *viewPosition
	}

	if tags != nil {
		squad.Tags = tags
	} else {
		squad.Tags = []string{}
	}

	return squad, nil
}

func buildInternalSquadResponse(squad InternalSquad, membersCount int, inbounds []InternalSquadInboundAPI) InternalSquadAPI {
	if inbounds == nil {
		inbounds = []InternalSquadInboundAPI{}
	}
	tags := squad.Tags
	if tags == nil {
		tags = []string{}
	}
	return InternalSquadAPI{
		UUID:         squad.UUID,
		ViewPosition: squad.ViewPosition,
		Name:         squad.Name,
		Tags:         tags,
		Info: InternalSquadInfo{
			MembersCount:  membersCount,
			InboundsCount: len(inbounds),
		},
		Inbounds:  inbounds,
		CreatedAt: squad.CreatedAt,
		UpdatedAt: squad.UpdatedAt,
	}
}
