package app

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/nikolaysavelev/soldout/internal/catalog/api"
)

// GroupLayout возвращает только геометрию: booking проверяет занятость в собственной транзакции.
func (s *Service) GroupLayout(ctx context.Context, eventID, sectorID uuid.UUID) (api.SectorLayout, error) {
	event, err := s.repo.GetEvent(ctx, eventID)
	if err != nil {
		return api.SectorLayout{}, err
	}
	sector, err := s.repo.GetSector(ctx, sectorID)
	if err != nil {
		return api.SectorLayout{}, err
	}
	if sector.VenueID != event.VenueID || sector.Kind != api.SectorSeated {
		return api.SectorLayout{}, fmt.Errorf("%w: нужен seated-сектор площадки мероприятия", api.ErrValidation)
	}
	seats, err := s.repo.SeatsBySector(ctx, sectorID)
	if err != nil {
		return api.SectorLayout{}, err
	}
	out := api.SectorLayout{Seats: make([]api.Seat, len(seats))}
	for i, seat := range seats {
		out.Seats[i] = seat.ToAPI()
	}
	sort.Slice(out.Seats, func(i, j int) bool {
		a, b := out.Seats[i], out.Seats[j]
		if a.RowNo != b.RowNo {
			return a.RowNo < b.RowNo
		}
		return a.SeatNo < b.SeatNo
	})
	return out, nil
}
