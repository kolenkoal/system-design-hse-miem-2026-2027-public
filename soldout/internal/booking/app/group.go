package app

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
	queueapi "github.com/nikolaysavelev/soldout/internal/queue/api"
)

// CreateGroupHold создаёт всю группу или не создаёт ничего, включая событие outbox.
func (s *Service) CreateGroupHold(ctx context.Context, in api.CreateGroupHoldInput) (group api.GroupHold, created bool, err error) {
	ctx, span := otel.Start(ctx, "booking.CreateGroupHold", otel.Int("group.size", in.Size))
	defer otel.End(span, &err)
	if in.EventID == uuid.Nil || in.SectorID == uuid.Nil || in.UserID == uuid.Nil || in.Size < 1 || in.Size > 4 || len(in.IdempotencyKey) < 1 || len(in.IdempotencyKey) > 128 {
		return group, false, fmt.Errorf("%w: нужны event_id, sector_id, user_id, size 1..4 и ключ длиной 1..128 байт", api.ErrValidation)
	}
	if err = s.queue.Validate(ctx, in.AdmissionToken, in.EventID, in.UserID); err != nil {
		if errors.Is(err, queueapi.ErrAdmissionInvalid) {
			err = fmt.Errorf("%w: %v", api.ErrAdmissionRequired, err)
		}
		return group, false, err
	}
	event, err := s.catalog.GetEvent(ctx, in.EventID)
	if err != nil {
		return group, false, err
	}
	layout, err := s.catalog.GroupLayout(ctx, in.EventID, in.SectorID)
	if err != nil {
		return group, false, err
	}
	blocks, err := domain.AdjacentBlocks(layout.Seats, in.Size)
	if err != nil {
		return group, false, err
	}
	err = s.store.InTx(ctx, func(tx Tx) error {
		if err := tx.LockGroupKey(ctx, in.IdempotencyKey); err != nil {
			return err
		}
		existing, err := tx.FindGroup(ctx, in.IdempotencyKey)
		if err == nil {
			if existing.EventID != in.EventID || existing.SectorID != in.SectorID || existing.UserID != in.UserID || existing.Size != in.Size {
				return api.ErrIdempotencyConflict
			}
			group = existing
			return nil
		}
		if !errors.Is(err, api.ErrGroupNotFound) {
			return err
		}
		if event.SalesState != "open" {
			return api.ErrSalesClosed
		}
		if err := tx.LockUser(ctx, in.EventID, in.UserID); err != nil {
			return err
		}
		n, err := tx.CountUserHolds(ctx, in.EventID, in.UserID)
		if err != nil {
			return err
		}
		if n+in.Size > api.MaxTicketsPerUser {
			return api.ErrHoldLimit
		}
		if err := tx.LockGroupSector(ctx, in.EventID, in.SectorID); err != nil {
			return err
		}
		for _, block := range blocks {
			// Сначала проверяем кандидата. После блокировки повторяем проверку:
			// одиночный hold не берёт блокировку сектора и мог успеть занять место.
			free := true
			for _, seat := range block {
				taken, err := tx.SeatTaken(ctx, in.EventID, seat.ID)
				if err != nil {
					return err
				}
				if taken {
					free = false
					break
				}
			}
			if !free {
				continue
			}
			ids := make([]int64, len(block))
			for i, seat := range block {
				ids[i] = seat.ID
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			for _, id := range ids {
				if err := tx.LockSeat(ctx, in.EventID, id); err != nil {
					return err
				}
			}
			for _, id := range ids {
				taken, err := tx.SeatTaken(ctx, in.EventID, id)
				if err != nil {
					return err
				}
				if taken {
					free = false
					break
				}
			}
			if !free {
				continue
			}
			now := s.now()
			group = api.GroupHold{ID: uuid.New(), EventID: in.EventID, SectorID: in.SectorID, UserID: in.UserID, Size: in.Size, RowNo: block[0].RowNo, FirstSeatNo: block[0].SeatNo, CreatedAt: now, ExpiresAt: now.Add(s.opts.HoldTTL)}
			for _, seat := range block {
				group.Holds = append(group.Holds, domain.NewHold(in.EventID, seat.ID, in.UserID, now, s.opts.HoldTTL).ToAPI())
			}
			if err := tx.InsertGroup(ctx, group, in.IdempotencyKey, block); err != nil {
				return err
			}
			if err := tx.AppendGroupHeld(ctx, api.GroupHeldEvent{ID: uuid.New(), GroupID: group.ID, EventID: in.EventID, UserID: in.UserID, SectorID: in.SectorID, SeatIDs: ids, At: now}); err != nil {
				return err
			}
			created = true
			return nil
		}
		return api.ErrNoAdjacentSeats
	})
	if err != nil {
		return api.GroupHold{}, false, err
	}
	if created {
		for _, hold := range group.Holds {
			s.publishSeat(ctx, hold.EventID, hold.SeatID, api.SeatStateHeld, hold.ID)
		}
	}
	return group, created, nil
}
