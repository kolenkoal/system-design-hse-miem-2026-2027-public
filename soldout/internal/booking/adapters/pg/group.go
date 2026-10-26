package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
)

func (q *queries) lockGroup(ctx context.Context, scope string) error {
	_, err := q.db.Exec(ctx, "/* booking.group_lock */ SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", scope)
	if err != nil {
		return fmt.Errorf("booking/pg: блокировка группы: %w", err)
	}
	return nil
}

// LockUser сериализует single и group по одному пользователю, устраняя гонку лимита.
func (q *queries) LockUser(ctx context.Context, eventID, userID uuid.UUID) error {
	return q.lockGroup(ctx, "booking.user:"+eventID.String()+":"+userID.String())
}
func (q *queries) LockGroupKey(ctx context.Context, key string) error {
	return q.lockGroup(ctx, "booking.group.key:"+key)
}
func (q *queries) LockGroupSector(ctx context.Context, eventID, sectorID uuid.UUID) error {
	return q.lockGroup(ctx, "booking.group.sector:"+eventID.String()+":"+sectorID.String())
}

// SeatTaken читает истину booking, включая confirmed и ещё не обработанные expirer active.
func (q *queries) SeatTaken(ctx context.Context, eventID uuid.UUID, seatID int64) (bool, error) {
	var taken bool
	err := q.db.QueryRow(ctx, `/* booking.group_seat_taken */ SELECT EXISTS (
		SELECT 1 FROM holds WHERE event_id=$1 AND seat_id=$2 AND status IN ('active','confirmed'))`, eventID, seatID).Scan(&taken)
	if err != nil {
		return false, fmt.Errorf("booking/pg: занятость места: %w", err)
	}
	return taken, nil
}

func (q *queries) FindGroup(ctx context.Context, key string) (api.GroupHold, error) {
	var g api.GroupHold
	err := q.db.QueryRow(ctx, `/* booking.find_group */
		SELECT id,event_id,sector_id,user_id,size,row_no,first_seat_no,created_at,expires_at FROM hold_groups WHERE idempotency_key=$1`, key).
		Scan(&g.ID, &g.EventID, &g.SectorID, &g.UserID, &g.Size, &g.RowNo, &g.FirstSeatNo, &g.CreatedAt, &g.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return g, api.ErrGroupNotFound
	}
	if err != nil {
		return g, fmt.Errorf("booking/pg: поиск группы: %w", err)
	}
	rows, err := q.db.Query(ctx, `/* booking.group_items */ SELECT h.id,h.event_id,h.seat_id,h.user_id,h.status,h.expires_at,h.created_at
		FROM hold_group_items i JOIN holds h ON h.id=i.hold_id WHERE i.group_id=$1 ORDER BY i.seat_no`, g.ID)
	if err != nil {
		return g, fmt.Errorf("booking/pg: состав группы: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		h, err := scanHold(rows)
		if err != nil {
			return g, fmt.Errorf("booking/pg: чтение позиции: %w", err)
		}
		g.Holds = append(g.Holds, h.ToAPI())
	}
	return g, rows.Err()
}

func (q *queries) InsertGroup(ctx context.Context, g api.GroupHold, key string, seats []catalogapi.Seat) error {
	_, err := q.db.Exec(ctx, `/* booking.insert_group */ INSERT INTO hold_groups
		(id,event_id,sector_id,user_id,size,row_no,first_seat_no,idempotency_key,created_at,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		g.ID, g.EventID, g.SectorID, g.UserID, g.Size, g.RowNo, g.FirstSeatNo, key, g.CreatedAt, g.ExpiresAt)
	if err != nil {
		return fmt.Errorf("booking/pg: создать группу: %w", err)
	}
	for i, h := range g.Holds {
		if err := q.InsertHold(ctx, domain.Hold{ID: h.ID, EventID: h.EventID, SeatID: h.SeatID, UserID: h.UserID, Status: h.Status, ExpiresAt: h.ExpiresAt, CreatedAt: h.CreatedAt}); err != nil {
			return err
		}
		_, err := q.db.Exec(ctx, `/* booking.insert_group_item */ INSERT INTO hold_group_items (group_id,hold_id,seat_id,row_no,seat_no) VALUES ($1,$2,$3,$4,$5)`, g.ID, h.ID, h.SeatID, seats[i].RowNo, seats[i].SeatNo)
		if err != nil {
			return fmt.Errorf("booking/pg: записать позицию группы: %w", err)
		}
	}
	return nil
}

func (q *queries) AppendGroupHeld(ctx context.Context, e api.GroupHeldEvent) error {
	return q.outbox.Append(ctx, q.db, e)
}
