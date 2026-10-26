package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
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

// TakenSeats читает занятость только запрошенных мест одним SQL, без N+1 на заполненных рядах.
func (q *queries) TakenSeats(ctx context.Context, eventID uuid.UUID, seatIDs []int64) (map[int64]bool, error) {
	rows, err := q.db.Query(ctx, `/* booking.group_taken_seats */
		SELECT seat_id FROM holds WHERE event_id=$1 AND seat_id=ANY($2) AND status IN ('active','confirmed')`, eventID, seatIDs)
	if err != nil {
		return nil, fmt.Errorf("booking/pg: занятость мест: %w", err)
	}
	defer rows.Close()
	taken := make(map[int64]bool)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("booking/pg: чтение занятого места: %w", err)
		}
		taken[id] = true
	}
	return taken, rows.Err()
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

// InsertGroup записывает метаданные, N hold и N позиций одним запросом вместо 1+2N round trip.
// Все CTE принадлежат booking, FK и UNIQUE остаются включёнными.
func (q *queries) InsertGroup(ctx context.Context, g api.GroupHold, key string, seats []catalogapi.Seat) error {
	if len(g.Holds) != g.Size || len(seats) != g.Size {
		return fmt.Errorf("%w: размер группы и число позиций не совпадают", api.ErrValidation)
	}
	ids := make([]uuid.UUID, g.Size)
	seatIDs := make([]int64, g.Size)
	numbers := make([]int, g.Size)
	for i, hold := range g.Holds {
		ids[i], seatIDs[i], numbers[i] = hold.ID, hold.SeatID, seats[i].SeatNo
	}
	tag, err := q.db.Exec(ctx, `/* booking.insert_group_bulk */
		WITH new_group AS (
		 INSERT INTO hold_groups (id,event_id,sector_id,user_id,size,row_no,first_seat_no,idempotency_key,created_at,expires_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id
		), new_holds AS (
		 INSERT INTO holds (id,event_id,seat_id,user_id,status,created_at,expires_at)
		 SELECT x.id,$2,x.seat_id,$4,'active',$9,$10 FROM unnest($11::uuid[],$12::bigint[]) AS x(id,seat_id)
		 RETURNING id,seat_id
		)
		INSERT INTO hold_group_items (group_id,hold_id,seat_id,row_no,seat_no)
		SELECT g.id,h.id,h.seat_id,$6,x.seat_no
		FROM new_group g CROSS JOIN new_holds h
		JOIN unnest($12::bigint[],$13::int[]) AS x(seat_id,seat_no) ON x.seat_id=h.seat_id`,
		g.ID, g.EventID, g.SectorID, g.UserID, g.Size, g.RowNo, g.FirstSeatNo, key, g.CreatedAt, g.ExpiresAt, ids, seatIDs, numbers)
	if db.IsUniqueViolation(err, "") {
		return api.ErrSeatHeld
	}
	if err != nil {
		return fmt.Errorf("booking/pg: записать группу целиком: %w", err)
	}
	if tag.RowsAffected() != int64(g.Size) {
		return fmt.Errorf("booking/pg: записано %d позиций вместо %d", tag.RowsAffected(), g.Size)
	}
	return nil
}

func (q *queries) AppendGroupHeld(ctx context.Context, e api.GroupHeldEvent) error {
	return q.outbox.Append(ctx, q.db, e)
}
