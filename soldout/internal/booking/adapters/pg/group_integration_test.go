package pg_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	bookingpg "github.com/nikolaysavelev/soldout/internal/booking/adapters/pg"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/app"
	catalogpg "github.com/nikolaysavelev/soldout/internal/catalog/adapters/pg"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	catalogapp "github.com/nikolaysavelev/soldout/internal/catalog/app"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
	"github.com/nikolaysavelev/soldout/internal/platform/outbox"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestGroupHold(t *testing.T) {
	if testing.Short() {
		t.Skip("интеграционный тест PostgreSQL: нужен Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pgc, err := tcpostgres.Run(ctx, "postgres:17-alpine", tcpostgres.WithDatabase("soldout"), tcpostgres.WithUsername("soldout"), tcpostgres.WithPassword("soldout"), testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgc.Terminate(context.Background()) })
	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Migrate(ctx, dsn, slog.Default()); err != nil {
		t.Fatal(err)
	}
	pool, err := db.NewPool(ctx, dsn, db.PoolOptions{MaxConns: 30}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = db.Seed(ctx, pool, 3000, nil); err != nil {
		t.Fatal(err)
	}
	event := uuid.MustParse("10000000-0000-4000-8000-000000000001")
	sector := uuid.MustParse("30000000-0000-4000-8000-0000000003e9")
	cat := catalogapp.New(catalogpg.New(pool), nil, catalogapp.Options{}, nil, slog.Default())
	store := bookingpg.New(pool, bookingpg.WithOutbox(outbox.NewWriter(outbox.Options{})))
	svc := app.New(store, cat, fakeQueue{}, fakePayment{}, fakeTickets{}, app.Options{HoldTTL: time.Minute, TicketPriceMinor: 100}, slog.Default())
	input := func(size int) api.CreateGroupHoldInput {
		return api.CreateGroupHoldInput{EventID: event, SectorID: sector, UserID: uuid.New(), Size: size, AdmissionToken: "ok", IdempotencyKey: uuid.NewString()}
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	count := func(sql string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	reset := func() { exec("TRUNCATE hold_group_items,hold_groups,order_items,orders,holds,outbox CASCADE") }
	parallel := func(n int, fn func(int) error) []error {
		start := make(chan struct{})
		results := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); <-start; results[i] = fn(i) }(i)
		}
		close(start)
		wg.Wait()
		return results
	}
	t.Run("размеры, TTL, повтор и конфликт ключа", func(t *testing.T) {
		reset()
		for size := 1; size <= 4; size++ {
			in := input(size)
			g, created, err := svc.CreateGroupHold(ctx, in)
			if err != nil || !created || len(g.Holds) != size || g.RowNo < 1 {
				t.Fatalf("группа: %+v %v %v", g, created, err)
			}
			for _, h := range g.Holds {
				if !h.ExpiresAt.Equal(g.ExpiresAt) {
					t.Fatal("разный TTL")
				}
			}
			again, created, err := svc.CreateGroupHold(ctx, in)
			if err != nil || created || again.ID != g.ID {
				t.Fatalf("повтор: %v %v", created, err)
			}
			in.Size = 5 - size
			if in.Size == size {
				in.UserID = uuid.New()
			}
			if _, _, err := svc.CreateGroupHold(ctx, in); !errors.Is(err, api.ErrIdempotencyConflict) {
				t.Fatalf("другие параметры: %v", err)
			}
		}
		if n := count("SELECT count(*) FROM outbox WHERE type='GroupHeld'"); n != 4 {
			t.Fatalf("событий %d, нужно 4", n)
		}
	})
	t.Run("нет блока и rollback при сбое outbox", func(t *testing.T) {
		reset()
		exec("INSERT INTO holds(id,event_id,seat_id,user_id,status,expires_at) SELECT gen_random_uuid(),$1,id,gen_random_uuid(),'active',now()+interval '1 minute' FROM seats WHERE sector_id=$2 AND seat_no%2=0", event, sector)
		in := input(2)
		if _, _, err := svc.CreateGroupHold(ctx, in); !errors.Is(err, api.ErrNoAdjacentSeats) {
			t.Fatalf("разрывы: %v", err)
		}
		if count("SELECT count(*) FROM hold_groups") != 0 || count("SELECT count(*) FROM holds WHERE user_id=$1", in.UserID) != 0 {
			t.Fatal("частичная группа")
		}
		reset()
		exec("ALTER TABLE outbox ADD CONSTRAINT test_no_group CHECK (type <> 'GroupHeld')")
		if _, _, err := svc.CreateGroupHold(ctx, input(4)); err == nil {
			t.Fatal("нужна ошибка outbox")
		}
		exec("ALTER TABLE outbox DROP CONSTRAINT test_no_group")
		if count("SELECT count(*) FROM hold_groups") != 0 || count("SELECT count(*) FROM holds") != 0 || count("SELECT count(*) FROM outbox") != 0 {
			t.Fatal("rollback не отменил всю группу")
		}
	})
	t.Run("конкуренция групп на единственный блок", func(t *testing.T) {
		reset()
		exec("INSERT INTO holds(id,event_id,seat_id,user_id,status,expires_at) SELECT gen_random_uuid(),$1,id,gen_random_uuid(),'active',now()+interval '1 minute' FROM seats WHERE sector_id=$2 AND id>4", event, sector)
		wins := 0
		for _, err := range parallel(12, func(int) error { _, _, err := svc.CreateGroupHold(ctx, input(4)); return err }) {
			if err == nil {
				wins++
			} else if !errors.Is(err, api.ErrNoAdjacentSeats) {
				t.Error(err)
			}
		}
		if wins != 1 || count("SELECT count(*) FROM hold_groups") != 1 {
			t.Fatalf("успехов %d, нужно 1", wins)
		}
	})
	t.Run("одновременный повтор одного ключа", func(t *testing.T) {
		reset()
		in := input(4)
		for _, err := range parallel(12, func(int) error { _, _, err := svc.CreateGroupHold(ctx, in); return err }) {
			if err != nil {
				t.Error(err)
			}
		}
		if count("SELECT count(*) FROM hold_groups") != 1 || count("SELECT count(*) FROM holds") != 4 || count("SELECT count(*) FROM outbox") != 1 {
			t.Fatal("дубли по ключу")
		}
	})
	t.Run("смешанные single и group сохраняют лимит", func(t *testing.T) {
		reset()
		user := uuid.New()
		for _, err := range parallel(12, func(i int) error {
			if i%2 == 0 {
				in := input(2)
				in.UserID = user
				_, _, err := svc.CreateGroupHold(ctx, in)
				return err
			}
			_, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: event, SeatID: int64(100 + i), UserID: user, AdmissionToken: "ok"})
			return err
		}) {
			if err != nil && !errors.Is(err, api.ErrHoldLimit) {
				t.Error(err)
			}
		}
		if n := count("SELECT count(*) FROM holds WHERE user_id=$1 AND status='active'", user); n != 4 {
			t.Fatalf("hold=%d, нужно 4", n)
		}
	})
	t.Run("группа и single на пересекающееся место", func(t *testing.T) {
		reset()
		for _, err := range parallel(12, func(i int) error {
			if i%2 == 0 {
				_, _, err := svc.CreateGroupHold(ctx, input(4))
				return err
			}
			_, err := svc.CreateHold(ctx, api.CreateHoldInput{EventID: event, SeatID: 1, UserID: uuid.New(), AdmissionToken: "ok"})
			return err
		}) {
			if err != nil && !errors.Is(err, api.ErrSeatHeld) {
				t.Error(err)
			}
		}
		if n := count("SELECT count(*) FROM (SELECT seat_id FROM holds GROUP BY seat_id HAVING count(*)>1) d"); n != 0 {
			t.Fatal("пересечение мест")
		}
	})
	t.Run("полный цикл, TTL и валидация геометрии", func(t *testing.T) {
		reset()
		in := input(3)
		g, _, err := svc.CreateGroupHold(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]uuid.UUID, len(g.Holds))
		for i, h := range g.Holds {
			ids[i] = h.ID
		}
		o, _, err := svc.CreateOrder(ctx, api.CreateOrderInput{HoldIDs: ids, UserID: in.UserID, IdempotencyKey: uuid.NewString()})
		if err != nil || o.AmountMinor != 300 {
			t.Fatalf("order: %v %+v", err, o)
		}
		if _, err := svc.PayOrder(ctx, api.PayOrderInput{OrderID: o.ID}); err != nil {
			t.Fatal(err)
		}
		if n := count("SELECT count(*) FROM holds WHERE user_id=$1 AND status='confirmed'", in.UserID); n != 3 {
			t.Fatal("подтверждены не все места")
		}
		in = input(2)
		in.SectorID = uuid.MustParse("30000000-0000-4000-8000-0000000003ea")
		if _, _, err := svc.CreateGroupHold(ctx, in); !errors.Is(err, catalogapi.ErrValidation) {
			t.Fatalf("standing: %v", err)
		}
		in.SectorID = uuid.MustParse("30000000-0000-4000-8000-0000000007d1")
		if _, _, err := svc.CreateGroupHold(ctx, in); !errors.Is(err, catalogapi.ErrValidation) {
			t.Fatalf("чужая площадка: %v", err)
		}
		in = input(1)
		in.AdmissionToken = ""
		if _, _, err := svc.CreateGroupHold(ctx, in); !errors.Is(err, api.ErrAdmissionRequired) {
			t.Fatalf("admission: %v", err)
		}
		for _, size := range []int{0, 5} {
			in = input(size)
			if _, _, err := svc.CreateGroupHold(ctx, in); !errors.Is(err, api.ErrValidation) {
				t.Fatal(fmt.Sprintf("size=%d: %v", size, err))
			}
		}
		in = input(2)
		g, _, err = svc.CreateGroupHold(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		exec("UPDATE holds SET expires_at=now()-interval '1 second' WHERE user_id=$1", in.UserID)
		if _, err := store.ExpireHoldsBatch(ctx, time.Now(), 100); err != nil {
			t.Fatal(err)
		}
		again, created, err := svc.CreateGroupHold(ctx, in)
		if err != nil || created || again.ID != g.ID || again.Holds[0].Status != api.HoldReleased {
			t.Fatalf("повтор истёкшей группы: %+v %v", again, err)
		}
	})
}
