package domain

import (
	"errors"
	"github.com/google/uuid"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	"testing"
)

func TestAdjacentBlocks(t *testing.T) {
	s := uuid.New()
	seat := func(id int64, row, num int) catalogapi.Seat {
		return catalogapi.Seat{ID: id, SectorID: s, RowNo: row, SeatNo: num}
	}
	for _, tc := range []struct {
		name        string
		seats       []catalogapi.Seat
		size, count int
	}{
		{"четыре смежных с несмежными id", []catalogapi.Seat{seat(9, 1, 1), seat(2, 1, 2), seat(13, 1, 3), seat(4, 1, 4)}, 4, 1},
		{"разрыв нумерации", []catalogapi.Seat{seat(1, 1, 1), seat(2, 1, 3)}, 2, 0},
		{"граница рядов", []catalogapi.Seat{seat(1, 1, 50), seat(2, 2, 1)}, 2, 0},
		{"танцпол", []catalogapi.Seat{seat(1, 0, 1)}, 1, 0},
		{"скользящие окна", []catalogapi.Seat{seat(1, 1, 1), seat(2, 1, 2), seat(3, 1, 3)}, 2, 2},
		{"пустой сектор", nil, 4, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks, err := AdjacentBlocks(tc.seats, tc.size)
			if err != nil || len(blocks) != tc.count {
				t.Fatalf("блоков %d, нужно %d, ошибка %v", len(blocks), tc.count, err)
			}
		})
	}
	for _, size := range []int{-1, 0, 5} {
		if _, err := AdjacentBlocks(nil, size); !errors.Is(err, api.ErrValidation) {
			t.Fatalf("size=%d: %v", size, err)
		}
	}
}
