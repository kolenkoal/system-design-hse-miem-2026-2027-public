package domain

import (
	"fmt"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
)

// AdjacentBlocks перечисляет непрерывные окна по упорядоченной геометрии сектора.
// Нумерация мест, а не их id, задаёт соседство, standing исключён даже для size=1.
func AdjacentBlocks(seats []catalogapi.Seat, size int) ([][]catalogapi.Seat, error) {
	if size < 1 || size > api.MaxTicketsPerUser {
		return nil, fmt.Errorf("%w: size должен быть от 1 до 4", api.ErrValidation)
	}
	var out [][]catalogapi.Seat
	for i := 0; i+size <= len(seats); i++ {
		block := seats[i : i+size]
		valid := block[0].RowNo > 0
		for j, seat := range block {
			if seat.SectorID != block[0].SectorID || seat.RowNo != block[0].RowNo || seat.SeatNo != block[0].SeatNo+j {
				valid = false
				break
			}
		}
		if valid {
			out = append(out, block)
		}
	}
	return out, nil
}
