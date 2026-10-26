package http

import (
	"github.com/google/uuid"
	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/platform/httpx"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
	"net/http"
)

func (h *Handler) createGroupHold(w http.ResponseWriter, r *http.Request) {
	var in struct {
		EventID  uuid.UUID `json:"event_id"`
		SectorID uuid.UUID `json:"sector_id"`
		UserID   uuid.UUID `json:"user_id"`
		Size     int       `json:"size"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	otel.SetUser(r.Context(), in.UserID.String())
	g, created, err := h.svc.CreateGroupHold(r.Context(), api.CreateGroupHoldInput{
		EventID: in.EventID, SectorID: in.SectorID, UserID: in.UserID, Size: in.Size,
		AdmissionToken: r.Header.Get("X-Admission-Token"), IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		httpx.WriteError(w, r, mapErr(err))
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	httpx.JSON(w, status, g)
}
