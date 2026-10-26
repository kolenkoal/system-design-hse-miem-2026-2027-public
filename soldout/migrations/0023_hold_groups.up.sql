CREATE TABLE hold_groups (
    id uuid PRIMARY KEY,
    event_id uuid NOT NULL,
    sector_id uuid NOT NULL,
    user_id uuid NOT NULL,
    size int NOT NULL CHECK (size BETWEEN 1 AND 4),
    row_no int NOT NULL CHECK (row_no > 0),
    first_seat_no int NOT NULL CHECK (first_seat_no > 0),
    idempotency_key text NOT NULL UNIQUE CHECK (octet_length(idempotency_key) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);
COMMENT ON TABLE hold_groups IS 'booking: атомарно созданные группы смежных мест, ключ идемпотентности и исторический состав';
CREATE TABLE hold_group_items (
    group_id uuid NOT NULL REFERENCES hold_groups(id),
    hold_id uuid NOT NULL UNIQUE REFERENCES holds(id),
    seat_id bigint NOT NULL,
    row_no int NOT NULL,
    seat_no int NOT NULL,
    PRIMARY KEY (group_id, seat_id)
);
COMMENT ON TABLE hold_group_items IS 'booking: историческая геометрия и удержания группы, проверяемые инвариантом I7';
