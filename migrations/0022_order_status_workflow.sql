-- 0022_order_status_workflow.sql
-- Order status progression: SC (Scheduled) → AR (Arrived) → IP (In Progress) → CM (Complete)
-- Adds workflow timestamp columns and a full audit log table.

ALTER TABLE hl7_orders
    ADD COLUMN IF NOT EXISTS arrived_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS arrived_by VARCHAR(100),
    ADD COLUMN IF NOT EXISTS started_at  TIMESTAMP,
    ADD COLUMN IF NOT EXISTS started_by  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS done_at     TIMESTAMP,
    ADD COLUMN IF NOT EXISTS done_by     VARCHAR(100);

CREATE TABLE IF NOT EXISTS order_status_log (
    id          SERIAL PRIMARY KEY,
    order_id    INT REFERENCES hl7_orders(id) ON DELETE SET NULL,
    message_id  VARCHAR(200),
    from_status VARCHAR(10),
    to_status   VARCHAR(10) NOT NULL,
    changed_by  VARCHAR(100) NOT NULL,
    changed_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    source      VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_osl_order_id   ON order_status_log(order_id);
CREATE INDEX IF NOT EXISTS idx_osl_changed_at ON order_status_log(changed_at);
