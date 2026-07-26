-- Migration 0071: device floor-plan positions (Live AE Status spatial redesign).
--
-- Operator instruction (2026-07-26): "Live AE Status" should become a literal 2D
-- spatial floor plan, not just a grid of per-modality status tiles (the current
-- routes/live_feed.py implementation). Devices need a physical X/Y position on a
-- floor-plan canvas so their live busy/delayed/free/closed status can be rendered
-- at the right spot, per device (not pooled per modality).
--
-- Position is stored as a PERCENTAGE (0.0-100.0) of the canvas width/height, not
-- pixels — keeps it independent of canvas resolution/zoom, matches how the existing
-- frontend already scales responsively (Tailwind/flex-based, no fixed-px layouts).
-- NULL = not yet placed on the floor plan; those devices simply don't render there
-- yet (existing tile-grid view stays as the fallback until every device is placed).
--
-- Floor plans are per-site (RH / SJH have physically different buildings) — one
-- background image per site, referenced by floor_plans.site_id.

CREATE TABLE IF NOT EXISTS floor_plans (
    site_id       INTEGER PRIMARY KEY REFERENCES sites(id),
    image_path    TEXT,      -- relative path under static/ to the uploaded floor-plan image; NULL = no image yet (blank canvas)
    image_width   INTEGER,   -- original upload dimensions, for correct aspect-ratio rendering
    image_height  INTEGER,
    uploaded_by   INTEGER REFERENCES users(id),
    uploaded_at   TIMESTAMP,
    last_update   TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE aetitle_modality_map
    ADD COLUMN IF NOT EXISTS floor_x NUMERIC(5,2),   -- 0.00-100.00, percentage of canvas width
    ADD COLUMN IF NOT EXISTS floor_y NUMERIC(5,2),   -- 0.00-100.00, percentage of canvas height
    ADD COLUMN IF NOT EXISTS floor_positioned_by INTEGER REFERENCES users(id),
    ADD COLUMN IF NOT EXISTS floor_positioned_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_aetitle_modality_map_floor_positioned
    ON aetitle_modality_map (site_id)
    WHERE floor_x IS NOT NULL AND floor_y IS NOT NULL;
