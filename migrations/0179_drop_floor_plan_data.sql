-- Migration 0179: the floor plan data removed (operator, 2026-11-20: "remove the floor plan data,
-- we replace them with hardcoded live dashboard"). *** LAUMC BRANCH ONLY ***
--
-- Admin > Floor Plan and the board's plan mode were removed on 2026-11-19 (the Live Department
-- board, one card per RIS room, replaced them); their data was kept until the operator's word.
-- Nothing reads it (checked 2026-11-20): the table floor_plans and the four position columns of
-- aetitle_modality_map from migration 0071, with its partial index (dropped with the columns). No
-- view uses them. The uploaded images under static/floor_plans/ on the server are files, removed by
-- hand (docs/LAUMC_NEXT_SESSION.md).

DROP TABLE IF EXISTS public.floor_plans;

ALTER TABLE public.aetitle_modality_map
    DROP COLUMN IF EXISTS floor_x,
    DROP COLUMN IF EXISTS floor_y,
    DROP COLUMN IF EXISTS floor_positioned_by,
    DROP COLUMN IF EXISTS floor_positioned_at;
