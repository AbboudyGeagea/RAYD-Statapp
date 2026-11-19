-- Migration 0172: the device the RIS booked each exam on (LAUMC to-do item 1). *** LAUMC BRANCH ONLY ***
--
-- Report 27's HIS vs RIS tab guessed the room of an order not performed yet (the room most used
-- for its procedure over 90 days). The RIS knows the booked room: SPS.MODALITY_KEY -> MODALITY,
-- the same device key the performed step carries. Checked on production 19 Nov
-- (scripts/sql/ris_booked_room_check.sql): SPS_ID is SPS's key, one booking per exam; every
-- booked open order names a device with an AE title (Scheduled: RH 21,861, SJH 15,759); a done
-- exam was performed on its booked device 99.6 % of the time (116 of 30,875 moved). Exams never
-- booked (Requested Signed, Cancelled by OP) have no SPS row.
--
-- ETL_JOBS/etl_orders.refresh_booked_rooms fills it every night from the bookings of the last
-- 30 days on (SPS has no change date). The AE title as the RIS's MODALITY names it, matched to
-- aetitle_modality_map like std_pps.performing_ae_title. Every site database runs this.

ALTER TABLE public.etl_orders ADD COLUMN IF NOT EXISTS booked_ae_title VARCHAR(100);

-- rh.etl_orders is SELECT o.*, expanded when the view was made: rebuilt to show the new column.
SELECT rh_refresh_views();
