-- Migration 0181: Report Composer keeps one example report (operator, 2026-11-20: "remove the
-- existing templates, keep one only, it doesn't matter which one"). *** LAUMC BRANCH ONLY ***
--
-- Migration 0136 seeded 14 shared reports in the library's Examples folder, ownerless. One stays:
-- "Studies per modality, by month" (grouping, a split and a chart in one), else, if it was
-- renamed or deleted, the oldest example left. An admin's edits never change owner_id, so an
-- edited or renamed example is still one of the 14; a report someone saved into a folder called
-- Examples has an owner and stays. An example in a scheduled email is kept (the schedule item
-- RESTRICTs the delete; production had none, 20 Nov). Their version history goes with them
-- (ON DELETE CASCADE).

WITH keep AS (
    SELECT id FROM worklists
     WHERE folder = 'Examples' AND owner_id IS NULL
     ORDER BY (name = 'Studies per modality, by month') DESC, id
     LIMIT 1
)
DELETE FROM worklists w
 WHERE w.folder = 'Examples'
   AND w.owner_id IS NULL
   AND w.id NOT IN (SELECT id FROM keep)
   AND NOT EXISTS (SELECT 1 FROM worklist_email_schedule_items i WHERE i.worklist_id = w.id);
