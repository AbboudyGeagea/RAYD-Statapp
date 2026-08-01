# Outbound ACK→HL7 Request — CRN (Critical Result Notification)

Follow-up to `docs/LAUMC_CRN_FIREWALL_REQUEST.md` (which covers the *sending* side: getting the
notification out to the referring physician). This document covers what happens *after* they
click the acknowledgment link: operator instruction (2026-08-01) is that RAYD should transform
that acknowledgment into both a DB record (already built — `crn_notifications.status =
'acknowledged'`, `routes/crn_ack.py`) **and** an outbound HL7 message fired to one or more of the
hospital's own systems (HIS, PACS, RIS), "as the agreement with the hospital."

**Not built yet** — deliberately. `utils/hl7_forward.py` already has the MLLP send mechanics
(framing, socket connect, ACK read, admin-configurable host/port via `settings`), used elsewhere
for raw order forwarding — extending it once the answers below exist is a small change. Writing
the actual outbound message now, against a guessed format, risks sending malformed HL7 into a
hospital's live HIS/PACS/RIS — worse than not sending anything.

---

## Questions that need an answer from the hospital's integration team

| Question | Why it matters |
|---|---|
| **Which destination(s)** actually need this — HIS, PACS, RIS, all three? | Determines whether one message fans out identically to all three, or each needs its own tailored format/segments. |
| **Message type + trigger event** — is there an existing HL7 message type the hospital's systems already expect for "this critical result was acknowledged by the referring physician" (e.g. an ORU update, a generic ACK^ACK response, a custom Z-segment message)? | RAYD should emit whatever the *receiving* system is built to parse — this is their integration to define, not ours to guess. |
| **Which fields are required** in that message — accession number and ack timestamp are the obvious minimum; does the hospital also want the referring physician's name, the channel they acknowledged on, or anything else carried in the message body? | Determines the segment/field layout once the message type above is settled. |
| **Per-destination connection details** — host, port, and whether each destination has its own MLLP listener or shares one gateway. | Same shape as the firewall request already sent for outbound notification sends — Network Engineering will need this to open the right rules. |
| **Delivery guarantee expectations** — is a best-effort fire-and-forget send acceptable (matching `hl7_forward.py`'s current behavior — logs an ACK if one comes back, doesn't retry), or does the hospital need retry-on-failure / a dead-letter record if a destination is unreachable? | Changes the implementation shape (today's forwarder is intentionally fire-and-forget with no retry). |

---

## What's already built and ready to extend once the above is answered

- `crn_dispatcher.acknowledge()` (`utils/crn_dispatcher.py`) already marks the notification
  acknowledged in the DB — the trigger point for firing the outbound HL7 message would be right
  after this call succeeds, in `routes/crn_ack.py`'s `ack()` view.
- `utils/hl7_forward.py`'s MLLP send function (framing, connect, ACK read, background thread) is
  reusable as-is for the socket mechanics — it currently supports one configured destination
  (`hl7_forward_host`/`hl7_forward_port` in `settings`); multi-destination fan-out (HIS + PACS +
  RIS) needs either multiple settings keys or a small `crn_ack_destinations` table, decided once
  "which destination(s)" above is answered.
- Message *construction* (building the actual HL7 string with the right segments) is the one
  piece with zero code today, pending the message-type answer above.
