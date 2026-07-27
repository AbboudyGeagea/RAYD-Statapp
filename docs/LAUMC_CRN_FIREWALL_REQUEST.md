# Firewall + Procurement Request — CRN (Critical Result Notification)

Follow-up to `LAUMC_FIREWALL_REQUEST.md`'s appendix ("If the Critical Result Notification
feature is enabled later... we will send a separate request at that time.") — that time is now.
CRN is being built to notify a referring physician (email / SMS / WhatsApp, whichever they
prefer — see `referring_contacts` admin tool) when a critical result is signed, with a
tokenized one-click acknowledgment link, and escalation if unacknowledged.

Two separate things are needed before the send-side code can go live: **procurement decisions**
(which provider for each channel — ours to make, not IT's) and **firewall rules** (IT's side,
depends on the answers to the procurement questions below).

---

## 1. Procurement decisions needed first

Firewall rules below can't be finalized until these are answered — the destination depends on
the provider chosen.

| Channel | Question | Notes |
|---|---|---|
| **Email** | Do we route through LAUMC's own Exchange/O365 relay, or an external transactional service (e.g. SendGrid, AWS SES)? | Hospital relay is likely the fastest path (no new vendor contract) if IT will issue RAYD a relay-only SMTP account. |
| **SMS** | Which SMS gateway/provider — local telecom API or an international one (e.g. Twilio)? Do we have a contract already? | If none exists, this is a new procurement step outside RAYD's scope — flag lead time. |
| **WhatsApp** | Meta's own WhatsApp Cloud API directly, or through a BSP (Business Solution Provider)? | Either way needs Meta Business verification + template pre-approval before ANY message can send — this is the slowest piece (weeks, not days), start it in parallel with everything else. |

---

## 2. Firewall rules (send to Network Engineering once §1 is answered)

**To:** Network Engineering
**Cc:** ‹project sponsor›
**Subject:** Firewall request — CRN outbound access (RAYD server, 10.4.15.121)

Dear Network Team,

Following up on the RAYD server firewall request (10.4.15.121) — we're now enabling Critical
Result Notification, which needs the following additional **outbound** rules:

| Source | Destination | Port | Purpose |
|--------|-------------|------|---------|
| 10.4.15.121 | ‹SMTP relay host/IP› | **587** (or 465 for implicit TLS) | Outbound email notifications |
| 10.4.15.121 | ‹SMS gateway API host› | **443** | Outbound SMS notifications |
| 10.4.15.121 | ‹WhatsApp Cloud API host, e.g. graph.facebook.com — or the chosen BSP's API host› | **443** | Outbound WhatsApp notifications |

> All three are outbound-only — RAYD initiates the connection to send a notification, nothing
> connects back in on these ports. The public acknowledgment landing page (physician clicks the
> link in the message) is served over the **existing** inbound 443 rule already in place for the
> web application — no new inbound rule needed for that.

Please confirm the exact destination host/IP for each once we've selected providers (see §1) —
placeholders above will be filled in before this is sent for real.

Kind regards,
RAYD Deployment Team

---

## 3. Not yet decided — needs operator input before send-side code is wired up

- Escalation policy: how long to wait for an unacknowledged critical result before escalating,
  and who/what receives the escalation (resend on a different channel? a department contact?).
- Message content: what a critical-result notification actually says (minimal PHI in the body
  per `LAUMC_SCOPE.md`'s CRN spec — the secure link carries the detail, not the message itself).
- `crn_notifications` table + tokenized ack landing page — architecture ready to build once the
  above is settled; see `docs/LAUMC_NEXT_SESSION.md`.
