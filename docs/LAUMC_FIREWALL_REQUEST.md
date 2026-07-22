# DNS + Firewall Request — RAYD Deployment (email)

Ready to send. Replace the `‹…›` values (site-specific IPs/subnets) before sending, or leave
them for the network engineer to complete where they already know them.

---

**To:** Network Engineering
**Cc:** ‹project sponsor›
**Subject:** DNS + firewall request — RAYD server (RAYD-Statapp.ad.umcrh.com / 10.4.15.121)

Dear Network Team,

We are commissioning the **RAYD** radiology statistics platform on the server
**10.4.15.121**. RAYD reads data from the PACS and RIS Oracle databases, receives HL7 messages
from the PACS/HIS/RIS, and serves a web dashboard to clinical staff. To bring it online we need
two things from your side: a **DNS record** and a set of **firewall rules**. Details below —
all ports are TCP unless stated otherwise.

---

## 1. DNS record

Please create an internal DNS **A record**:

| Hostname | Type | Points to |
|----------|------|-----------|
| **RAYD-Statapp.ad.umcrh.com** | A | **10.4.15.121** |

This hostname is what clinical staff will use in the browser
(`https://RAYD-Statapp.ad.umcrh.com`) and the name the TLS certificate will be issued for.

---

## 2. Firewall rules

### 2.1  Inbound — traffic arriving AT the RAYD server (permanent)

| Source | Destination | Port | Purpose |
|--------|-------------|------|---------|
| ‹PACS IP› — Carestream | 10.4.15.121 | **6661** | HL7 radiology results (ORU) |
| ‹HIS IP› — SAP / Mirth | 10.4.15.121 | **6661** | HL7 orders & ADT (ORM/ADT) |
| ‹RIS IP› | 10.4.15.121 | **6661** | HL7 order status events |
| ‹Clinical user subnet(s)› | 10.4.15.121 | **443** | Web application (HTTPS) |
| ‹Clinical user subnet(s)› | 10.4.15.121 | **80** | HTTP → HTTPS redirect |
| ‹Admin/IT subnet› | 10.4.15.121 | **22** | Server administration (SSH) |

> **Please restrict port 6661 to the three HL7 sender IPs only.** The HL7 listener has no
> application-level authentication, so 6661 must not be reachable from the general network.

### 2.2  Outbound — traffic leaving FROM the RAYD server (permanent)

| Source | Destination | Port | Purpose |
|--------|-------------|------|---------|
| 10.4.15.121 | ‹PACS Oracle IP› | **1521** | Read study data from PACS (Oracle SQL*Net) |
| 10.4.15.121 | ‹RIS Oracle IP› | **1521** | Read orders / worklist / reports from RIS |
| 10.4.15.121 | ‹DNS resolver› | **53** (TCP+UDP) | Name resolution |
| 10.4.15.121 | ‹NTP server› | **123** (UDP) | Time synchronisation |

> Accurate time is important — RAYD's turnaround-time statistics rely on the server, PACS and
> RIS clocks agreeing. If these systems share a VLAN with no firewall between them, rules 2.2
> may already be satisfied; please confirm.

### 2.3  Outbound — internet, for installation & updates only

Required only while installing or updating the software; may remain closed at all other times.
Please whitelist the exact FQDNs below (all **443** unless a second port is shown). If outbound
internet is not permitted at all, tell us — we can install fully offline from a pre-staged
bundle and this section becomes unnecessary.

**Oracle Instant Client** (database driver)
- `download.oracle.com`
- `www.oracle.com`
- *(May redirect to an Akamai CDN. If the download fails under strict filtering, also allow
  `*.oracle.com.edgekey.net` and `*.akamaiedge.net`, or let us supply the 30 MB file offline.)*

**Ubuntu apt — host OS packages**
- `archive.ubuntu.com` — 80, 443
- `security.ubuntu.com` — 80, 443
- `ports.ubuntu.com` — 80, 443

**Debian apt — container build packages** *(the app image is Debian-based, separate from the host)*
- `deb.debian.org` — 80, 443
- `security.debian.org` — 80, 443

**Docker Hub — base images**
- `registry-1.docker.io`
- `auth.docker.io`
- `index.docker.io`
- `production.cloudflare.docker.com` *(image layers download from here)*

**PyPI — Python packages**
- `pypi.org`
- `files.pythonhosted.org` *(package files download from here)*

**GitHub — application code & updates**
- `github.com`
- `codeload.github.com`
- `objects.githubusercontent.com`

---

## 3. Values we still need (please confirm)

- ‹PACS Oracle IP› and ‹RIS Oracle IP› — the two source database hosts.
- ‹PACS / HIS / RIS HL7 sender IPs› — the three systems allowed to reach port 6661.
- ‹Clinical user subnet(s)› — where staff will browse the dashboard from.
- ‹Admin/IT subnet› — where we will administer the server from.
- ‹DNS resolver› and ‹NTP server› addresses for the RAYD host.

Once the DNS record and the inbound/outbound-operational rules (sections 1, 2.1, 2.2) are in
place we can begin. The internet rules (2.3) are only needed for the install window.

Thank you for your assistance — happy to jump on a quick call if that's easier.

Kind regards,
RAYD Deployment Team

---

### Appendix — future notice (no action needed now)

If the Critical Result Notification feature is enabled later, RAYD will additionally need
outbound access from 10.4.15.121 to: an SMTP relay (587/465) for email, and — if adopted — an
SMS gateway and/or WhatsApp Business API provider (443). We will send a separate request at that
time.

*(Internal, not part of the email: RAYD↔PostgreSQL and RAYD↔NLP-worker traffic stays on the
host's internal Docker network — no firewall rule needed. The dev-only PostgreSQL 5432 exposure
must never be opened in production.)*
