import re


def _n(val):
    """Parse a number from any string representation (commas, units, %, +/-)."""
    if val is None:
        return 0.0
    s = re.sub(r'[^\d.\-]', '', str(val).replace(',', ''))
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def diagnose(stats: dict) -> str:
    """
    Rule-based diagnostic engine for radiology KPIs.
    Detects report context from stats keys and returns a structured diagnosis.
    """
    keys = set(stats.keys())

    if 'median_tat_min' in keys or 'active_scan_hours' in keys:
        return _dx_utilization(stats)
    if 'orphan_orders' in keys and 'at_risk_physicians' in keys:
        return _dx_physician(stats)
    if 'matched_orders' in keys and 'volume_delta_pct' in keys:
        return _dx_orders(stats)
    if 'total_storage_tb' in keys or 'avg_mb_per_study' in keys:
        return _dx_storage(stats)
    return _dx_generic(stats)


# ── Report 25: Equipment Utilization / TAT ────────────────────────────────────

def _dx_utilization(stats):
    total   = _n(stats.get('total_studies', 0))
    hours   = _n(stats.get('active_scan_hours', '0'))
    stress  = int(_n(stats.get('high_stress_devices', 0)))
    tat_med = _n(stats.get('median_tat_min', 0))
    signed  = _n(stats.get('signed_studies', 0))
    dr      = stats.get('date_range', '')

    severity = 0
    findings = []

    if tat_med > 0:
        tat_h = tat_med / 60
        if tat_med > 2880:
            findings.append(
                f"Median TAT is {tat_h:.1f} hours — severely exceeds the 24-hour reporting "
                f"benchmark. A chronic reporting backlog is indicated."
            )
            severity = max(severity, 2)
        elif tat_med > 1440:
            findings.append(
                f"Median TAT is {tat_h:.1f} hours — exceeds the 24-hour outpatient target. "
                f"The reporting queue requires immediate intervention."
            )
            severity = max(severity, 2)
        elif tat_med > 480:
            findings.append(
                f"Median TAT is {tat_h:.1f} hours — above the 8-hour inpatient guideline. "
                f"Monitor closely for further deterioration."
            )
            severity = max(severity, 1)
        else:
            findings.append(
                f"Median TAT is {tat_med:.0f} minutes — within the 8-hour inpatient benchmark."
            )

    if hours > 0 and total > 0:
        tph = total / hours
        findings.append(
            f"{total:,.0f} studies completed across {hours:,.0f} active scanning hours "
            f"({tph:.1f} studies/hour throughput)."
        )
    elif total > 0:
        findings.append(f"{total:,.0f} total studies in the period.")

    if stress >= 3:
        findings.append(
            f"{stress} devices are operating above the high-stress utilization threshold — "
            f"capacity ceiling is being approached. Additional scheduling slots or equipment should be evaluated."
        )
        severity = max(severity, 2)
    elif stress > 0:
        findings.append(
            f"{stress} device(s) flagged above the high-stress utilization threshold — "
            f"schedule review recommended before peak periods."
        )
        severity = max(severity, 1)
    else:
        findings.append("No devices are operating above the high-stress utilization threshold.")

    if signed > 0 and total > 0:
        pct = signed / total * 100
        if pct < 70:
            findings.append(
                f"Reporting coverage is only {pct:.0f}% ({signed:,.0f} of {total:,.0f} studies signed) — "
                f"significant backlog detected."
            )
            severity = max(severity, 2)
        elif pct < 90:
            findings.append(
                f"Reporting coverage is {pct:.0f}% ({signed:,.0f} signed) — "
                f"minor backlog present. Target is 95%+ within the SLA window."
            )
            severity = max(severity, 1)
        else:
            findings.append(
                f"Reporting coverage is strong at {pct:.0f}% ({signed:,.0f} signed studies)."
            )

    if severity == 2:
        headline = "CRITICAL — Reporting Backlog & Capacity Pressure"
        action   = (
            "Immediate Action Required: Redistribute radiologist workload, extend reporting "
            "hours, and audit device scheduling. Consider offsite reading for overflow volume."
        )
    elif severity == 1:
        headline = "ELEVATED — Performance Concerns Detected"
        action   = (
            "Recommended: Review shift assignments for peak-volume periods. "
            "Set TAT alerts for studies exceeding 8 hours without a signed report."
        )
    else:
        headline = "NORMAL — Department Operating Within Benchmarks"
        action   = (
            "Continue monitoring. Verify STAT studies are being prioritised and TAT targets "
            "are tracked per priority class (STAT / Urgent / Routine)."
        )

    return _format(headline, dr, findings, action)


# ── Report 22: Physician Referral Audit ──────────────────────────────────────

def _dx_physician(stats):
    orphans  = int(_n(stats.get('orphan_orders', 0)))
    at_risk  = int(_n(stats.get('at_risk_physicians', 0)))
    churn    = int(_n(stats.get('churn_physicians', 0)))
    dr       = stats.get('date_range', '')

    severity = 0
    findings = []

    if orphans > 200:
        findings.append(
            f"{orphans:,} orphan orders detected (studies with no matching HL7 order) — "
            f"indicates a systemic scheduling or integration gap."
        )
        severity = max(severity, 2)
    elif orphans > 50:
        findings.append(
            f"{orphans:,} orphan orders — elevated mismatch rate requiring an integration audit."
        )
        severity = max(severity, 1)
    else:
        findings.append(f"{orphans:,} orphan orders — within an acceptable range.")

    if churn > 5:
        findings.append(
            f"{churn} referring physicians have sent no orders this period — "
            f"significant referral attrition detected."
        )
        severity = max(severity, 2)
    elif churn > 0:
        findings.append(
            f"{churn} referring physician(s) show churn (no recent orders) — follow-up recommended."
        )
        severity = max(severity, 1)

    if at_risk > 3:
        findings.append(
            f"{at_risk} physicians flagged at-risk (declining referral trend) — "
            f"proactive outreach is needed to prevent further attrition."
        )
        severity = max(severity, 1)
    elif at_risk > 0:
        findings.append(
            f"{at_risk} physician(s) show a declining referral pattern — monitor closely."
        )

    if severity == 2:
        headline = "ALERT — Referral Integrity & Volume Risk"
        action   = (
            "Immediate Action: Contact churned physicians to identify reasons for disengagement. "
            "Audit HL7 order workflow for the top orphan-generating modalities."
        )
    elif severity == 1:
        headline = "WATCH — Physician Engagement Concerns"
        action   = (
            "Recommended: Schedule outreach with at-risk referring physicians. "
            "Review order-matching logic for common failure points."
        )
    else:
        headline = "NORMAL — Referral Network Healthy"
        action   = (
            "Continue monitoring. Track month-over-month trends for at-risk "
            "physician referral volumes."
        )

    return _format(headline, dr, findings, action)


# ── Report 27: HL7 Order Audit ────────────────────────────────────────────────

def _dx_orders(stats):
    total   = _n(stats.get('total_orders', 0))
    matched = _n(stats.get('matched_orders', 0))
    orphans = _n(stats.get('orphan_orders', 0))
    avg_dur = _n(stats.get('avg_duration_min', 0))
    delta   = _n(stats.get('volume_delta_pct', 0))
    dr      = stats.get('date_range', '')

    severity = 0
    findings = []

    if total > 0:
        match_pct = matched / total * 100
        if match_pct < 80:
            findings.append(
                f"Only {match_pct:.0f}% of orders matched to a study ({matched:,.0f} of {total:,.0f}) — "
                f"{orphans:,.0f} unmatched orders indicate scheduling or HL7 integration failures."
            )
            severity = max(severity, 2)
        elif match_pct < 95:
            findings.append(
                f"Order matching rate is {match_pct:.0f}% ({matched:,.0f} of {total:,.0f}) — "
                f"{orphans:,.0f} unmatched orders require review."
            )
            severity = max(severity, 1)
        else:
            findings.append(
                f"Order matching rate is {match_pct:.0f}% ({total:,.0f} total orders) — "
                f"strong HL7 integration health."
            )

    if delta > 20:
        findings.append(
            f"Order volume increased {delta:.0f}% vs. prior period — capacity planning review required."
        )
        severity = max(severity, 1)
    elif delta > 0:
        findings.append(f"Order volume is up {delta:.0f}% vs. prior period — positive growth trend.")
    elif delta < -10:
        findings.append(
            f"Order volume dropped {abs(delta):.0f}% vs. prior period — investigate referral sources."
        )
        severity = max(severity, 1)

    if avg_dur > 90:
        findings.append(
            f"Average procedure duration is {avg_dur:.0f} min — "
            f"review scheduling slot allocations to reduce device idle time."
        )
        severity = max(severity, 1)
    elif avg_dur > 0:
        findings.append(f"Average procedure duration: {avg_dur:.0f} min.")

    if severity == 2:
        headline = "CRITICAL — Order Matching Failure"
        action   = (
            "Immediate Action: Investigate HL7 message routing for the top failing modalities. "
            "Ensure all scheduling systems are sending well-formed ORM messages."
        )
    elif severity == 1:
        headline = "ELEVATED — Order Workflow Concerns"
        action   = (
            "Recommended: Audit unmatched orders by modality and date. "
            "Review scheduling slot durations for high-utilisation procedures."
        )
    else:
        headline = "NORMAL — Order Workflow Operating Smoothly"
        action   = "Continue monitoring. Set alerts for matching-rate drops below 95%."

    return _format(headline, dr, findings, action)


# ── Report 29: Storage ────────────────────────────────────────────────────────

def _dx_storage(stats):
    total_tb  = _n(stats.get('total_storage_tb', 0))
    avg_mb    = _n(stats.get('avg_mb_per_study', 0))
    alert_cnt = int(_n(stats.get('storage_alerts', 0)))
    top_mod   = stats.get('top_modality', '')
    dr        = stats.get('date_range', '')

    severity = 0
    findings = []

    if total_tb > 50:
        findings.append(
            f"Total PACS archive: {total_tb:.1f} TB — large-scale storage requiring a "
            f"tiered hot/cold archival strategy."
        )
        severity = max(severity, 1)
    elif total_tb > 0:
        findings.append(f"Total PACS storage consumed: {total_tb:.1f} TB.")

    if avg_mb > 500:
        findings.append(
            f"Average study size is {avg_mb:.0f} MB — well above the 200 MB benchmark, "
            f"driven by high-slice CT or MRI acquisitions."
        )
        severity = max(severity, 1)
    elif avg_mb > 200:
        findings.append(
            f"Average study size is {avg_mb:.0f} MB — moderate; monitor for monthly growth."
        )
    elif avg_mb > 0:
        findings.append(f"Average study size is {avg_mb:.0f} MB — within normal range.")

    if top_mod:
        findings.append(f"Highest storage consumer: {top_mod}.")

    if alert_cnt > 5:
        findings.append(
            f"{alert_cnt} procedure types generating studies above 500 MB — "
            f"these are the primary storage cost drivers."
        )
        severity = max(severity, 2)
    elif alert_cnt > 0:
        findings.append(
            f"{alert_cnt} procedure type(s) flagged above the 500 MB/study threshold."
        )
        severity = max(severity, 1)

    if severity == 2:
        headline = "CRITICAL — Storage Growth Requires Intervention"
        action   = (
            "Immediate Action: Apply compression or archival policies to the flagged high-density "
            "procedures. Initiate PACS capacity expansion planning with your vendor."
        )
    elif severity == 1:
        headline = "ELEVATED — Storage Growth Trending High"
        action   = (
            "Recommended: Set monthly storage growth targets. "
            "Evaluate lossless DICOM compression for CT and MRI series to reduce footprint."
        )
    else:
        headline = "NORMAL — Storage Consumption Within Budget"
        action   = (
            "Continue monitoring. Plan for a 20% annual growth buffer "
            "in PACS capacity projections."
        )

    return _format(headline, dr, findings, action)


# ── Generic fallback ──────────────────────────────────────────────────────────

def _dx_generic(stats):
    dr    = stats.get('date_range', '')
    lines = [f"[REPORT SUMMARY]  {dr}", ""]
    for k, v in stats.items():
        if k == 'date_range':
            continue
        lines.append(f"  • {k.replace('_', ' ').title()}: {v}")
    lines += ["", "Review the metrics above for trends and anomalies."]
    return "\n".join(lines)


# ── Shared formatter ──────────────────────────────────────────────────────────

def _format(headline, date_range, findings, action):
    parts = [f"[{headline}]  {date_range}", ""]
    parts += [f"  • {f}" for f in findings]
    parts += ["", action]
    return "\n".join(parts)
