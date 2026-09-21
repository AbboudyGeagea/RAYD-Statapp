"""
utils/phone_lb.py
-----------------
Lebanese phone-number normalisation, ported verbatim in behaviour from the LAUMC
SMS reminder service's oracle_client.py (operator-supplied 2026-09-21).

Deliberately a port rather than a fresh implementation. That logic has been running
in production against these exact `site_person.PATIENT_PHONE_NUMBER` values for a
while and has already met their edge cases; two systems disagreeing about the same
patient's number would be worse than not storing it at all. If the reminder service
changes its rules, this file should be re-synced rather than independently "fixed".

The source column is free text, VARCHAR2(40), and messier than a phone field:
measured on the RIS 2026-09-21 over 434,595 populated rows --
  * 8-digit locals dominate: 03 (186,157), 70 (72,789), 71 (53,511), 76 (36,592),
    81 (17,203), 78, 79
  * landlines are mixed in under the same column: 01 (2,028), 07 (1,458)
  * 17- and 18-character values (1,962 rows) are TWO numbers in one field
  * ~19,000 rows are junk: a bare "0", "00", "000", "0000"
"""
import re

# Local area codes that cannot receive SMS.
LANDLINE_PREFIXES = ("01", "04", "05", "06", "07", "08", "09", "21", "24", "25", "29")


def is_landline(raw_phone):
    digits = re.sub(r"\D", "", raw_phone or "")
    if digits.startswith("00961"):
        digits = digits[2:]
    if digits.startswith("961"):
        # Already carries the country code -- trust it, don't reclassify.
        return False
    return digits.startswith(LANDLINE_PREFIXES)


def normalize_phone(raw_phone):
    """Return a bare international number ('961XXXXXXXX'), or None.

    No '+' prefix -- that matches what the reminder service stores and what it
    concatenates into the SMS gateway address.
    """
    if not raw_phone:
        return None

    digits = re.sub(r"\D", "", raw_phone)
    if not digits:
        return None

    if digits.startswith("00961") and len(digits) >= 12:
        digits = digits[2:]

    if digits.startswith("+961"):
        digits = digits[1:]

    if digits.startswith("961") and len(digits) in (10, 11):
        # 11 digits: new-format prefixes (70/71/76/78/79/81) -- 961 + 8 digits.
        # 10 digits: old-format Alfa '03' -- 961 + 7 digits, leading 0 dropped.
        return digits

    if digits.startswith("0") and len(digits) == 8:
        return f"961{digits[1:]}"

    if len(digits) == 8:
        return f"961{digits}"

    # KNOWN QUIRK, carried over deliberately: this strips exactly one leading zero,
    # so a 9-digit value beginning '00' (e.g. '003217711', which is not the '00961'
    # international form handled above) yields '96103217711' -- a zero surviving after
    # the country code. The reminder service has always behaved this way and its output
    # is what actually gets sent, so RAYD matches it rather than quietly producing a
    # different number for the same patient. Not currently reachable in practice: the
    # 9-character values in the RIS start '03'/'70'/'71'/'76'/'81' (measured
    # 2026-09-21), and the '00'-prefixed rows are 2-4 characters of junk that resolve
    # to None. Fix it upstream in the reminder service first if it ever matters.
    if len(digits) == 9 and digits.startswith("0"):
        return f"961{digits[1:]}"

    return None


def _split_candidates(raw_phone):
    """Some records hold 2+ numbers in one field, separated by spaces or punctuation."""
    if not raw_phone:
        return []
    return [p for p in re.split(r"[\s,/;]+", raw_phone.strip()) if p]


def resolve_phone(raw_phone):
    """Return (normalized_mobile_or_None, is_landline, chosen_raw_candidate).

    Tries the whole field first: both helpers above already strip internal
    punctuation, so '03/217711' and '03-217711' reduce to the same digits and
    resolve correctly instead of being mistaken for two numbers. Only if that
    fails does it split on separators looking for genuinely distinct numbers,
    first valid mobile wins. If nothing normalises but something looks like a
    landline, that is reported instead.
    """
    if not raw_phone:
        return None, False, raw_phone

    whole_is_landline = is_landline(raw_phone)
    if not whole_is_landline:
        whole = normalize_phone(raw_phone)
        if whole:
            return whole, False, raw_phone

    any_landline = whole_is_landline
    for candidate in _split_candidates(raw_phone):
        if is_landline(candidate):
            any_landline = True
            continue
        normalized = normalize_phone(candidate)
        if normalized:
            return normalized, False, candidate

    return None, any_landline, raw_phone


def has_relative_marker(raw_phone):
    """True when staff wrote an 'R' into the field.

    Their convention for "this number intentionally belongs to a relative" -- e.g. a
    parent's number on a child's record. Carried across because it is the difference
    between an explained shared number and an unexplained collision between two
    different patients, and that distinction is lost if only the digits are kept.
    """
    return bool(raw_phone) and re.search(r"[Rr]", raw_phone) is not None
