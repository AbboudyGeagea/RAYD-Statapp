"""
utils/site_branding.py — each hospital's logo and name, by site code.

Operator, 2026-11-25/26: the hospital's logo sits in the middle of the header, changes when
another hospital (database) is picked, and each hospital's logo goes on its own reports' PDF
header (to-do item 44), whose footer names the hospital too.

logo_on says what the logo needs behind it: 'light' for a coloured logo (Rizk Hospital's, drawn
on a white tile on the dark header), 'dark' for a white logo (Saint John's, drawn on LAU green:
on a white header, the light theme or a white PDF page it would vanish). LAUMC-SJH-header.png is
static/LAUMC-SJH.png with its empty margins cropped, so it fills its tile like Rizk's does.
"""

BRANDS = {
    'RH': {
        'logo': 'lau.png',
        'logo_on': 'light',
        'alt': 'LAU Medical Center - Rizk Hospital',
        'hospital_line': 'Lebanese American University – Rizk Hospital, Radiology Department',
    },
    'SJH': {
        'logo': 'LAUMC-SJH-header.png',
        'logo_on': 'dark',
        'alt': "LAU Medical Center - Saint John's Hospital",
        'hospital_line': "Lebanese American University – Saint John's Hospital, Radiology Department",
    },
}
DEFAULT_CODE = 'RH'


def brand(site_id=None):
    """The brand of site_id, else of the site this request (or use_site block) reads; the
    default hospital's for a site without one."""
    from utils import site_db
    sid = site_db.current_site_id() if site_id is None else site_id
    code = (site_db.site_code(sid) or '').upper()
    return BRANDS.get(code, BRANDS[DEFAULT_CODE])
