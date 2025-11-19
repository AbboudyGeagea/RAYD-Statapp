# utils/report_token.py
import hashlib
import hmac
import os

SECRET_KEY = os.environ.get('SECRET_KEY', 'P@ssw0rd123!')

def generate_report_token(*args, **kwargs):
    report_id = None

    if args:
        report_id = args[-1]
    elif 'report_id' in kwargs:
        report_id = kwargs['report_id']

    if report_id is None:
        raise ValueError("Missing report_id")

    msg = str(report_id).encode('utf-8')
    return hmac.new(
        SECRET_KEY.encode('utf-8'),
        msg,
        hashlib.sha256
    ).hexdigest()


def resolve_report_token(token: str):
    from db import ReportTemplate
    for r in ReportTemplate.query.with_entities(ReportTemplate.report_id).all():
        rid = r[0]
        if generate_report_token(rid) == token:
            return rid
    return None

