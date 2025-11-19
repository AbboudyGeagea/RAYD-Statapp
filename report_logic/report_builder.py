from datetime import date, datetime
from flask import current_app
from sqlalchemy import func, case, cast, Integer, Date, text
from sqlalchemy.orm import joinedload
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import literal

# Import all models and the database instance
from db import db, ETLStudies, ETLOrders, ETLPatientsView, AEModalityMap, ProcedureDurationMap, GoLiveDate, SINGLETON_ID

# --- REPORT CONFIGURATION ---
# Each report now has a unique ID, a display title, and the function reference.
REPORTS_CONFIGURATION = [
    ('report_1', 'Total Studies', 'report_1_total_studies'),
    ('report_2', 'Studies by Modality', 'report_2_studies_by_modality'),
    ('report_3', 'Modality Count Per Month', 'report_3_studies_by_modality_per_month'),
    ('report_4', 'Final TAT Per Signing Physician', 'report_4_tat_per_signing_physician'),
    ('report_5', 'Final TAT Per Physician & Modality', 'report_5_tat_per_signing_physician_and_modality'),
    ('report_6', 'Patient/Order/Study Comparison', 'report_6_patient_order_study_comparison'),
    ('report_7', 'Top Patients by Study Count', 'report_7_patients_highest_study_count'),
    ('report_8', 'Patient Waiting Time Per Modality', 'report_8_patient_waiting_time_per_modality'),
    ('report_9', 'Avg Image Transfer Delay (PACS)', 'report_9_image_transfer_delay'),
    ('report_10', 'Count of Unread Studies', 'report_10_count_unread_studies'),
    ('report_11', 'Addendum Studies Count & Percentage', 'report_11_addendum_studies'),
    ('report_12', 'Body Part Count Per Modality', 'report_12_study_body_part_per_modality'),
    ('report_13', 'Referring Physician/Modality Studies', 'report_13_referring_physician_studies_per_modality'),
    ('report_14', 'Final Reports Per Period/Physician', 'report_14_finalized_reports_per_period'),
    ('report_15', 'Highest Study Description Counts', 'report_15_highest_study_description'),
    ('report_16', 'Patient Presence Time (Scheduled)', 'report_16_patient_presence_time'),
    ('report_17', 'Resident (Prelim) TAT', 'report_17_tat_of_residents'),
    ('report_18', 'Avg Age Per Modality/Description', 'report_18_average_age_per_modality_desc'),
    ('report_19', 'Gender Percentage Per 10-Year Age Bracket', 'report_19_gender_percentage_per_age_bracket'),
    ('report_20', 'Patient Count by Patient Class', 'report_20_patients_by_patient_class'),
]

# --- UTILITY FUNCTIONS (For Date Handling and SQL Calculations) ---

def _calculate_age_years():
    """Calculates the age in years using PostgreSQL's AGE function."""
    # Assumes PostgreSQL compatibility for the `AGE` and `DATE_PART` functions
    return func.cast(func.date_part('year', func.age(ETLPatientsView.date_of_birth)), Integer).label('patient_age')

def _combine_physician_name(model):
    """Combines first and last name columns into a single physician name string."""
    return func.concat(
        model.referring_physician_first_name, 
        literal(' '), # Spacer
        model.referring_physician_last_name
    ).label('referring_physician_full')


def get_etl_cutoff_date() -> date | None:
    """Fetches the global Go-Live Date to use as the earliest allowed reporting date."""
    try:
        # Use db.session.get for primary key lookup
        go_live_entry = db.session.get(GoLiveDate, SINGLETON_ID)
        return go_live_entry.date if go_live_entry else None
    except SQLAlchemyError as e:
        # Note: current_app is only available within a Flask application context
        # We assume this function is called inside one.
        if current_app:
            current_app.logger.error(f"Error fetching Go-Live Date for reporting: {e}")
        return None

def validate_dates(start_date: date, end_date: date, app_cutoff_date: date) -> tuple[bool, str]:
    """Validates that the date range is logical and within the application's Go-Live bounds."""
    if start_date > end_date:
        return False, "Start date cannot be after end date."
    if start_date < app_cutoff_date:
        return False, f"Start date ({start_date}) cannot be before the application's Go-Live Date ({app_cutoff_date})."
    if end_date > date.today():
        return False, "End date cannot be in the future."
    return True, "Dates are valid."

# --- REPORT GENERATION FUNCTIONS (1-20) ---

def _get_base_studies_query(start_date: date, end_date: date):
    """Base query filtered by ETLStudies.study_date, used by many reports."""
    # Uses the correct column ETLStudies.study_date
    return ETLStudies.query.filter(
        cast(ETLStudies.study_date, Date) >= start_date,
        cast(ETLStudies.study_date, Date) <= end_date
    )

def report_1_total_studies(start_date: date, end_date: date) -> dict:
    """1. Total number of studies in the system within the date filter."""
    count = _get_base_studies_query(start_date, end_date).count()
    return {'total_studies': count}

def report_2_studies_by_modality(start_date: date, end_date: date) -> list[dict]:
    """2. Total count per modality (AE Title mapping)."""
    results = _get_base_studies_query(start_date, end_date).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).group_by(
        AEModalityMap.modality
    ).with_entities(
        AEModalityMap.modality,
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        func.count(ETLStudies.study_db_uid).desc() # Corrected PK
    ).all()
    return [{'modality': r.modality, 'count': r.study_count} for r in results]

def report_3_studies_by_modality_per_month(start_date: date, end_date: date) -> list[dict]:
    """3. Total count per modality per month."""
    results = _get_base_studies_query(start_date, end_date).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).group_by(
        AEModalityMap.modality,
        func.date_trunc('month', ETLStudies.study_date)
    ).with_entities(
        AEModalityMap.modality,
        func.date_trunc('month', ETLStudies.study_date).label('report_month'),
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        'report_month',
        AEModalityMap.modality
    ).all()
    # Format month into string for easier consumption
    return [{'modality': r.modality, 'month': r.report_month.strftime('%Y-%m'), 'count': r.study_count} for r in results]

def report_4_tat_per_signing_physician(start_date: date, end_date: date) -> list[dict]:
    """4. TAT (Final Signature Time - Study Date) per signing physician."""
    # Convert interval difference to average minutes using epoch difference
    tat_minutes = func.extract('EPOCH', ETLStudies.rep_final_timestamp - ETLStudies.study_date) / 60.0
    
    results = _get_base_studies_query(start_date, end_date).filter(
        ETLStudies.rep_final_signed_by.isnot(None),
        ETLStudies.rep_final_timestamp.isnot(None)
    ).group_by(
        ETLStudies.rep_final_signed_by
    ).with_entities(
        ETLStudies.rep_final_signed_by.label('physician'),
        func.avg(tat_minutes).label('avg_tat_minutes'),
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        func.avg(tat_minutes)
    ).all()
    return [{'physician': r.physician, 'avg_tat_minutes': round(r.avg_tat_minutes, 2), 'count': r.study_count} for r in results]

def report_5_tat_per_signing_physician_and_modality(start_date: date, end_date: date) -> list[dict]:
    """5. TAT per signing physician per modality type."""
    tat_minutes = func.extract('EPOCH', ETLStudies.rep_final_timestamp - ETLStudies.study_date) / 60.0
    
    results = _get_base_studies_query(start_date, end_date).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).filter(
        ETLStudies.rep_final_signed_by.isnot(None),
        ETLStudies.rep_final_timestamp.isnot(None)
    ).group_by(
        ETLStudies.rep_final_signed_by,
        AEModalityMap.modality
    ).with_entities(
        ETLStudies.rep_final_signed_by.label('physician'),
        AEModalityMap.modality,
        func.avg(tat_minutes).label('avg_tat_minutes'),
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        AEModalityMap.modality,
        func.avg(tat_minutes)
    ).all()
    return [{'physician': r.physician, 'modality': r.modality, 'avg_tat_minutes': round(r.avg_tat_minutes, 2), 'count': r.study_count} for r in results]

def report_6_patient_order_study_comparison(start_date: date, end_date: date) -> dict:
    """6. Comparison of total patients, orders, studies, and patients without orders."""
    
    studies_q = _get_base_studies_query(start_date, end_date)

    # 1. Total Patients with studies in the period (join using patient_db_uid)
    total_patients = studies_q.join(ETLPatientsView, ETLStudies.patient_db_uid == ETLPatientsView.patient_db_uid).distinct(ETLPatientsView.patient_db_uid).count()

    # 2. Total Studies
    total_studies = studies_q.count()

    # 3. Total Orders associated with studies in the period (join using study_db_uid)
    total_orders = studies_q.join(ETLOrders, ETLStudies.study_db_uid == ETLOrders.study_db_uid).distinct(ETLOrders.order_dbid).count() # Corrected to use ETLOrders.order_dbid

    # 4. Patients with studies but NO associated orders 
    # Get all patient_db_uids that have studies in the period
    patients_with_studies_q = ETLStudies.query.filter(
        cast(ETLStudies.study_date, Date) >= start_date,
        cast(ETLStudies.study_date, Date) <= end_date
    )

    # Subquery: Find all study_db_uids that DO have an order entry
    studies_with_orders_subquery = db.session.query(ETLOrders.study_db_uid).subquery()

    # Count distinct patients whose studies are NOT in the 'studies_with_orders' list
    patients_without_orders = patients_with_studies_q.filter(
        ~ETLStudies.study_db_uid.in_(db.session.query(studies_with_orders_subquery))
    ).distinct(ETLStudies.patient_db_uid).count()
    
    return {
        'total_patients_with_studies': total_patients,
        'total_studies': total_studies,
        'total_orders': total_orders,
        'patients_without_orders': patients_without_orders
    }

def report_7_patients_highest_study_count(start_date: date, end_date: date, limit=10) -> list[dict]:
    """7. Top N patients with the highest number of studies (ID only)."""
    # Using patient_db_uid as the unique patient ID as no dedicated patient_id column was defined
    results = _get_base_studies_query(start_date, end_date).join(
        ETLPatientsView, ETLStudies.patient_db_uid == ETLPatientsView.patient_db_uid # Corrected join
    ).group_by(
        ETLPatientsView.patient_db_uid
    ).with_entities(
        ETLPatientsView.patient_db_uid.label('patient_id'), # Use db_uid as the ID for reporting
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        func.count(ETLStudies.study_db_uid).desc()
    ).limit(limit).all()
    return [{'patient_id': r.patient_id, 'study_count': r.study_count} for r in results]

def report_8_patient_waiting_time_per_modality(start_date: date, end_date: date) -> list[dict]:
    """8. Patient waiting time (Study Date - Scheduled DT) per modality."""
    # Corrected column name: ETLOrders.scheduled_datetime
    waiting_minutes = func.extract('EPOCH', ETLStudies.study_date - ETLOrders.scheduled_datetime) / 60.0
    
    results = _get_base_studies_query(start_date, end_date).join(
        ETLOrders, ETLStudies.study_db_uid == ETLOrders.study_db_uid # Corrected join
    ).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).filter(
        ETLOrders.scheduled_datetime.isnot(None)
    ).group_by(
        AEModalityMap.modality
    ).with_entities(
        AEModalityMap.modality,
        func.avg(waiting_minutes).label('avg_waiting_minutes'),
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        AEModalityMap.modality
    ).all()
    return [{'modality': r.modality, 'avg_waiting_minutes': round(r.avg_waiting_minutes, 2), 'count': r.study_count} for r in results]

def report_9_image_transfer_delay(start_date: date, end_date: date) -> dict:
    """9. Delay in sending images from modality to PACS (Insert Time - Study Date)."""
    delay_minutes = func.extract('EPOCH', ETLStudies.insert_time - ETLStudies.study_date) / 60.0

    result = _get_base_studies_query(start_date, end_date).filter(
        ETLStudies.insert_time.isnot(None)
    ).with_entities(
        func.avg(delay_minutes).label('avg_delay_minutes'),
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).first()
    
    return {'avg_delay_minutes': round(result.avg_delay_minutes, 2) if result and result.avg_delay_minutes else 0.0,
            'study_count': result.study_count if result else 0}

def report_10_count_unread_studies(start_date: date, end_date: date) -> dict:
    """10. Number of not signed studies (study_status = 'unread')."""
    count = _get_base_studies_query(start_date, end_date).filter(
        func.lower(ETLStudies.study_status) == 'unread'
    ).count()
    return {'unread_studies': count}

def report_11_addendum_studies(start_date: date, end_date: date) -> dict:
    """11. Number of rep_has_addendum and the percentage of the total."""
    studies_q = _get_base_studies_query(start_date, end_date)
    total_count = studies_q.count()
    # Assuming 'True' is the string representation stored in the ETL column
    addendum_count = studies_q.filter(func.lower(ETLStudies.rep_has_addendum) == 'true').count()
    
    percentage = (addendum_count / total_count * 100) if total_count > 0 else 0.0
    
    return {'addendum_count': addendum_count, 'total_count': total_count, 'percentage': round(percentage, 2)}

def report_12_study_body_part_per_modality(start_date: date, end_date: date) -> list[dict]:
    """12. Study body part per modality per date filter."""
    results = _get_base_studies_query(start_date, end_date).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).filter(
        ETLStudies.study_body_part.isnot(None)
    ).group_by(
        ETLStudies.study_body_part,
        AEModalityMap.modality
    ).with_entities(
        AEModalityMap.modality,
        ETLStudies.study_body_part,
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        AEModalityMap.modality,
        func.count(ETLStudies.study_db_uid).desc()
    ).all()
    return [{'modality': r.modality, 'body_part': r.study_body_part, 'count': r.study_count} for r in results]

def report_13_referring_physician_studies_per_modality(start_date: date, end_date: date) -> list[dict]:
    """13. Number of studies per referring_physician per modality."""
    referring_physician_full = _combine_physician_name(ETLStudies) # Calculate full name
    
    results = _get_base_studies_query(start_date, end_date).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).filter(
        ETLStudies.referring_physician_last_name.isnot(None) # Filter on last name presence
    ).group_by(
        referring_physician_full,
        AEModalityMap.modality
    ).with_entities(
        referring_physician_full.label('physician'), # Use the calculated name
        AEModalityMap.modality,
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        'physician',
        func.count(ETLStudies.study_db_uid).desc()
    ).all()
    return [{'physician': r.physician, 'modality': r.modality, 'count': r.study_count} for r in results]

def report_14_finalized_reports_per_period(start_date: date, end_date: date) -> list[dict]:
    """14. Number of finalized reports per day/month per signing_physician."""
    # CRITICAL: This report filters on the final signature date, not study_date.
    
    # Decide grouping based on date granularity (Day if range < 3 months, Month otherwise)
    date_diff = (end_date - start_date).days
    date_grouping = 'day' if date_diff <= 90 else 'month'
    date_format = '%Y-%m-%d' if date_grouping == 'day' else '%Y-%m'

    # Filter by the rep_final_timestamp
    final_reports_q = ETLStudies.query.filter(
        cast(ETLStudies.rep_final_timestamp, Date) >= start_date,
        cast(ETLStudies.rep_final_timestamp, Date) <= end_date,
        ETLStudies.rep_final_signed_by.isnot(None)
    )

    results = final_reports_q.group_by(
        ETLStudies.rep_final_signed_by,
        func.date_trunc(date_grouping, ETLStudies.rep_final_timestamp)
    ).with_entities(
        ETLStudies.rep_final_signed_by.label('physician'),
        func.date_trunc(date_grouping, ETLStudies.rep_final_timestamp).label('report_date'),
        func.count(ETLStudies.study_db_uid).label('report_count') # Corrected PK
    ).order_by(
        'report_date',
        ETLStudies.rep_final_signed_by
    ).all()
    
    return [{'physician': r.physician, 'period': r.report_date.strftime(date_format), 'count': r.report_count} for r in results]

def report_15_highest_study_description(start_date: date, end_date: date, limit=10) -> list[dict]:
    """15. Highest count of study_description."""
    results = _get_base_studies_query(start_date, end_date).filter(
        ETLStudies.study_description.isnot(None)
    ).group_by(
        ETLStudies.study_description
    ).with_entities(
        ETLStudies.study_description,
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        func.count(ETLStudies.study_db_uid).desc()
    ).limit(limit).all()
    return [{'description': r.study_description, 'count': r.study_count} for r in results]

def report_16_patient_presence_time(start_date: date, end_date: date) -> dict:
    """16. Number of present patient in radiology (Current Time - Order Scheduled DT)."""
    # Corrected column name: ETLOrders.scheduled_datetime
    
    # Orders that were scheduled in the report period but haven't been completed yet (no study_db_uid association)
    current_orders_q = ETLOrders.query.filter(
        ETLOrders.study_db_uid.is_(None), # Has not been studied yet (Corrected column name)
        cast(ETLOrders.scheduled_datetime, Date) >= start_date, # Corrected column name
        cast(ETLOrders.scheduled_datetime, Date) <= end_date, # Corrected column name
        ETLOrders.scheduled_datetime.isnot(None) # Corrected column name
    )
    
    # Get the count of "present" patients
    count = current_orders_q.count()
    
    # Calculate average time elapsed since scheduled time
    now_ts = func.now()
    presence_minutes = func.extract('EPOCH', now_ts - ETLOrders.scheduled_datetime) / 60.0
    
    avg_presence_minutes = current_orders_q.with_entities(
        func.avg(presence_minutes)
    ).scalar()
    
    return {
        'present_patients_count': count,
        'avg_presence_minutes_since_scheduled': round(avg_presence_minutes, 2) if avg_presence_minutes else 0.0
    }

def report_17_tat_of_residents(start_date: date, end_date: date) -> dict:
    """17. TAT of residents (Prelim Timestamp - Study Date)."""
    tat_minutes = func.extract('EPOCH', ETLStudies.rep_prelim_timestamp - ETLStudies.study_date) / 60.0

    result = _get_base_studies_query(start_date, end_date).filter(
        ETLStudies.rep_prelim_timestamp.isnot(None)
    ).with_entities(
        func.avg(tat_minutes).label('avg_prelim_tat_minutes'),
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).first()
    
    return {'avg_prelim_tat_minutes': round(result.avg_prelim_tat_minutes, 2) if result and result.avg_prelim_tat_minutes else 0.0,
            'study_count': result.study_count if result else 0}

def report_18_average_age_per_modality_desc(start_date: date, end_date: date) -> list[dict]:
    """18. Average age per study_description per modality (with outlier removal)."""
    patient_age_calc = _calculate_age_years() # Calculate age
    
    results = _get_base_studies_query(start_date, end_date).join(
        ETLPatientsView, ETLStudies.patient_db_uid == ETLPatientsView.patient_db_uid # Corrected join
    ).join(
        AEModalityMap, ETLStudies.storing_ae == AEModalityMap.aetitle
    ).filter(
        ETLPatientsView.date_of_birth.isnot(None), # Ensure we can calculate age
        patient_age_calc >= 1, # Outlier removal
        patient_age_calc <= 100 # Outlier removal
    ).group_by(
        ETLStudies.study_description,
        AEModalityMap.modality
    ).with_entities(
        ETLStudies.study_description,
        AEModalityMap.modality,
        func.avg(patient_age_calc).label('avg_age'), # Use the calculated age
        func.count(ETLStudies.study_db_uid).label('study_count') # Corrected PK
    ).order_by(
        AEModalityMap.modality,
        ETLStudies.study_description
    ).all()
    return [{'description': r.study_description, 'modality': r.modality, 'avg_age': round(r.avg_age, 1), 'count': r.study_count} for r in results]

def report_19_gender_percentage_per_age_bracket(start_date: date, end_date: date) -> list[dict]:
    """19. Percentage of each gender per age bracket of 10 years."""
    
    patient_age_calc = _calculate_age_years() # Calculate age
    
    # 10-year age bracket calculation
    age_bracket = (func.floor(patient_age_calc / 10) * 10).cast(Integer)
    
    # Filter patients by studies in the period
    patients_q = ETLPatientsView.query.join(
        ETLStudies, ETLPatientsView.patient_db_uid == ETLStudies.patient_db_uid # Corrected join
    ).filter(
        cast(ETLStudies.study_date, Date) >= start_date,
        cast(ETLStudies.study_date, Date) <= end_date,
        ETLPatientsView.date_of_birth.isnot(None),
        ETLPatientsView.patient_sex.isnot(None)
    )

    # Subquery for total in each bracket (counting distinct patients)
    total_q = patients_q.group_by(age_bracket).with_entities(
        age_bracket.label('bracket'),
        func.count(func.distinct(ETLPatientsView.patient_db_uid)).label('total_in_bracket')
    ).subquery()
    
    # Main query for counts and percentage calculation (counting distinct patients)
    results = patients_q.group_by(
        age_bracket,
        ETLPatientsView.patient_sex,
        total_q.c.total_in_bracket
    ).join(
        total_q, total_q.c.bracket == age_bracket
    ).with_entities(
        age_bracket.label('age_bracket'),
        ETLPatientsView.patient_sex,
        func.count(func.distinct(ETLPatientsView.patient_db_uid)).label('gender_count'),
        (func.count(func.distinct(ETLPatientsView.patient_db_uid)) * 100.0 / total_q.c.total_in_bracket).label('percentage')
    ).order_by(
        'age_bracket',
        ETLPatientsView.patient_sex
    ).all()

    return [{
        'age_bracket': f"{r.age_bracket}-{r.age_bracket + 9}", 
        'gender': r.patient_sex, 
        'count': r.gender_count, 
        'percentage': round(r.percentage, 2)
    } for r in results]

def report_20_patients_by_patient_class(start_date: date, end_date: date) -> list[dict]:
    """20. Number of patient per patient_class."""
    
    # Note: Using patient_class from ETLStudies as it was not defined in ETLPatientsView in db.py
    results = ETLStudies.query.filter( # Start with studies to filter by date
        cast(ETLStudies.study_date, Date) >= start_date,
        cast(ETLStudies.study_date, Date) <= end_date,
        ETLStudies.patient_class.isnot(None)
    ).group_by(
        ETLStudies.patient_class
    ).with_entities(
        ETLStudies.patient_class,
        func.count(func.distinct(ETLStudies.patient_db_uid)).label('patient_count') # Count distinct patients
    ).order_by(
        func.count(func.distinct(ETLStudies.patient_db_uid)).desc()
    ).all()
    
    return [{'patient_class': r.patient_class, 'count': r.patient_count} for r in results]


# --- CENTRAL REPORT RUNNER ---

def build_all_reports(start_date_str: str, end_date_str: str) -> dict:
    """
    Central function to run all reports using the formal configuration.
    Expects date strings in YYYY-MM-DD format from the UI.
    """
    app_cutoff_date = get_etl_cutoff_date()
    if not app_cutoff_date:
        return {'error': 'System configuration error: Go-Live Date is not set.', 'reports': {}}

    if not end_date_str:
        return {'error': 'End date is required.', 'reports': {}}
    
    try:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        return {'error': 'Invalid end date format provided. Expected YYYY-MM-DD.', 'reports': {}}

    used_start_date: date
    default_start_date_used: bool = False
    
    if not start_date_str:
        used_start_date = app_cutoff_date
        default_start_date_used = True
    else:
        try:
            used_start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            return {'error': 'Invalid start date format provided. Expected YYYY-MM-DD.', 'reports': {}}

    is_valid, error_msg = validate_dates(used_start_date, end_date, app_cutoff_date)
    if not is_valid:
        return {'error': error_msg, 'reports': {}}

    try:
        if current_app:
            current_app.logger.info(f"Building reports for period: {used_start_date} to {end_date}")
        
        reports = {}
        # Execute reports based on the configuration list
        for report_id, report_title, func_name in REPORTS_CONFIGURATION:
            # We use globals() to dynamically fetch the function reference by its name
            report_func = globals().get(func_name)
            
            if report_func:
                # Store the result using the unique ID as the key
                reports[report_id] = {
                    'title': report_title,
                    'data': report_func(used_start_date, end_date)
                }
            elif current_app:
                current_app.logger.warning(f"Report function not found: {func_name}")

        
        return {
            'reports': reports,
            'used_dates': {
                'start_date': used_start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'default_start_date_used': default_start_date_used
            }
        }
    except Exception as e:
        if current_app:
            current_app.logger.error(f"Failed to build all reports: {e}")
        return {'error': 'An internal error occurred while generating reports. Check application logs for database errors.', 'reports': {}}

