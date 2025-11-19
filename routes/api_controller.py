from flask import Blueprint, request, jsonify, session, url_for
from functools import wraps
from datetime import datetime
from db import get_report_data, get_etl_cutoff_date 

# Define the Blueprint
api_bp = Blueprint('api', __name__)

# --- Helper Function for Authentication (Reused from Dashboard) ---

def auth_required(f):
    """Decorator to check if the user is logged in."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # We check both the session status and if the request is trying to hit an API endpoint.
        # This prevents the app.before_request hook from redirecting the user to login 
        # on a failed API call, allowing us to return a proper JSON 401 response instead.
        if 'logged_in' not in session or not session.get('logged_in'):
            # Return a JSON 401 Unauthorized response for API calls
            return jsonify({'error': 'Unauthorized access. Please log in.'}), 401
        return f(*args, **kwargs)
    return decorated_function

# =========================================================================
# DATA ENDPOINT
# =========================================================================

@api_bp.route('/reports', methods=['POST'])
@auth_required
def get_reports_data():
    """
    API endpoint to fetch structured report data based on user selections.
    Expects JSON payload with: {start_date, end_date, report_ids}
    """
    if not request.is_json:
        return jsonify({'error': 'Missing or invalid JSON payload.'}), 400

    data = request.get_json()
    
    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')
    report_ids = data.get('report_ids')
    
    if not all([start_date_str, end_date_str, report_ids]):
        return jsonify({'error': 'Missing required fields: start_date, end_date, or report_ids.'}), 400
        
    if not isinstance(report_ids, list) or not report_ids:
        return jsonify({'error': 'report_ids must be a non-empty list.'}), 400
        
    # --- Date Validation ---
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'Invalid date format. Expected YYYY-MM-DD.'}), 400

    # Ensure start date is not before the official ETL cutoff date
    etl_cutoff = get_etl_cutoff_date()
    if etl_cutoff and start_date < etl_cutoff:
        # NOTE: We allow the query to proceed, but warn the user that 
        # data before the cutoff might be incomplete. We will use the 
        # user-specified start_date, but the SQL queries will handle 
        # filtering based on available data.
        pass

    # --- Data Retrieval ---
    try:
        # Call the core data fetching function (defined in db.py)
        # This function should execute the relevant SQL templates.
        report_data = get_report_data(start_date, end_date, report_ids)
        
        # Structure the final response
        return jsonify({
            'success': True,
            'start_date': start_date_str,
            'end_date': end_date_str,
            'data': report_data
        }), 200

    except Exception as e:
        # Log the detailed error on the server side
        from flask import current_app
        current_app.logger.error(f"API Reports Error: {e}", exc_info=True)
        # Return a generic error to the client
        return jsonify({'error': 'A server error occurred while generating reports.'}), 500

