from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash
from flask_login import login_user, logout_user, current_user
from db import User, ActiveSession, db
import uuid

auth_bp = Blueprint('auth', __name__, )


def get_client_ip():
    from flask import request
    xff = request.headers.get('X-Forwarded-For', '')
    if xff:
        return xff.split(',')[0].strip()
    return request.remote_addr or 'unknown'
#-----------------------------------------
# Redirect to the login route 
@auth_bp.route('/', methods=['GET'])
def go_home():
    return redirect(url_for('auth.login'))
#-----------------------------------------------
@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    # If user already logged in, redirect appropriately
    if current_user and getattr(current_user, 'is_authenticated', False):
        if current_user.role == 'admin':
            return redirect(url_for('admin.admin_dashboard'))
        return redirect(url_for('viewer.viewer_dashboard'))

    conflict = False
    prefill_username = None

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        override = request.form.get('override', 'false').lower() == 'true'

        prefill_username = username

        user = User.query.filter_by(username=username).first()
        
        # CRITICAL SECURITY FIX: Remove the 'or user.password_hash == password' check.
        # This prevents accepting unhashed passwords or raw hashes as input.
        if not user or not check_password_hash(user.password_hash, password):
            flash('Invalid username or password.', 'danger')
            return render_template('login.html', page_title='Login', conflict=False, username=prefill_username)

        # credentials valid — conflict detection
        ip = get_client_ip()
        existing_for_user = ActiveSession.query.filter_by(user_id=user.id).all()
        same_user_different_ip = any(s.ip_address != ip for s in existing_for_user) if existing_for_user else False

        existing_on_ip = ActiveSession.query.filter_by(ip_address=ip).all()
        same_ip_other_user = any(s.user_id != user.id for s in existing_on_ip) if existing_on_ip else False

        if (same_user_different_ip or same_ip_other_user) and not override:
            # signal conflict --> show modal on login page
            conflict = True
            return render_template('login.html', page_title='Login', conflict=True, username=prefill_username)

        # If override requested or no conflict: delete conflicting sessions
        try:
            if same_user_different_ip:
                ActiveSession.query.filter_by(user_id=user.id).delete()
            if same_ip_other_user:
                ActiveSession.query.filter_by(ip_address=ip).delete()
            db.session.commit()
        except Exception:
            db.session.rollback()

        # Clear any existing session to avoid leakage, then create new session
        session.clear()

        # Create a new session uuid
        sess_uuid = str(uuid.uuid4())

        # Persist Flask-Login session (Sets the user ID in the session cookie)
        login_user(user)
        session.permanent = True
        
        # Set custom session keys required by the @app.before_request check
        # NOTE: session['logged_in'] and session['username'] are redundant if using current_user, 
        # but are kept to maintain existing system dependencies.
        session['logged_in'] = True 
        session['user_id'] = user.id
        session['username'] = user.username
        session['role'] = user.role
        session['session_uuid'] = sess_uuid

        # Save active session server-side
        try:
            # FIX: Ensure all columns are correctly mapped (using session_id as per your model)
            new_active = ActiveSession(
                session_id=sess_uuid,
                user_id=user.id,
                role=user.role,
                ip_address=ip
            )
            db.session.add(new_active)
            db.session.commit()
        except Exception:
            db.session.rollback()

        flash('Login successful!', 'success')
        if user.role == 'admin':
            return redirect(url_for('admin.admin_dashboard'))
        elif user.role == 'viewer':
            return redirect(url_for('viewer.viewer_dashboard'))
        else:
            flash('Unknown role. Please contact the administrator.', 'danger')
            return redirect(url_for('auth.login'))

    # GET
    return render_template('login.html', page_title='Login', conflict=conflict, username=prefill_username)


@auth_bp.route('/logout')
def logout():
    try:
        sess_uuid = session.get('session_uuid')
        if sess_uuid:
            ActiveSession.query.filter_by(session_id=sess_uuid).delete()
            db.session.commit()
    except Exception:
        db.session.rollback()

    logout_user()
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('auth.login'))
    
@auth_bp.route('/', methods=['GET'])
def root_redirect():
    return redirect(url_for('auth.login'))

