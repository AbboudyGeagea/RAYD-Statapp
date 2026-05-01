import uuid
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, current_app
from flask_login import login_user, logout_user, current_user, login_required
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import text

from db import User, UserPagePermission, UserAuditLog, active_sessions, db

auth_bp = Blueprint('auth', __name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _get_demo_settings():
    try:
        rows = db.session.execute(text(
            "SELECT key, value FROM settings WHERE key IN ('demo_mode','demo_user')"
        )).fetchall()
        d = {r[0]: r[1] for r in rows}
        return d.get('demo_mode', 'false').lower() == 'true', d.get('demo_user', '')
    except Exception:
        return False, ''


def _audit(action, target_user_id, detail=None, actor_id=None):
    """Write one row to user_audit_log. Caller commits."""
    try:
        actor = actor_id if actor_id is not None else (
            current_user.id if current_user.is_authenticated else None
        )
        db.session.add(UserAuditLog(
            actor_user_id=actor,
            target_user_id=target_user_id,
            action=action,
            event_category='auth',
            detail=detail,
            ip_address=request.remote_addr,
        ))
    except Exception:
        pass


def _open_session(user):
    """Insert an active_sessions row and store the sid in the Flask session."""
    sid = str(uuid.uuid4())
    session['sid'] = sid
    db.session.add(active_sessions(
        session_id=sid,
        user_id=user.id,
        role=user.role,
        ip_address=request.remote_addr,
    ))


def _close_session():
    """Remove the current session row from active_sessions."""
    sid = session.pop('sid', None)
    if sid:
        active_sessions.query.filter_by(session_id=sid).delete()


# ── register ─────────────────────────────────────────────────────────────────

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    demo_mode, _ = _get_demo_settings()
    if demo_mode:
        flash('Registration is disabled during demo mode.', 'warning')
        return redirect(url_for('auth.login'))

    if request.method == 'POST':
        username  = request.form.get('username', '').strip()
        full_name = request.form.get('full_name', '').strip() or None
        email     = request.form.get('email', '').strip() or None
        password  = request.form.get('password', '')
        confirm   = request.form.get('confirm_password', '')

        if not username or not password:
            flash('Username and password are required.', 'danger')
            return render_template('register.html')

        if password != confirm:
            flash('Passwords do not match.', 'danger')
            return render_template('register.html')

        if len(password) < 6:
            flash('Password must be at least 6 characters.', 'danger')
            return render_template('register.html')

        if User.query.filter_by(username=username).first():
            flash('Username already taken.', 'danger')
            return render_template('register.html')

        try:
            new_user = User(
                username=username,
                password_hash=generate_password_hash(password, method='pbkdf2:sha256'),
                role='viewer',          # role assigned by admin at approval time
                status='pending',
                full_name=full_name,
                email=email,
            )
            db.session.add(new_user)
            db.session.flush()          # get new_user.id

            _audit('registered', new_user.id, actor_id=new_user.id)
            db.session.commit()

            flash('Registration submitted. An administrator will review your account shortly.', 'success')
            return redirect(url_for('auth.login'))

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"REGISTRATION ERROR: {e}")
            flash('A database error occurred. Please try again.', 'danger')

    return render_template('register.html')


# ── login ─────────────────────────────────────────────────────────────────────

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        dest = 'admin.admin_dashboard' if current_user.role == 'admin' else 'viewer.viewer_dashboard'
        return redirect(url_for(dest))

    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')

        user = User.query.filter_by(username=username).first()

        if not (user and check_password_hash(user.password_hash, password)):
            flash('Invalid username or password.', 'danger')
            return render_template('login.html')

        # Account status gate
        if user.status == 'pending':
            flash('Your account is pending administrator approval.', 'warning')
            return render_template('login.html')

        if user.status == 'disabled':
            flash('Your account has been disabled. Contact your administrator.', 'danger')
            return render_template('login.html')

        # Demo mode: only admin and the designated demo user
        demo_mode, demo_user = _get_demo_settings()
        if demo_mode and user.role != 'admin' and user.username != demo_user:
            flash('Access is restricted during demo mode.', 'warning')
            return render_template('login.html')

        # License checks (skip for admin)
        if user.role != 'admin':
            from routes.registry import check_license_limit
            ok, msg = check_license_limit(current_app, 'expired')
            if not ok:
                flash(msg, 'danger')
                return render_template('login.html')

            ok, msg = check_license_limit(current_app, 'max_sessions')
            if not ok:
                flash(msg, 'warning')
                return render_template('login.html')

        login_user(user)
        session.permanent = True

        # Track session in DB
        _open_session(user)

        # Update last login
        user.last_login = datetime.now(timezone.utc).replace(tzinfo=None)

        _audit('login', user.id)
        db.session.commit()

        if user.must_change_password:
            return redirect(url_for('auth.profile_password'))

        if user.role == 'admin':
            return redirect(url_for('admin.admin_dashboard'))
        if user.role == 'tec':
            return redirect(url_for('hl7_orders.hl7_orders_page'))
        return redirect(url_for('viewer.viewer_dashboard'))

    return render_template('login.html')


# ── logout ────────────────────────────────────────────────────────────────────

@auth_bp.route('/logout', methods=['GET', 'POST'])
def logout():
    if current_user.is_authenticated:
        _audit('logout', current_user.id)
        _close_session()
        db.session.commit()
    logout_user()
    session.clear()
    return redirect(url_for('auth.login'))


# ── password change (self-service) ───────────────────────────────────────────

@auth_bp.route('/profile/password', methods=['GET', 'POST'])
@login_required
def profile_password():
    if request.method == 'POST':
        current_pw  = request.form.get('current_password', '')
        new_pw      = request.form.get('new_password', '')
        confirm_pw  = request.form.get('confirm_password', '')

        if not check_password_hash(current_user.password_hash, current_pw):
            flash('Current password is incorrect.', 'danger')
            return render_template('profile_password.html')

        if new_pw != confirm_pw:
            flash('New passwords do not match.', 'danger')
            return render_template('profile_password.html')

        if len(new_pw) < 6:
            flash('Password must be at least 6 characters.', 'danger')
            return render_template('profile_password.html')

        current_user.password_hash = generate_password_hash(new_pw, method='pbkdf2:sha256')
        current_user.must_change_password = False
        _audit('password_changed', current_user.id)
        db.session.commit()

        flash('Password updated successfully.', 'success')
        if current_user.role == 'admin':
            return redirect(url_for('admin.admin_dashboard'))
        return redirect(url_for('viewer.viewer_dashboard'))

    return render_template('profile_password.html')
