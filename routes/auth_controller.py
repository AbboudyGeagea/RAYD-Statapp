# routes/auth_controller.py
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from flask_login import login_user, logout_user, current_user
from werkzeug.security import check_password_hash
from db import User, db

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    # If already logged in, send them where they belong
    if current_user.is_authenticated:
        return redirect(url_for('admin.admin_dashboard' if current_user.role == 'admin' else 'viewer.viewer_dashboard'))

    if request.method == 'POST':
        user = User.query.filter_by(username=request.form.get('username')).first()
        password = request.form.get('password')

        if user and check_password_hash(user.password_hash, password):
            # The only thing that matters: log the user into the session cookie
            login_user(user)
            session.permanent = True
            flash('Login successful!', 'success')
            
            if user.role == 'admin':
                return redirect(url_for('admin.admin_dashboard'))
            return redirect(url_for('viewer.viewer_dashboard'))
        
        flash('Invalid username or password.', 'danger')

    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    logout_user()
    session.clear()
    return redirect(url_for('auth.login'))
