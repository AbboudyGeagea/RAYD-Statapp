from flask_login import LoginManager
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message_category = 'info'

limiter = Limiter(key_func=get_remote_address, default_limits=[])
