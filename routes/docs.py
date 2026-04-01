from flask import Blueprint, render_template
from flask_login import login_required

docs_bp = Blueprint('docs', __name__)

@docs_bp.route('/docs')
@login_required
def docs_page():
    return render_template('docs.html')

@docs_bp.route('/docs/fr')
@login_required
def docs_page_fr():
    return render_template('docs_fr.html')
