from flask import Blueprint, jsonify
from sync_summary import get_table_comparison

bp = Blueprint('sync_summary', __name__)

@bp.route('/sync-summary/<server_name>/tables.json')
def get_server_tables(server_name):
    comparison = get_table_comparison(server_name)
    return jsonify(comparison)