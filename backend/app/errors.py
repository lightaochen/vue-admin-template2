# app/errors.py
from .utils.response import make_err

def register_error_handlers(app):
    @app.errorhandler(404)
    def not_found(e):
        return make_err(code=40400, message="Not Found", http_status=404)

    @app.errorhandler(405)
    def method_not_allowed(e):
        return make_err(code=40500, message="Method Not Allowed", http_status=405)
