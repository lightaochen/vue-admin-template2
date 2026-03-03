from flask import jsonify

def make_ok(data=None, message="ok"):
    return jsonify({"code": 20000, "data": data, "message": message})

def make_err(code=50000, message="Error", http_status=400):
    return jsonify({"code": code, "message": message}), http_status

def register_error_handlers(app):
    @app.errorhandler(404)
    def _404(e):
        return make_err(code=40400, message="Not Found", http_status=404)
    @app.errorhandler(405)
    def _405(e):
        return make_err(code=40500, message="Method Not Allowed", http_status=405)
