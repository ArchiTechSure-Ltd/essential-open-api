"""Factory function for the Essential Open API Flask application."""

from flask import Flask, jsonify
from flask_cors import CORS
from flasgger import Swagger

from .connection import ConnectionManager
from .jvm import (
    CONNECTION_WAIT_SECONDS,
    get_connection_manager,
    jvm_is_started,
    start_connection_manager,
)
from .routes import api_bp


def create_app(
    connection_manager: ConnectionManager | None = None,
    *,
    start_connections: bool = True,
) -> Flask:
    """Create and configure a Flask application instance."""
    app = Flask(__name__)

    # Configure Flasgger
    swagger_template = {
        "swagger": "2.0",
        "info": {
            "title": "Essential Open API",
            "description": "API for accessing the Protégé knowledge base.",
            "version": "1.0.0"
        },
        "basePath": "/"
    }
    swagger_config = {
        "headers": [],
        "specs": [
            {
                "endpoint": 'apispec',
                "route": '/apispec.json',
                "rule_filter": lambda rule: True,
                "model_filter": lambda tag: True,
            }
        ],
        "static_url_path": "/flasgger_static",
        "swagger_ui": True,
        "specs_route": "/api/"
    }
    Swagger(app, template=swagger_template, config=swagger_config)

    CORS(app, resources={r"/api/*": {"origins": "*"}})

    manager = connection_manager or get_connection_manager()
    if start_connections:
        if connection_manager is None:
            start_connection_manager()
        else:
            manager.start()

    app.register_blueprint(api_bp, url_prefix="/api")

    @app.get("/health/live")
    def health_live():
        """Process liveness is independent from repository readiness."""

        return jsonify({"status": "LIVE", "jvm_started": jvm_is_started()}), 200

    @app.get("/health/ready")
    def health_ready():
        """Readiness forces a real Protégé server/session probe."""

        ready = manager.readiness(wait_timeout=CONNECTION_WAIT_SECONDS)
        payload = manager.status()
        return jsonify(payload), 200 if ready else 503

    @app.get("/health")
    def health():
        """Backward-compatible summary that never equates a proxy with readiness."""

        ready = manager.readiness(wait_timeout=CONNECTION_WAIT_SECONDS)
        payload = manager.status()
        payload["kb_loaded"] = ready
        return jsonify(payload), 200

    return app
