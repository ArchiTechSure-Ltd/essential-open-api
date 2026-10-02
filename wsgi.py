"""WSGI entry point for running the Essential Open API application."""

from essential_open_api import create_app


app = create_app()

__all__ = ["app"]
