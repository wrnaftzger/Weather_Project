"""
weather_globe_web.py
====================
Dash web app that renders a regression-kriged global temperature surface.

Run:
    python visualization\\weather_globe_web.py

Then open:
    http://127.0.0.1:8051
"""

from __future__ import annotations

from visualization.globe_app import build_app


if __name__ == "__main__":
    app = build_app()
    print("Starting weather globe web app on http://127.0.0.1:8051")
    app.run(debug=False, host="127.0.0.1", port=8051)
