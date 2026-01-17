"""
Airflow local settings for UI customization
This file allows adding UI alerts/banners to the Airflow UI
"""
from airflow.configuration import conf
# Removed import - not available in Airflow 2.9.3
# from airflow.utils.dag_processing_context import get_current_context
import os

# Get environment info
ENV = os.getenv("AIRFLOW_ENV", "unknown").upper()

# Get wheel information
WHEEL_VERSION = "Not installed"
try:
    import importlib.metadata
    for dist in importlib.metadata.distributions():
        if dist.metadata["Name"].startswith("trading_agent-"):
            WHEEL_VERSION = f"{dist.metadata['Name']} {dist.version}"
            break
except (ImportError, AttributeError):
    try:
        import importlib_metadata
        for dist in importlib_metadata.distributions():
            if dist.metadata["Name"].startswith("trading_agent-"):
                WHEEL_VERSION = f"{dist.metadata['Name']} {dist.version}"
                break
    except ImportError:
        pass

# Get database info
db_host = os.getenv("POSTGRES_HOST", "not set")
db_port = os.getenv("POSTGRES_PORT", "not set")
db_name = os.getenv("POSTGRES_DB", "not set")
db_user = os.getenv("POSTGRES_USER", "not set")

if db_host != "not set" and db_port != "not set":
    db_instance = f"{db_host}:{db_port}/{db_name}"
else:
    db_instance = "not configured"

# Create environment badge HTML
env_color = "#28a745" if ENV == "DEV" else "#ffc107" if ENV == "STAGING" else "#dc3545"
env_text_color = "black" if ENV == "STAGING" else "white"

banner_html = f"""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px 20px; margin-bottom: 10px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
        <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap;">
            <div>
                <strong style="font-size: 14px; opacity: 0.9;">Environment:</strong>
                <span style="background-color: {env_color}; color: {env_text_color}; padding: 4px 12px; border-radius: 4px; font-weight: bold; margin-left: 8px;">
                    {ENV}
                </span>
            </div>
            <div>
                <strong style="font-size: 14px; opacity: 0.9;">Wheel:</strong>
                <span style="font-family: monospace; margin-left: 8px;">{WHEEL_VERSION}</span>
            </div>
            <div>
                <strong style="font-size: 14px; opacity: 0.9;">Database:</strong>
                <span style="font-family: monospace; margin-left: 8px;">{db_user}@{db_instance}</span>
            </div>
        </div>
        <div>
            <a href="/environment-info/" style="color: white; text-decoration: underline; font-size: 13px;">View Details →</a>
        </div>
    </div>
</div>
"""

# UIAlert for dashboard (shows on home page)
try:
    from airflow.utils.ui_alerts import UIAlert
    
    DASHBOARD_UIALERTS = [
        UIAlert(
            message=banner_html,
            category="info",
            html=True,
        ),
    ]
except ImportError:
    # Fallback if UIAlert is not available
    DASHBOARD_UIALERTS = []
