"""
Airflow plugin to display environment and wheel version information
"""
import os
import glob
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, render_template
from flask_appbuilder import BaseView, expose

# Get environment from environment variable
ENV = os.getenv("AIRFLOW_ENV", "unknown")

# Get wheel information
WHEEL_VERSION = "Not installed"
WHEEL_FILE = None

# Try to find the installed wheel version
try:
    import importlib.metadata
    # Try to find trading_agent package
    for dist in importlib.metadata.distributions():
        if dist.metadata["Name"].startswith("trading_agent-"):
            WHEEL_VERSION = f"{dist.metadata['Name']} {dist.version}"
            WHEEL_FILE = dist.metadata["Name"]
            break
except (ImportError, AttributeError):
    # Fallback to older importlib_metadata or manual detection
    try:
        import importlib_metadata
        for dist in importlib_metadata.distributions():
            if dist.metadata["Name"].startswith("trading_agent-"):
                WHEEL_VERSION = f"{dist.metadata['Name']} {dist.version}"
                WHEEL_FILE = dist.metadata["Name"]
                break
    except ImportError:
        # Last resort: try to detect from wheel files
        wheel_dir = "/opt/airflow/wheels"
        if os.path.exists(wheel_dir):
            wheel_files = glob.glob(os.path.join(wheel_dir, f"trading_agent-{ENV}-*.whl"))
            if wheel_files:
                # Sort by version and get the latest
                wheel_files.sort(reverse=True)
                wheel_name = os.path.basename(wheel_files[0])
                # Extract version from filename: trading_agent-{env}-{version}-py3-none-any.whl
                parts = wheel_name.replace(".whl", "").split("-")
                if len(parts) >= 3:
                    WHEEL_VERSION = f"{parts[0]}-{parts[1]} {parts[2]}"
                    WHEEL_FILE = wheel_name


class EnvironmentInfoView(BaseView):
    """
    Custom view to display environment and wheel information
    """
    route_base = "/environment-info"

    @expose("/")
    def info(self):
        """Display environment and wheel information"""
        return self.render_template(
            "environment_info/info.html",
            env=ENV,
            wheel_version=WHEEL_VERSION,
            wheel_file=WHEEL_FILE,
            airflow_env=os.getenv("AIRFLOW_ENV", "not set"),
            git_branch=os.getenv("GIT_BRANCH", "not set"),
        )


# Create blueprint
bp = Blueprint(
    "environment_info",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/environment_info",
)


@bp.route("/environment-badge")
def environment_badge():
    """Simple endpoint that returns environment info as text"""
    return f"Environment: {ENV} | Wheel: {WHEEL_VERSION}", 200, {"Content-Type": "text/plain"}


class EnvironmentInfoPlugin(AirflowPlugin):
    """
    Airflow plugin to display environment and wheel information
    """
    name = "environment_info"
    appbuilder_views = [
        {
            "name": "Environment Info",
            "category": "Admin",
            "view": EnvironmentInfoView(),
        },
    ]
    flask_blueprints = [bp]
