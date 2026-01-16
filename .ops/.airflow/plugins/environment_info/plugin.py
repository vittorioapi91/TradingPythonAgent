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
    default_view = "info"

    @expose("/")
    def list(self):
        """Default list view - redirects to info"""
        return self.info()

    @expose("/info")
    def info(self):
        """Display environment and wheel information"""
        # Get database connection information
        db_host = os.getenv("POSTGRES_HOST", "not set")
        db_port = os.getenv("POSTGRES_PORT", "not set")
        db_name = os.getenv("POSTGRES_DB", "not set")
        db_user = os.getenv("POSTGRES_USER", "not set")
        
        # Construct database instance string
        if db_host != "not set" and db_port != "not set":
            db_instance = f"{db_host}:{db_port}/{db_name}"
        else:
            db_instance = "not configured"
        
        return self.render_template(
            "environment_info/info.html",
            env=ENV,
            wheel_version=WHEEL_VERSION,
            wheel_file=WHEEL_FILE,
            airflow_env=os.getenv("AIRFLOW_ENV", "not set"),
            git_branch=os.getenv("GIT_BRANCH", "not set"),
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_instance=db_instance,
        )


# Create blueprint
bp = Blueprint(
    "environment_info",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/environment_info",
)


@bp.app_context_processor
def inject_environment_info():
    """Inject environment info into all templates"""
    db_host = os.getenv("POSTGRES_HOST", "not set")
    db_port = os.getenv("POSTGRES_PORT", "not set")
    db_name = os.getenv("POSTGRES_DB", "not set")
    db_user = os.getenv("POSTGRES_USER", "not set")
    
    if db_host != "not set" and db_port != "not set":
        db_instance = f"{db_host}:{db_port}/{db_name}"
    else:
        db_instance = "not configured"
    
    return {
        "airflow_env": ENV.upper(),
        "airflow_wheel_version": WHEEL_VERSION,
        "airflow_wheel_file": WHEEL_FILE,
        "airflow_db_instance": db_instance,
        "airflow_db_user": db_user,
    }


@bp.route("/environment-badge")
def environment_badge():
    """Simple endpoint that returns environment info as text"""
    return f"Environment: {ENV} | Wheel: {WHEEL_VERSION}", 200, {"Content-Type": "text/plain"}


@bp.app_context_processor
def inject_environment_info():
    """Inject environment info into all templates"""
    db_host = os.getenv("POSTGRES_HOST", "not set")
    db_port = os.getenv("POSTGRES_PORT", "not set")
    db_name = os.getenv("POSTGRES_DB", "not set")
    db_user = os.getenv("POSTGRES_USER", "not set")
    
    if db_host != "not set" and db_port != "not set":
        db_instance = f"{db_host}:{db_port}/{db_name}"
    else:
        db_instance = "not configured"
    
    return {
        "airflow_env": ENV.upper(),
        "airflow_wheel_version": WHEEL_VERSION,
        "airflow_wheel_file": WHEEL_FILE,
        "airflow_db_instance": db_instance,
        "airflow_db_user": db_user,
    }


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
