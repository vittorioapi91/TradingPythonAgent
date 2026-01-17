"""
Airflow plugin to display environment and wheel version information
"""
import os
import glob
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, render_template, request, g
from flask_appbuilder import BaseView, expose

# Get environment from environment variable
ENV = os.getenv("AIRFLOW_ENV", "unknown")

# Get wheel information
WHEEL_VERSION = "Not installed"
WHEEL_FILE = None
PACKAGE_INSTALLED = False

# Try multiple methods to detect installed wheel
# Method 1: Check if package can be imported (most reliable)
try:
    import trading_agent
    # Try to get version from package
    try:
        version = getattr(trading_agent, '__version__', 'unknown')
        # Try to get package name from installed distribution
        import importlib.metadata
        for dist in importlib.metadata.distributions():
            # Check if this distribution provides trading_agent
            if 'trading_agent' in dist.metadata.get('Name', '').lower():
                package_name = dist.metadata['Name']
                WHEEL_VERSION = f"{package_name} {dist.version}"
                WHEEL_FILE = package_name
                PACKAGE_INSTALLED = True
                break
    except (AttributeError, ImportError):
        # Package is importable but can't get version - mark as installed
        WHEEL_VERSION = "trading_agent (installed, version unknown)"
        PACKAGE_INSTALLED = True
except ImportError:
    # Package not importable, continue to other detection methods
    pass

# Method 2: Check installed distributions via importlib.metadata
if not PACKAGE_INSTALLED:
    try:
        import importlib.metadata
        # Try to find trading_agent package (check various name formats)
        for dist in importlib.metadata.distributions():
            name = dist.metadata.get("Name", "")
            # Check for trading_agent-dev, trading_agent_dev, trading-agent-dev, etc.
            if "trading_agent" in name.lower() or "trading-agent" in name.lower():
                WHEEL_VERSION = f"{name} {dist.version}"
                WHEEL_FILE = name
                PACKAGE_INSTALLED = True
                break
    except (ImportError, AttributeError):
        # Fallback to older importlib_metadata
        try:
            import importlib_metadata
            for dist in importlib_metadata.distributions():
                name = dist.metadata.get("Name", "")
                if "trading_agent" in name.lower() or "trading-agent" in name.lower():
                    WHEEL_VERSION = f"{name} {dist.version}"
                    WHEEL_FILE = name
                    PACKAGE_INSTALLED = True
                    break
        except ImportError:
            pass

# Method 3: Check for wheel files in wheels directory (fallback)
if not PACKAGE_INSTALLED:
    # Check multiple possible wheel directory locations
    wheel_dirs = [
        "/opt/airflow/wheels",  # Docker mount point
        os.path.join(os.path.dirname(__file__), "..", "..", "wheels"),  # Relative to plugin
    ]
    
    for wheel_dir in wheel_dirs:
        if os.path.exists(wheel_dir):
            # Note: setuptools converts hyphens to underscores in package names
            # So trading_agent-dev becomes trading_agent_dev in wheel filename
            # But the actual package name might be trading_agent-dev
            wheel_patterns = [
                f"trading_agent_{ENV}-*.whl",  # trading_agent_dev-0.1.0-py3-none-any.whl
                f"trading-agent-{ENV}-*.whl",   # trading-agent-dev-0.1.0-py3-none-any.whl
                "trading_agent_*-*.whl",        # Any trading_agent wheel
                "trading-agent-*-*.whl",       # Any trading-agent wheel
            ]
            
            for pattern in wheel_patterns:
                wheel_files = glob.glob(os.path.join(wheel_dir, pattern))
                if wheel_files:
                    # Sort by version and get the latest
                    wheel_files.sort(reverse=True)
                    wheel_name = os.path.basename(wheel_files[0])
                    # Extract version from filename
                    parts = wheel_name.replace(".whl", "").split("-")
                    if len(parts) >= 2:
                        # Reconstruct readable name
                        package_part = parts[0]  # trading_agent_dev or trading_agent
                        version_part = parts[1] if len(parts) > 1 else "unknown"
                        # Convert trading_agent_dev to trading_agent-dev
                        readable_name = package_part.replace("_", "-")
                        WHEEL_VERSION = f"{readable_name} {version_part} (wheel file available)"
                        WHEEL_FILE = wheel_name
                        break
                
                if WHEEL_FILE:
                    break
            
            if WHEEL_FILE:
                break


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


@bp.before_app_request
def inject_banner_data():
    """Inject banner data into Flask g for use in templates"""
    db_host = os.getenv("POSTGRES_HOST", "not set")
    db_port = os.getenv("POSTGRES_PORT", "not set")
    db_name = os.getenv("POSTGRES_DB", "not set")
    db_user = os.getenv("POSTGRES_USER", "not set")
    
    if db_host != "not set" and db_port != "not set":
        db_instance = f"{db_host}:{db_port}/{db_name}"
    else:
        db_instance = "not configured"
    
    g.airflow_env = ENV.upper()
    g.airflow_wheel_version = WHEEL_VERSION
    g.airflow_wheel_file = WHEEL_FILE
    g.airflow_db_instance = db_instance
    g.airflow_db_user = db_user


@bp.after_app_request
def inject_banner_into_dags_page(response):
    """Inject environment banner into DAGs page HTML response"""
    # Only modify HTML responses for DAGs page
    if (response.content_type and 
        'text/html' in response.content_type and 
        request.path in ['/home', '/dags', '/']):
        
        try:
            content = response.get_data(as_text=True)
            
            # Create banner HTML
            env_color = "#28a745" if ENV.upper() == "DEV" else "#ffc107" if ENV.upper() == "STAGING" else "#dc3545"
            env_text_color = "black" if ENV.upper() == "STAGING" else "white"
            
            db_host = os.getenv("POSTGRES_HOST", "not set")
            db_port = os.getenv("POSTGRES_PORT", "not set")
            db_name = os.getenv("POSTGRES_DB", "not set")
            db_user = os.getenv("POSTGRES_USER", "not set")
            
            if db_host != "not set" and db_port != "not set":
                db_instance = f"{db_host}:{db_port}/{db_name}"
            else:
                db_instance = "not configured"
            
            banner_html = f"""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px 20px; margin-bottom: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
        <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap;">
            <div>
                <strong style="font-size: 14px; opacity: 0.9;">Environment:</strong>
                <span style="background-color: {env_color}; color: {env_text_color}; padding: 4px 12px; border-radius: 4px; font-weight: bold; margin-left: 8px;">
                    {ENV.upper()}
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
            
            # Find a good insertion point - look for the DAGs heading or main content area
            # Try multiple insertion points
            insertion_points = [
                '<h1>DAGs</h1>',
                '<h1 class="',
                '<div class="container-fluid">',
                '<div id="dag-table"',
                '<div class="dag-list-container"',
            ]
            
            inserted = False
            for point in insertion_points:
                if point in content:
                    # Insert banner before the insertion point
                    content = content.replace(point, banner_html + '\n' + point, 1)
                    inserted = True
                    break
            
            # Fallback: insert after body tag if no specific point found
            if not inserted and '<body' in content:
                # Find the first content div after body
                body_end = content.find('>', content.find('<body'))
                if body_end > 0:
                    # Look for the main container or first div
                    next_div = content.find('<div', body_end)
                    if next_div > 0:
                        content = content[:next_div] + banner_html + '\n' + content[next_div:]
                        inserted = True
            
            if inserted:
                response.set_data(content)
                
        except Exception as e:
            # Silently fail - don't break the page if banner injection fails
            pass
    
    return response


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
