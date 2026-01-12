"""
TradingPythonAgent - A Python trading agent package
"""

# Load environment configuration early when package is imported
# This ensures environment variables are available throughout the application
try:
    from . import config  # noqa: F401
except ImportError:
    # If config module is not available (e.g., missing dependencies), continue without it
    pass

__version__ = "0.1.0"
