"""
Airflow webserver configuration
This file allows customization of the Airflow UI
"""
import os

# Custom template folder for environment banner
# This will be used to override templates
TEMPLATE_FOLDER = os.path.join(os.path.dirname(__file__), "plugins", "environment_info", "templates")
