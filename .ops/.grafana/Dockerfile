FROM grafana/grafana:latest

# Copy provisioning configurations
COPY provisioning/ /etc/grafana/provisioning/

# Copy dashboards
COPY dashboards/ /var/lib/grafana/dashboards/

# Set permissions
USER root
RUN chown -R grafana:grafana /var/lib/grafana/dashboards
USER grafana

EXPOSE 3000

