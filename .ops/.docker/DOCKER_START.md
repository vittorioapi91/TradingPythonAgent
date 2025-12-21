# Starting Docker on macOS

## Quick Start

### Option 1: Using Docker Desktop (GUI) - Recommended

1. **Open Docker Desktop**:
   - Press `Cmd + Space` to open Spotlight
   - Type "Docker" and press Enter
   - Or find Docker Desktop in Applications folder

2. **Wait for Docker to start**:
   - Look for the Docker whale icon in the menu bar (top right)
   - Wait until it shows "Docker Desktop is running"

3. **Verify it's running**:
   ```bash
   docker info
   ```

### Option 2: Using Command Line

```bash
# Open Docker Desktop from terminal
open -a Docker

# Wait a few seconds, then verify
docker info
```

### Option 3: Using Launchpad

1. Open Launchpad (F4 or pinch gesture)
2. Find "Docker" application
3. Click to launch

## Verify Docker is Running

After starting Docker Desktop, verify it's working:

```bash
# Check Docker version
docker --version

# Check Docker daemon status
docker info

# Test with a simple command
docker run hello-world
```

## Troubleshooting

### Docker Desktop won't start

1. **Check if it's already running**:
   ```bash
   ps aux | grep -i docker
   ```

2. **Restart Docker Desktop**:
   - Quit Docker Desktop (right-click whale icon → Quit)
   - Wait 10 seconds
   - Restart Docker Desktop

3. **Check system requirements**:
   - macOS 10.15 or later
   - At least 4GB RAM
   - VirtualBox or HyperKit installed

4. **Reset Docker Desktop** (if needed):
   - Docker Desktop → Settings → Troubleshoot → Reset to factory defaults

### "Docker daemon is not running" error

If you see this error:
```bash
Cannot connect to the Docker daemon. Is the docker daemon running on this host?
```

**Solution:**
1. Make sure Docker Desktop is open and running
2. Check the Docker icon in menu bar shows "Docker Desktop is running"
3. Wait 30-60 seconds after starting Docker Desktop for it to fully initialize

### Permission denied errors

If you get permission errors:
```bash
# Add your user to docker group (if needed)
# On macOS, this is usually handled automatically by Docker Desktop
```

## Starting Services After Docker is Running

Once Docker is running, you can start the monitoring services (Grafana, Prometheus, MLflow, Airflow, Postgres, Redis, Feast):

```bash
cd .ops/.docker
./start-docker-monitoring.sh
```

Or manually:

```bash
docker-compose up -d
```

## Auto-start Docker on Login

To automatically start Docker when you log in:

1. Open Docker Desktop
2. Go to Settings (gear icon)
3. Check "Start Docker Desktop when you log in"

## Alternative: Docker via Homebrew (Advanced)

If you installed Docker via Homebrew and want to use the CLI-only version:

```bash
# Install Docker via Homebrew (if not using Docker Desktop)
brew install --cask docker

# Start Docker service (requires Docker Desktop or Colima)
# For Colima (lightweight alternative):
brew install colima
colima start
```

**Note:** For most users, Docker Desktop is the recommended and easiest option.

## Quick Reference

| Action | Command |
|--------|---------|
| Start Docker Desktop | `open -a Docker` |
| Check if running | `docker info` |
| Stop Docker Desktop | Right-click whale icon → Quit |
| View Docker status | Check menu bar icon |
| Test Docker | `docker run hello-world` |

