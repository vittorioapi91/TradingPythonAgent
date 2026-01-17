# Jenkins Workspace Cleanup

## Overview

The Jenkins workspace directory (`.ops/.jenkins/data/workspace/`) can grow very large (10GB+) over time due to:
- Build artifacts from past pipeline runs
- Virtual environments created during builds
- Checked-out code from various branches
- Temporary files and cache

## Cleanup Script

Use `cleanup-workspace.sh` to regularly clean up old workspaces and build artifacts.

### Usage

```bash
# Dry-run to see what would be deleted (safe to run)
./cleanup-workspace.sh --dry-run

# Actually clean up (default: keeps workspaces from last 1 day, keeps last 3 per job)
./cleanup-workspace.sh

# Custom options
./cleanup-workspace.sh --keep-days=3 --keep-workspaces=2

# Dry-run with custom options
./cleanup-workspace.sh --dry-run --keep-days=3 --keep-workspaces=2
```

### Options

- `--dry-run`: Show what would be deleted without actually deleting
- `--keep-days=N`: Keep workspaces that were modified in the last N days (default: 1, meaning everything older than yesterday is deleted)
- `--keep-workspaces=N`: Keep the last N workspace versions per job (default: 3)

### What Gets Cleaned

1. **Old workspaces**: Workspaces older than `--keep-days` days (if not modified recently)
2. **Old workspace versions**: For each job, only keeps the last `--keep-workspaces` versions
3. **Virtual environments**: All `venv`, `.venv`, `.venv-jenkins` directories (recreated on next build)
4. **Build artifacts**: `build/`, `dist/`, `*.egg-info` directories
5. **Python cache**: `__pycache__/`, `*.pyc`, `*.pyo` files

### Automatic Daily Cleanup (2 AM)

Set up automatic daily cleanup at 2 AM:

```bash
# Setup automated daily cleanup at 2 AM (macOS LaunchAgent)
./setup-cleanup-schedule.sh
```

This will:
- Create a LaunchAgent to run cleanup daily at 2:00 AM
- Keep only workspaces from the last 1 day (everything older than yesterday is deleted)
- Log output to `.ops/.jenkins/cleanup.log`

**Status:**
```bash
# Check if scheduled
launchctl list | grep com.tradingagent.jenkins-cleanup

# View logs
tail -f .ops/.jenkins/cleanup.log
```

**To disable:**
```bash
launchctl unload ~/Library/LaunchAgents/com.tradingagent.jenkins-cleanup.plist
```

### Example Output

```
Jenkins Workspace Cleanup
========================
Workspace directory: .ops/.jenkins/data/workspace
Keep workspaces from last 7 days
Keep last 5 workspaces per job
Dry run: false

Total workspace size before cleanup:  10G

Cleaning old artifacts in: DEV-5_trading_agent-fundamentals
  Deleting venv: .ops/.jenkins/data/workspace/DEV-5_trading_agent-fundamentals/venv (1.2G)
Removing old workspace: TradingPythonAgent_main
  Deleting: .ops/.jenkins/data/workspace/TradingPythonAgent_main (1.5G)

Total workspace size after cleanup:  2.1G

Cleanup completed!
```

### Safety

- Always test with `--dry-run` first
- The script preserves active workspaces (modified in last N days)
- Virtual environments are safe to delete (recreated on next build)
- Build artifacts are safe to delete (recreated on next build)
