# Fix GitHub Authentication in Jenkins

## Problem

You're seeing this error:
```
Could not update commit status. Message: {"message": "Requires authentication", "status": "401"}
```

This happens because Jenkins needs a GitHub Personal Access Token to update commit statuses.

## Quick Fix (5 minutes)

### Step 1: Create GitHub Personal Access Token

1. Go to: https://github.com/settings/tokens
2. Click **Generate new token** → **Generate new token (classic)**
3. Name: `Jenkins CI/CD`
4. Expiration: 90 days (or your preference)
5. Select scopes:
   - ✅ **`repo`** (includes `repo:status` for commit status updates)
6. Click **Generate token**
7. **Copy the token** (you won't see it again!)

### Step 2: Add Token to Jenkins

1. Go to Jenkins → **Manage Jenkins** → **Manage Credentials**
2. Click **Global** (or your domain)
3. Click **Add Credentials**
4. Configure:
   - **Kind**: `Secret text`
   - **Secret**: Paste your GitHub token
   - **ID**: `github-token` (or any ID you prefer)
   - **Description**: `GitHub Personal Access Token for CI/CD`
5. Click **OK**

### Step 3: Configure Your Multibranch Pipeline

1. Go to your **TradingPythonAgent** job in Jenkins
2. Click **Configure**
3. Scroll to **Branch Sources**
4. Under your GitHub source, click **Advanced...**
5. Find **Credentials** dropdown
6. Select the credential you just created (e.g., `github-token`)
7. Click **Save**

### Step 4: Test

1. Trigger a new build
2. Check the logs - you should no longer see "Requires authentication" errors
3. Check GitHub - commit statuses should update automatically

## Alternative: Use Environment Variable

If you prefer environment variables:

1. Go to Jenkins → **Manage Jenkins** → **Configure System**
2. Scroll to **Global properties**
3. Check **Environment variables**
4. Click **Add**:
   - **Name**: `GITHUB_TOKEN`
   - **Value**: Your GitHub Personal Access Token
5. Click **Save**

**Note**: Using credentials is more secure than environment variables.

## Verify It's Working

After configuration, you should see:
- ✅ No "401 Requires authentication" errors in build logs
- ✅ Commit statuses updating on GitHub
- ✅ Green/red status indicators on commits in GitHub

## Troubleshooting

### Still Getting 401 Errors?

1. **Check token hasn't expired**
2. **Verify token has `repo` scope** (includes `repo:status`)
3. **Ensure credential ID matches** what's configured in your pipeline
4. **Check Jenkins logs**: Manage Jenkins → System Log

### Token Expired?

1. Generate a new token in GitHub
2. Update the credential in Jenkins
3. Restart Jenkins if needed

## Current Setup

- **Repository**: `vittorioapi91/TradingPythonAgent`
- **Required Scope**: `repo:status` (included in `repo` scope)
- **Credential Type**: Secret text (Personal Access Token)
