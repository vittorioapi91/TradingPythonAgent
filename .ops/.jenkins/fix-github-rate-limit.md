# Fix GitHub API Rate Limiting in Jenkins

## Problem
Jenkins is using authenticated credentials but still hitting the 60 requests/hour limit instead of 5000/hour.

## Solution

### Step 1: Configure GitHub API Usage Settings

1. Go to Jenkins → **Manage Jenkins** → **Configure System**
2. Scroll down to **"GitHub API usage"** section
3. Change the strategy:
   - **From**: "Evenly distribute GitHub API requests" (very conservative)
   - **To**: "Only when near or above limit" (uses full authenticated limit)
4. **Verify credentials are selected**:
   - Under "GitHub API usage", ensure the correct credentials are selected
   - Should show: `vittorioapi/******` or credential ID `39a94d87-8a43-468b-9138-14b4f86d7b93`
5. Click **Save**

### Step 2: Verify Credentials

1. Go to **Manage Jenkins** → **Manage Credentials** → **Global**
2. Verify credential `39a94d87-8a43-468b-9138-14b4f86d7b93` exists
3. Check that it's a valid GitHub token/password

### Step 3: Restart Jenkins (if needed)

After changing the API usage strategy, you may need to restart Jenkins:

```bash
docker restart jenkins
```

### Step 4: Verify Rate Limit

After the next pipeline run, check the logs. You should see:
- **Before**: "Current quota for Github API usage has 46 remaining (1 over budget). Next quota of 60"
- **After**: Should show 5000 requests/hour limit or no rate limiting messages

## Alternative: Use GitHub Personal Access Token

If username/password isn't working, use a GitHub Personal Access Token:

1. Create token on GitHub: Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Scopes needed: `repo`, `read:org`, `read:user`
3. In Jenkins: Manage Credentials → Add → Secret text
4. Use the token as the password (username can be anything, or use token as both)

## Why This Happens

Jenkins has a global rate limiting strategy that can be overly conservative even with authentication. The "Evenly distribute" strategy assumes you want to spread requests evenly, which limits you to 60/hour. Changing to "Only when near or above limit" allows Jenkins to use the full authenticated limit of 5000/hour.
