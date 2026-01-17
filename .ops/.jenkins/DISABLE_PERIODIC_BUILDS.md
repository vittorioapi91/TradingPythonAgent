# Disable Periodic Builds in Jenkins

This document explains how to ensure Jenkins pipelines run only on manual triggers or when code is pushed, not on a periodic schedule.

## Jenkinsfile Configuration

The `Jenkinsfile` includes an empty `triggers {}` block to explicitly disable periodic builds. This ensures:

- ✅ **Manual triggers**: You can always click "Build Now" to trigger a build
- ✅ **Push-triggered builds**: Builds run when code is pushed (via SCM polling or webhooks)
- ❌ **Periodic builds**: No automatic scheduled builds (cron disabled)

## Jenkins Job Configuration

To ensure periodic builds are disabled in the Jenkins UI:

### Option 1: Via Jenkins Web UI

1. Go to your Jenkins job
2. Click **Configure**
3. Scroll to **Build Triggers** section
4. **Uncheck** any of the following if enabled:
   - ☐ **Build periodically** (cron-based schedules)
   - ☐ **Poll SCM** (if you want to disable polling, though this is usually fine for push detection)
5. **Keep enabled**:
   - ✅ **GitHub hook trigger for GITScm polling** (if available - enables webhook-based builds)
   - ✅ **GitHub Pull Request Builder** (if you use PR builds)
6. Click **Save**

### Option 2: Via Jenkins Job DSL (if using)

If you're using Jenkins Job DSL or Configuration as Code, ensure no `triggers` are configured for periodic builds.

### Recommended Configuration

**For push-triggered builds (recommended):**

1. **Enable GitHub webhooks** (best option):
   - In GitHub: Settings → Webhooks → Add webhook
   - Payload URL: `http://your-jenkins-url/github-webhook/`
   - Content type: `application/json`
   - Events: `Just the push event`
   - This triggers builds immediately on push

2. **OR enable SCM polling** (fallback):
   - In Jenkins job: Configure → Build Triggers → Poll SCM
   - Schedule: `H/5 * * * *` (every 5 minutes)
   - This checks for changes and triggers builds on push

3. **Disable periodic builds**:
   - Uncheck "Build periodically" in Build Triggers
   - This prevents builds from running on a schedule

## Verification

To verify periodic builds are disabled:

1. Check the Jenkinsfile has `triggers {}` block (empty or with only SCM/webhook triggers)
2. Check Jenkins job configuration: Build Triggers section should not have "Build periodically" checked
3. Test by:
   - Pushing code → build should trigger
   - Clicking "Build Now" → build should trigger
   - Waiting → no automatic builds should occur

## Troubleshooting

### Builds still running periodically

- Check if there's a cron trigger in the Jenkinsfile (should be empty `triggers {}`)
- Check Jenkins job configuration for "Build periodically" checkbox
- Check if there are multiple Jenkins jobs with the same name
- Check Jenkins system configuration for global triggers

### Builds not triggering on push

- Verify GitHub webhook is configured and working (check webhook delivery logs in GitHub)
- Verify SCM polling is enabled if not using webhooks
- Check Jenkins logs for SCM polling errors
- Verify Jenkins has access to the repository
