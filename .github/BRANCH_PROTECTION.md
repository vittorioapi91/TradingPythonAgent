# Branch Protection Setup

This document describes how to configure branch protection rules to prevent merging branches that haven't completed their Jenkins pipelines.

## Overview

The project uses GitHub Actions workflows to enforce that:
- **Merges to `main`**: Require successful Jenkins pipeline on `staging` branch
- **Merges to `staging`**: Require successful Jenkins pipeline on the source `dev/*` branch

## GitHub Actions Workflows

### `block-merge-without-jenkins.yml`

This workflow runs on pull requests targeting `main` or `staging` and:
1. Checks if the required Jenkins pipeline has completed successfully
2. Blocks the merge if the pipeline hasn't passed
3. Uses GitHub Status API to check Jenkins build status

## Jenkins Configuration

### Required: Post Build Status to GitHub

Jenkins must be configured to post build status to GitHub. The `Jenkinsfile` includes code to post status, but you need to:

1. **Install GitHub Plugin in Jenkins**:
   - Go to Jenkins → Manage Jenkins → Manage Plugins
   - Install "GitHub Plugin" and "GitHub Branch Source Plugin"

2. **Configure GitHub Credentials**:
   - Go to Jenkins → Manage Jenkins → Manage Credentials
   - Add GitHub Personal Access Token with `repo:status` permission
   - Store it as a credential Jenkins can use

3. **Configure Job to Post Status**:
   - The `Jenkinsfile` includes `GitHubCommitStatusSetter` steps
   - Ensure the job has access to GitHub credentials
   - Jenkins will automatically post status to GitHub after each build

### Alternative: Manual Status Check

If Jenkins status posting isn't configured, the workflow will block all merges for safety. You can:

1. **Temporarily disable the check** (not recommended)
2. **Configure Jenkins to post status** (recommended)
3. **Use a different status check mechanism**

## GitHub Branch Protection Rules

For additional protection, configure GitHub branch protection rules:

### For `main` branch:
1. Go to Settings → Branches → Add rule
2. Branch name pattern: `main`
3. Enable:
   - ✅ Require a pull request before merging
   - ✅ Require status checks to pass before merging
   - ✅ Require branches to be up to date before merging
4. Required status checks:
   - `Block Merge Without Jenkins Status / block-merge-to-main`

### For `staging` branch:
1. Go to Settings → Branches → Add rule
2. Branch name pattern: `staging`
3. Enable:
   - ✅ Require a pull request before merging
   - ✅ Require status checks to pass before merging
   - ✅ Require branches to be up to date before merging
4. Required status checks:
   - `Block Merge Without Jenkins Status / block-merge-to-staging`

## Testing

To test the protection:

1. Create a PR from a `dev/*` branch to `staging`
2. The workflow should check if the dev branch's Jenkins pipeline passed
3. If not, the PR will be blocked

1. Create a PR from `staging` to `main`
2. The workflow should check if staging's Jenkins pipeline passed
3. If not, the PR will be blocked

## Troubleshooting

### "Jenkins status check not configured"

- Ensure Jenkins GitHub plugin is installed
- Verify Jenkins has GitHub credentials configured
- Check that Jenkinsfile includes status posting steps
- Verify Jenkins can access the GitHub repository

### "Status check always fails"

- Check Jenkins build logs for status posting errors
- Verify GitHub token has `repo:status` permission
- Ensure the repository URL in Jenkinsfile matches your repo

### "Merge still allowed despite failed pipeline"

- Verify branch protection rules are enabled in GitHub
- Check that the required status check is listed in branch protection
- Ensure the workflow is running (check Actions tab)
