pipeline {
    agent any
    
    environment {
        KIND_CLUSTER = 'trading-cluster'
    }
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.GIT_COMMIT_SHORT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_BRANCH = sh(
                        script: 'git rev-parse --abbrev-ref HEAD',
                        returnStdout: true
                    ).trim()
                    
                    // Parse branch pattern: dev/{jira_issue}/{project}-{subproject}
                    // Examples: dev/PROJ-123/trading_agent-fundamentals, dev/ISS-456/trading_agent-macro
                    env.JIRA_ISSUE = ''
                    env.PROJECT_NAME = ''
                    env.SUBPROJECT_NAME = ''
                    env.IS_FEATURE_BRANCH = 'false'
                    
                    // Pattern: dev/{JIRA_KEY-NUMBER}/{project}-{subproject}
                    // JIRA issue format: PROJECT_KEY-NUMBER (e.g., PROJ-123, ISS-456)
                    def featureBranchPattern = ~/^dev\/([A-Z]+-\d+)\/([^-]+)-(.+)$/
                    def matcher = env.GIT_BRANCH =~ featureBranchPattern
                    
                    if (matcher) {
                        env.IS_FEATURE_BRANCH = 'true'
                        env.JIRA_ISSUE = matcher[0][1]      // e.g., 'PROJ-123'
                        env.PROJECT_NAME = matcher[0][2]    // e.g., 'trading_agent'
                        env.SUBPROJECT_NAME = matcher[0][3] // e.g., 'fundamentals', 'macro'
                    }
                    
                    // Determine environment based on branch
                    // Feature branches (dev/{jira_issue}/...) and staging branch use dev environment
                    if (env.GIT_BRANCH == 'staging' || env.GIT_BRANCH.startsWith('dev/')) {
                        env.ENV_SUFFIX = 'dev'
                        env.IMAGE_NAME = 'hmm-model-training-dev'
                        env.NAMESPACE = 'trading-monitoring-dev'
                        env.JOB_NAME = 'hmm-model-calibration-dev'
                        env.IMAGE_TAG = "dev-${env.BUILD_NUMBER}-${env.GIT_COMMIT_SHORT}"
                    } else {
                        // Default to production/main
                        env.ENV_SUFFIX = ''
                        env.IMAGE_NAME = 'hmm-model-training'
                        env.NAMESPACE = 'trading-monitoring'
                        env.JOB_NAME = 'hmm-model-calibration'
                        env.IMAGE_TAG = "${env.BUILD_NUMBER}-${env.GIT_COMMIT_SHORT}"
                    }
                    
                    // Set module-specific paths (using relative paths from workspace root)
                    if (env.IS_FEATURE_BRANCH == 'true') {
                        env.MODULE_PATH = "src/${env.PROJECT_NAME}/${env.SUBPROJECT_NAME}"
                    } else {
                        env.MODULE_PATH = ''
                    }
                    
                    echo "Building for branch: ${env.GIT_BRANCH}"
                    echo "Environment: ${env.ENV_SUFFIX ?: 'production'}"
                    if (env.IS_FEATURE_BRANCH == 'true') {
                        echo "Feature branch detected: JIRA issue=${env.JIRA_ISSUE}, project=${env.PROJECT_NAME}, subproject=${env.SUBPROJECT_NAME}"
                        echo "Module path: ${env.MODULE_PATH}"
                    }
                    echo "Image: ${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                    echo "Namespace: ${env.NAMESPACE}"
                }
            }
        }
        
        // JIRA connectivity test (only for feature branches)
        stage('Test JIRA Connection') {
            when {
                expression { env.IS_FEATURE_BRANCH == 'true' }
            }
            steps {
                script {
                    echo "Testing JIRA connection..."
                    
                    // Get JIRA configuration from environment variables
                    def jiraUrl = env.JIRA_URL ?: 'https://vittorioapi91.atlassian.net'
                    def jiraUser = env.JIRA_USER ?: error("JIRA_USER environment variable is required")
                    def jiraToken = env.JIRA_API_TOKEN ?: error("JIRA_API_TOKEN environment variable is required")
                    
                    // Ensure JIRA URL doesn't have trailing slash
                    jiraUrl = jiraUrl.replaceAll(/\/+$/, '')
                    
                    echo "JIRA URL: ${jiraUrl}"
                    echo "JIRA User: ${jiraUser}"
                    echo "JIRA Token length: ${jiraToken.length()} characters"
                    echo "JIRA Token starts with: ${jiraToken.take(10)}..."
                    
                    // Test connection by getting current user info (doesn't require specific issue)
                    def testApiUrl = "${jiraUrl}/rest/api/3/myself"
                    
                    // Try with email as username first (most common)
                    def responseCode = sh(
                        script: """
                            curl -v -s -o /tmp/jira_test_response.json -w '%{http_code}' \\
                                -u '${jiraUser}:${jiraToken}' \\
                                -X GET \\
                                -H 'Accept: application/json' \\
                                '${testApiUrl}' 2>&1 | tee /tmp/jira_test_curl_debug.log || true
                        """,
                        returnStdout: true
                    ).trim()
                    
                    // Extract HTTP status code
                    responseCode = responseCode.split('\n')[-1].trim()
                    
                    // Show debug info for troubleshooting
                    if (responseCode != '200') {
                        echo "Debug information:"
                        sh """
                            echo "Response code: ${responseCode}"
                            echo "Curl debug log (showing auth header info):"
                            grep -i 'authorization\\|www-authenticate\\|401\\|403' /tmp/jira_test_curl_debug.log | head -10 || cat /tmp/jira_test_curl_debug.log | tail -30
                            echo ""
                            echo "Response body:"
                            cat /tmp/jira_test_response.json | head -20 || echo "No response body"
                        """
                        
                        // If 401, try alternative: account ID instead of email
                        if (responseCode == '401') {
                            echo "Attempting alternative authentication method..."
                            // Try with account ID (extract from email if possible, or use email as-is)
                            def accountId = jiraUser
                            responseCode = sh(
                                script: """
                                    curl -s -o /tmp/jira_test_response2.json -w '%{http_code}' \\
                                        -u '${accountId}:${jiraToken}' \\
                                        -X GET \\
                                        -H 'Accept: application/json' \\
                                        '${testApiUrl}' 2>&1
                                """,
                                returnStdout: true
                            ).trim()
                            responseCode = responseCode.split('\n')[-1].trim()
                            
                            if (responseCode == '200') {
                                echo "✓ JIRA connection successful (using account ID)"
                                sh """
                                    echo "User info:"
                                    cat /tmp/jira_test_response2.json | python3 -m json.tool 2>/dev/null | head -20 || cat /tmp/jira_test_response2.json | head -10
                                    rm -f /tmp/jira_test_response2.json
                                """
                            }
                        }
                    }
                    
                    if (responseCode == '200') {
                        echo "✓ JIRA connection successful"
                        sh """
                            echo "User info:"
                            cat /tmp/jira_test_response.json | python3 -m json.tool 2>/dev/null | head -20 || cat /tmp/jira_test_response.json | head -10
                            rm -f /tmp/jira_test_response.json /tmp/jira_test_curl_debug.log
                        """
                    } else if (responseCode == '401' || responseCode == '403') {
                        echo "⚠️  WARNING: JIRA authentication failed (HTTP ${responseCode}). Please check JIRA_USER and JIRA_API_TOKEN credentials. Note: New tokens may take up to a minute to activate. Pipeline will continue."
                    } else {
                        echo "⚠️  WARNING: JIRA connection failed (HTTP ${responseCode}). Please check JIRA_URL (${jiraUrl}) and network connectivity. Pipeline will continue."
                    }
                }
            }
        }
        
        // JIRA issue validation stage (only for feature branches)
        stage('Validate JIRA Issue') {
            when {
                expression { env.IS_FEATURE_BRANCH == 'true' }
            }
            steps {
                script {
                    echo "Validating JIRA issue: ${env.JIRA_ISSUE}"
                    
                    // Get JIRA configuration from environment variables
                    def jiraUrl = env.JIRA_URL ?: 'https://vittorioapi91.atlassian.net'
                    def jiraUser = env.JIRA_USER
                    def jiraToken = env.JIRA_API_TOKEN
                    
                    if (!jiraUser || !jiraToken) {
                        echo "⚠️  WARNING: JIRA_USER or JIRA_API_TOKEN not set. Skipping JIRA issue validation."
                        return
                    }
                    
                    // Ensure JIRA URL doesn't have trailing slash
                    jiraUrl = jiraUrl.replaceAll(/\/+$/, '')
                    
                    // Construct JIRA API endpoint
                    def jiraApiUrl = "${jiraUrl}/rest/api/3/issue/${env.JIRA_ISSUE}"
                    
                    echo "JIRA Issue: ${env.JIRA_ISSUE}"
                    echo "API Endpoint: ${jiraApiUrl}"
                    
                    // Validate JIRA issue exists using curl
                    def responseCode = sh(
                        script: """
                            curl -v -s -o /tmp/jira_response.json -w '%{http_code}' \\
                                -u '${jiraUser}:${jiraToken}' \\
                                -X GET \\
                                -H 'Accept: application/json' \\
                                '${jiraApiUrl}' 2>&1 | tee /tmp/jira_curl_debug.log || true
                        """,
                        returnStdout: true
                    ).trim()
                    
                    // Extract HTTP status code (last line should be the code)
                    responseCode = responseCode.split('\n')[-1].trim()
                    
                    // Show debug info for troubleshooting
                    if (responseCode != '200') {
                        echo "Debug information:"
                        sh """
                            echo "Response code: ${responseCode}"
                            echo "Curl debug log:"
                            cat /tmp/jira_curl_debug.log | tail -20 || true
                            echo ""
                            echo "Response body:"
                            cat /tmp/jira_response.json | head -50 || true
                        """
                    }
                    
                    if (responseCode == '200') {
                        echo "✓ JIRA issue ${env.JIRA_ISSUE} exists and is accessible"
                        // Optionally parse and display issue details
                        sh """
                            echo "Issue details:"
                            cat /tmp/jira_response.json | python3 -m json.tool 2>/dev/null | head -30 || cat /tmp/jira_response.json | head -20
                            rm -f /tmp/jira_response.json /tmp/jira_curl_debug.log
                        """
                    } else if (responseCode == '404') {
                        echo "⚠️  WARNING: JIRA issue ${env.JIRA_ISSUE} does not exist (HTTP 404). Please verify the issue exists at ${jiraUrl}/browse/${env.JIRA_ISSUE}. Pipeline will continue."
                    } else if (responseCode == '401' || responseCode == '403') {
                        echo "⚠️  WARNING: Authentication failed when accessing JIRA (HTTP ${responseCode}). Please check JIRA_USER and JIRA_API_TOKEN. Pipeline will continue."
                    } else {
                        echo "⚠️  WARNING: Failed to validate JIRA issue ${env.JIRA_ISSUE} (HTTP ${responseCode}). Please check JIRA_URL and network connectivity. Pipeline will continue."
                    }
                }
            }
        }
        
        // Module-specific validation stage (only for feature branches)
        stage('Validate Module') {
            when {
                expression { env.IS_FEATURE_BRANCH == 'true' }
            }
            steps {
                script {
                    echo "Validating module path: ${env.MODULE_PATH}"
                    sh """
                        if [ ! -d "${env.MODULE_PATH}" ]; then
                            echo "ERROR: Module path does not exist: ${env.MODULE_PATH}"
                            echo "Available modules in src/${env.PROJECT_NAME}/:"
                            ls -d src/${env.PROJECT_NAME}/*/ 2>/dev/null | xargs -n 1 basename || echo "No modules found"
                            exit 1
                        fi
                        echo "✓ Module path exists: ${env.MODULE_PATH}"
                        echo "Module contents:"
                        ls -la "${env.MODULE_PATH}" | head -20
                    """
                }
            }
        }
        
        stage('Run Tests') {
            steps {
                script {
                    echo "Running unit tests..."
                    sh """
                        # Install pytest and plugins if not already installed
                        python3 -m pip install --quiet pytest pytest-mock pytest-html || pip3 install --quiet pytest pytest-mock pytest-html || pip install --quiet pytest pytest-mock pytest-html
                        
                        # Create test results directory
                        mkdir -p test-results
                        
                        # Run tests with verbose output and JUnit XML for Jenkins
                        python3 -m pytest tests/ -v --tb=short --junitxml=test-results/junit.xml --html=test-results/report.html --self-contained-html || {
                            echo "⚠️  Some tests failed. Check output above for details."
                            exit 1
                        }
                        
                        echo "✓ All tests passed"
                    """
                }
            }
            post {
                always {
                    // Archive test results
                    junit 'test-results/junit.xml'
                    publishHTML([
                        reportName: 'Test Report',
                        reportDir: 'test-results',
                        reportFiles: 'report.html',
                        keepAll: true
                    ])
                }
            }
        }
        
        stage('Build Docker Image') {
            steps {
                script {
                    def latestTag = env.ENV_SUFFIX ? "${env.IMAGE_NAME}:${env.ENV_SUFFIX}-latest" : "${env.IMAGE_NAME}:latest"
                    echo "Building Docker image: ${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                    sh """
                        docker build \
                            -f .ops/.kubernetes/Dockerfile.model-training \
                            -t ${env.IMAGE_NAME}:${env.IMAGE_TAG} \
                            -t ${latestTag} \
                            .
                    """
                }
            }
        }
        
        stage('Load Image into Kind Cluster') {
            steps {
                script {
                    def latestTag = env.ENV_SUFFIX ? "${env.IMAGE_NAME}:${env.ENV_SUFFIX}-latest" : "${env.IMAGE_NAME}:latest"
                    echo "Loading image into kind cluster: ${env.KIND_CLUSTER}"
                    sh """
                        kind load docker-image ${env.IMAGE_NAME}:${env.IMAGE_TAG} --name ${env.KIND_CLUSTER}
                        kind load docker-image ${latestTag} --name ${env.KIND_CLUSTER}
                    """
                }
            }
        }
        
        stage('Create Namespace (if needed)') {
            steps {
                script {
                    echo "Ensuring namespace ${env.NAMESPACE} exists"
                    sh """
                        kubectl create namespace ${env.NAMESPACE} --context kind-${env.KIND_CLUSTER} 2>/dev/null || echo "Namespace ${env.NAMESPACE} already exists"
                    """
                }
            }
        }
        
        stage('Update Kubernetes Job') {
            steps {
                script {
                    echo "Updating Kubernetes Job with new image"
                    sh """
                        # Create a temporary YAML file with namespace updated
                        sed "s/namespace: trading-monitoring/namespace: ${env.NAMESPACE}/g; s/trading-monitoring\\.svc\\.cluster\\.local/${env.NAMESPACE}.svc.cluster.local/g" \
                            .ops/.kubernetes/hmm-model-training-job.yaml > /tmp/hmm-model-training-job-${env.NAMESPACE}.yaml
                        
                        # Set the image in the job (if it exists)
                        kubectl set image job/${env.JOB_NAME} \
                            model-training=${env.IMAGE_NAME}:${env.IMAGE_TAG} \
                            -n ${env.NAMESPACE} \
                            --context kind-${env.KIND_CLUSTER} \
                            2>/dev/null || echo "Job doesn't exist yet, will be created on apply"
                        
                        # Apply the job manifest (will create or update)
                        kubectl apply -f /tmp/hmm-model-training-job-${env.NAMESPACE}.yaml --context kind-${env.KIND_CLUSTER}
                        
                        # Clean up temporary file
                        rm -f /tmp/hmm-model-training-job-${env.NAMESPACE}.yaml
                    """
                }
            }
        }
        
        stage('Verify Deployment') {
            steps {
                script {
                    echo "Verifying job deployment"
                    sh """
                        kubectl get job ${env.JOB_NAME} -n ${env.NAMESPACE} --context kind-${env.KIND_CLUSTER} || true
                        kubectl get pods -l app=hmm-model,component=training -n ${env.NAMESPACE} --context kind-${env.KIND_CLUSTER} || true
                    """
                }
            }
        }
    }
    
    post {
        success {
            echo "✓ Pipeline succeeded! Image ${env.IMAGE_NAME}:${env.IMAGE_TAG} deployed to ${env.KIND_CLUSTER} (${env.NAMESPACE})"
        }
        failure {
            echo "✗ Pipeline failed. Check logs for details."
        }
        always {
            // Clean up old images (optional - keep last 10 builds)
            script {
                def pattern = env.ENV_SUFFIX ? "^${env.ENV_SUFFIX}-[0-9]+-" : "^[0-9]+-"
            sh """
                    docker images ${env.IMAGE_NAME} --format '{{.Tag}}' | \\
                        grep -E '${pattern}' | \\
                        sort -t- -k${env.ENV_SUFFIX ? '2' : '1'} -nr | \\
                    tail -n +11 | \\
                        xargs -r -I {} docker rmi ${env.IMAGE_NAME}:{} || true
            """
            }
        }
    }
}

