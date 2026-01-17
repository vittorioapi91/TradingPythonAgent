pipeline {
    agent any
    
    // Disable periodic builds - only run on manual trigger or SCM changes (push)
    // This ensures pipelines run only when:
    // 1. Manually triggered by user (Build Now)
    // 2. Code is pushed to the repository (via SCM polling or webhook)
    // Note: triggers block removed - periodic builds disabled via Jenkins job configuration
    // Empty triggers block causes compilation error, so we rely on job-level configuration
    
    environment {
        KIND_CLUSTER = 'trading-cluster'
    }
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.GIT_COMMIT = sh(
                        script: 'git rev-parse HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_COMMIT_SHORT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_BRANCH = sh(
                        script: 'git rev-parse --abbrev-ref HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_URL = sh(
                        script: 'git config --get remote.origin.url',
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
                    // Note: Structure is now src/{subproject} (no project name in path)
                    if (env.IS_FEATURE_BRANCH == 'true') {
                        env.MODULE_PATH = "src/${env.SUBPROJECT_NAME}"
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
                    
                    // Get JIRA configuration from environment variables or credentials
                    def jiraUrl = env.JIRA_URL ?: 'https://vittorioapi91.atlassian.net'
                    def jiraUser = env.JIRA_USER ?: 'vittorioapi91'
                    def jiraToken = env.JIRA_API_TOKEN
                    
                    // Try to get token from Jenkins credentials if not in environment
                    if (!jiraToken) {
                        try {
                            withCredentials([string(credentialsId: 'jira-api-token', variable: 'JIRA_TOKEN')]) {
                                jiraToken = env.JIRA_TOKEN
                            }
                        } catch (Exception e) {
                            error("JIRA_API_TOKEN not found in environment variables or Jenkins credentials (ID: jira-api-token). Please configure one of them.")
                        }
                    }
                    
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
                    
                    // Get JIRA configuration from environment variables or credentials
                    def jiraUrl = env.JIRA_URL ?: 'https://vittorioapi91.atlassian.net'
                    def jiraUser = env.JIRA_USER ?: 'vittorioapi91'
                    def jiraToken = env.JIRA_API_TOKEN
                    
                    // Try to get token from Jenkins credentials if not in environment
                    if (!jiraToken) {
                        try {
                            withCredentials([string(credentialsId: 'jira-api-token', variable: 'JIRA_TOKEN')]) {
                                jiraToken = env.JIRA_TOKEN
                            }
                        } catch (Exception e) {
                            error("JIRA_API_TOKEN not found in environment variables or Jenkins credentials (ID: jira-api-token). Please configure one of them.")
                        }
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
                            echo "Available modules in src/:"
                            ls -d src/*/ 2>/dev/null | xargs -n 1 basename || echo "No modules found"
                            exit 1
                        fi
                        echo "✓ Module path exists: ${env.MODULE_PATH}"
                        echo "Module contents:"
                        ls -la "${env.MODULE_PATH}" | head -20
                    """
                }
            }
        }
        
        stage('Validate Airflow DAGs') {
            when {
                // Only validate DAGs if .airflow-dags directory exists
                expression { 
                    fileExists('src/.airflow-dags') 
                }
            }
            steps {
                script {
                    echo "Validating Airflow DAGs from src/.airflow-dags/..."
                    sh """
                        # Create virtual environment if it doesn't exist
                        if [ ! -d "venv" ]; then
                            python3 -m venv venv
                        fi
                        
                        # Use virtual environment's Python directly
                        VENV_PYTHON="venv/bin/python"
                        VENV_PIP="venv/bin/pip"
                        
                        # Upgrade pip first
                        \${VENV_PIP} install --quiet --upgrade pip
                        
                        # Install Airflow (try latest stable, fallback to any version)
                        \${VENV_PIP} install --quiet apache-airflow || {
                            echo "Warning: Could not install apache-airflow, trying without version constraint"
                            \${VENV_PIP} install --quiet 'apache-airflow>=2.0.0' || {
                                echo "⚠️  Could not install Airflow. Skipping DAG validation."
                                exit 0
                            }
                        }
                        
                        # Install only critical project dependencies (needed for DAG imports)
                        \${VENV_PIP} install --quiet tqdm pandas psycopg2-binary requests python-dotenv || echo "Warning: Some dependencies failed"
                        
                        # Set environment variables for DAG execution context
                        export AIRFLOW_HOME=/tmp/airflow_home
                        export AIRFLOW__CORE__DAGS_FOLDER=src/.airflow-dags
                        export AIRFLOW__CORE__LOAD_EXAMPLES=False
                        export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow_home/airflow.db
                        
                        # Create minimal Airflow config
                        mkdir -p \${AIRFLOW_HOME}
                        echo "[core]" > \${AIRFLOW_HOME}/airflow.cfg
                        echo "dags_folder = src/.airflow-dags" >> \${AIRFLOW_HOME}/airflow.cfg
                        echo "load_examples = False" >> \${AIRFLOW_HOME}/airflow.cfg
                        echo "[database]" >> \${AIRFLOW_HOME}/airflow.cfg
                        echo "sql_alchemy_conn = sqlite:////tmp/airflow_home/airflow.db" >> \${AIRFLOW_HOME}/airflow.cfg
                        
                        # Initialize Airflow database (use migrate instead of init for newer Airflow versions)
                        \${VENV_PYTHON} -m airflow db migrate || {
                            # Fallback: try init for older versions
                            \${VENV_PYTHON} -m airflow db init 2>/dev/null || echo "Database may already exist"
                        }
                        
                        # Validate DAGs by listing them (this will parse and validate)
                        echo "Validating DAG files from src/.airflow-dags/..."
                        \${VENV_PYTHON} -m airflow dags list || {
                            echo "⚠️  DAG validation failed. Check output above for details."
                            exit 1
                        }
                        
                        echo "✓ All DAGs validated successfully"
                    """
                }
            }
        }
        
        stage('Install Airflow DAGs') {
            when {
                // Only install DAGs if .airflow-dags directory exists
                expression { 
                    fileExists('src/.airflow-dags') 
                }
            }
            steps {
                script {
                    echo "Installing Airflow DAGs from src/.airflow-dags/ to ../infra-platform/airflow/dags/..."
                    sh """
                        # Run install-dags.sh script
                        if [ -f "./install-dags.sh" ]; then
                            chmod +x ./install-dags.sh
                            ./install-dags.sh
                        else
                            echo "⚠️  install-dags.sh not found. Skipping DAG installation."
                        fi
                    """
                }
            }
        }
        
        stage('Run Tests') {
            steps {
                script {
                    echo "Running unit tests..."
                    sh """
                        # Create virtual environment if it doesn't exist
                        if [ ! -d "venv" ]; then
                            python3 -m venv venv
                        fi
                        
                        # Use virtual environment's Python directly (no need to activate)
                        VENV_PYTHON="venv/bin/python"
                        VENV_PIP="venv/bin/pip"
                        
                        # Upgrade pip first
                        \${VENV_PIP} install --quiet --upgrade pip
                        
                        # Install project dependencies (environment-specific)
                        # Determine environment from branch (using POSIX-compatible [ instead of [[)
                        if [ "\${GIT_BRANCH}" = "staging" ]; then
                            REQ_FILE="requirements-staging.txt"
                        elif [ "\${GIT_BRANCH}" = "main" ]; then
                            REQ_FILE="requirements-prod.txt"
                        else
                            # dev/* branches
                            REQ_FILE="requirements-dev.txt"
                        fi
                        
                        # Fallback to base requirements.txt if env-specific file doesn't exist
                        if [ ! -f "\${REQ_FILE}" ]; then
                            echo "⚠️  \${REQ_FILE} not found, using requirements.txt"
                            REQ_FILE="requirements.txt"
                        fi
                        
                        # Check if requirements need to be installed/updated
                        # Compare requirements file modification time with venv's pip cache or installed packages
                        REQ_MTIME=\$(stat -c %Y "\${REQ_FILE}" 2>/dev/null || stat -f %m "\${REQ_FILE}" 2>/dev/null || echo "0")
                        REQ_CACHE_FILE="venv/.requirements-\$(basename \${REQ_FILE}).mtime"
                        
                        if [ -f "\${REQ_CACHE_FILE}" ]; then
                            CACHED_MTIME=\$(cat "\${REQ_CACHE_FILE}")
                            if [ "\${REQ_MTIME}" = "\${CACHED_MTIME}" ]; then
                                echo "✓ Requirements already installed (no changes detected)"
                                echo "  Skipping installation from \${REQ_FILE}"
                            else
                                echo "Requirements file changed, reinstalling from \${REQ_FILE}..."
                                \${VENV_PIP} install --quiet -r \${REQ_FILE}
                                echo "\${REQ_MTIME}" > "\${REQ_CACHE_FILE}"
                            fi
                        else
                            echo "Installing dependencies from \${REQ_FILE}..."
                            \${VENV_PIP} install --quiet -r \${REQ_FILE}
                            echo "\${REQ_MTIME}" > "\${REQ_CACHE_FILE}"
                        fi
                        
                        # Create test results directory
                        mkdir -p test-results
                        
                        # Run tests with verbose output and JUnit XML for Jenkins
                        # Note: pytest will exit with non-zero if tests fail, which is expected
                        set +e  # Don't exit on error immediately
                        \${VENV_PYTHON} -m pytest tests/ --junitxml=test-results/junit.xml --html=test-results/report.html --self-contained-html
                        TEST_EXIT_CODE=\$?
                        set -e  # Re-enable exit on error
                        
                        # Check if test results were generated
                        if [ -f "test-results/junit.xml" ]; then
                            echo "✓ Test results generated: test-results/junit.xml"
                        else
                            echo "⚠️  Warning: JUnit XML file was not generated"
                        fi
                        
                        if [ -f "test-results/report.html" ]; then
                            echo "✓ HTML report generated: test-results/report.html"
                        else
                            echo "⚠️  Warning: HTML report was not generated"
                        fi
                        
                        # Exit with the test exit code
                        if [ \$TEST_EXIT_CODE -ne 0 ]; then
                            echo "⚠️  Some tests failed. Check output above for details."
                            exit \$TEST_EXIT_CODE
                        fi
                        
                        echo "✓ All tests passed"
                    """
                }
            }
            post {
                always {
                    // Archive test results (JUnit XML)
                    script {
                        try {
                            junit 'test-results/junit.xml'
                        } catch (Exception e) {
                            echo "Warning: Could not archive JUnit test results: ${e.message}"
                        }
                        
                        // Publish HTML report if it exists
                        try {
                            if (fileExists('test-results/report.html')) {
                                publishHTML([
                                    reportName: 'Test Report',
                                    reportDir: 'test-results',
                                    reportFiles: 'report.html',
                                    keepAll: true,
                                    alwaysLinkToLastBuild: true,
                                    allowMissing: true
                                ])
                            } else {
                                echo "HTML test report not found, skipping HTML publishing"
                            }
                        } catch (Exception e) {
                            echo "Warning: Could not publish HTML test report: ${e.message}"
                        }
                    }
                }
            }
        }
        
        stage('Check Registry and Build Docker Image') {
            steps {
                script {
                    def latestTag = env.ENV_SUFFIX ? "${env.IMAGE_NAME}:${env.ENV_SUFFIX}-latest" : "${env.IMAGE_NAME}:latest"
                    def REGISTRY_HOST = "localhost:5000"
                    def REGISTRY_URL = "http://\${REGISTRY_HOST}"
                    def REGISTRY_IMAGE = "\${REGISTRY_HOST}/${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                    def BASE_REGISTRY_IMAGE = "\${REGISTRY_HOST}/hmm-model-training-base:base"
                    
                    // Check if rebuild is needed
                    def rebuildNeeded = sh(
                        script: """
                            # Check if registry is accessible
                            if ! curl -s -f \${REGISTRY_URL}/v2/ > /dev/null 2>&1; then
                                echo "true"  # Registry not accessible, need to build
                                exit 0
                            fi
                            
                            # Check if base image exists in registry
                            if ! curl -s -f \${REGISTRY_URL}/v2/hmm-model-training-base/manifests/base > /dev/null 2>&1; then
                                echo "true"  # Base image not in registry, need to build
                                exit 0
                            fi
                            
                            # Check if target image exists in registry
                            if curl -s -f \${REGISTRY_URL}/v2/${env.IMAGE_NAME}/manifests/${env.IMAGE_TAG} > /dev/null 2>&1; then
                                echo "false"  # Image exists in registry, no rebuild needed
                            else
                                echo "true"   # Image not in registry, rebuild needed
                            fi
                        """,
                        returnStdout: true
                    ).trim() == "true"
                    
                    if (!rebuildNeeded) {
                        echo "✓ Image ${REGISTRY_IMAGE} exists in registry - skipping build"
                        echo "Kind cluster will pull from registry directly"
                    } else {
                        echo "Building Docker image: ${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                        // Use timeout wrapper to prevent Jenkins from thinking the script is hung
                        // activity: true extends timeout based on activity (output), unit: 'MINUTES'
                        timeout(time: 60, unit: 'MINUTES', activity: true) {
                            sh """
                                set -x  # Enable command tracing for better visibility
                                
                                # Ensure buildx is available
                                if ! docker buildx version >/dev/null 2>&1; then
                                    echo "ERROR: docker buildx is not available"
                                    echo "Please ensure the Jenkins container has buildx installed"
                                    echo "Rebuild the Jenkins image: docker build -t jenkins-custom:lts -f .ops/.docker/Dockerfile.jenkins .ops/.docker"
                                    exit 1
                                fi
                                
                                echo "[\$(date +%H:%M:%S)] Docker buildx version:"
                                docker buildx version
                                
                                # Create and use builder instance
                                echo "[\$(date +%H:%M:%S)] Setting up buildx builder..."
                                if ! docker buildx inspect builder >/dev/null 2>&1; then
                                    echo "[\$(date +%H:%M:%S)] Creating buildx builder instance..."
                                    docker buildx create --name builder --use --driver docker-container || {
                                        echo "[\$(date +%H:%M:%S)] Failed to create buildx builder, trying to use existing..."
                                        docker buildx use builder 2>/dev/null || docker buildx use default
                                    }
                                else
                                    echo "[\$(date +%H:%M:%S)] Using existing buildx builder..."
                                    docker buildx use builder
                                fi
                                
                                # Verify builder is ready
                                echo "[\$(date +%H:%M:%S)] Verifying buildx builder..."
                                docker buildx inspect --bootstrap
                                
                                # Try to pull base image from registry first
                                echo "[\$(date +%H:%M:%S)] Attempting to pull base image from registry..."
                                if curl -s -f \${REGISTRY_URL}/v2/hmm-model-training-base/manifests/base > /dev/null 2>&1; then
                                    docker pull \${BASE_REGISTRY_IMAGE} || {
                                        echo "[\$(date +%H:%M:%S)] Failed to pull from registry, checking local..."
                                    }
                                    # Tag pulled image for local use
                                    docker tag \${BASE_REGISTRY_IMAGE} hmm-model-training-base:base 2>/dev/null || true
                                fi
                                
                                # Check if base image exists locally (from registry or already built)
                                if ! docker images --format '{{.Repository}}:{{.Tag}}' | grep -q '^hmm-model-training-base:base\$'; then
                                    echo "[\$(date +%H:%M:%S)] ❌ ERROR: Base trading agent image 'hmm-model-training-base:base' not found!"
                                    echo ""
                                    echo "Base images must be built and pushed to registry before running pipelines."
                                    echo "To build and push the base image, run:"
                                    echo "  .ops/.docker/push-base-images.sh"
                                    echo ""
                                    exit 1
                                fi
                                
                                echo "[\$(date +%H:%M:%S)] ✓ Base trading agent image found: hmm-model-training-base:base"
                                
                                # Switch to default builder for local base images (container builder can't see host images)
                                # The default builder has access to local images, container builder doesn't
                                echo "[\$(date +%H:%M:%S)] Switching to default builder for local base image access..."
                                docker buildx use default || docker buildx use builder
                                
                                # Build incremental image (FROM base) - only copies source code
                                echo "[\$(date +%H:%M:%S)] Building incremental Docker image (FROM base)..."
                                docker buildx build \
                                    --platform linux/amd64 \
                                    -f .ops/.kubernetes/Dockerfile.model-training \
                                    -t ${env.IMAGE_NAME}:${env.IMAGE_TAG} \
                                    -t ${latestTag} \
                                    -t \${REGISTRY_IMAGE} \
                                    -t \${REGISTRY_HOST}/${latestTag} \
                                    --load \
                                    --progress=plain \
                                    .
                                
                                echo "[\$(date +%H:%M:%S)] ✓ Docker image built successfully: ${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                                
                                # Push to registry
                                echo "[\$(date +%H:%M:%S)] Pushing image to local registry..."
                                docker push \${REGISTRY_IMAGE} || {
                                    echo "[\$(date +%H:%M:%S)] ⚠️  Warning: Failed to push to registry (registry may not be accessible)"
                                    echo "Image will be loaded into kind directly"
                                }
                                docker push \${REGISTRY_HOST}/${latestTag} || true
                                
                                echo "[\$(date +%H:%M:%S)] ✓ Image pushed to registry: \${REGISTRY_IMAGE}"
                            """
                        }
                    }
                }
            }
        }
        
        stage('Make Image Available to Kind Cluster') {
            steps {
                script {
                    def latestTag = env.ENV_SUFFIX ? "${env.IMAGE_NAME}:${env.ENV_SUFFIX}-latest" : "${env.IMAGE_NAME}:latest"
                    def REGISTRY_HOST = "localhost:5000"
                    def REGISTRY_IMAGE = "\${REGISTRY_HOST}/${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                    
                    echo "Making image available to kind cluster: ${env.KIND_CLUSTER}"
                    sh """
                        # Try to pull from registry in kind cluster (faster than kind load)
                        # If registry is not accessible from kind, fall back to kind load
                        if kubectl run --rm -i --restart=Never --image=curlimages/curl:latest test-registry-\$\$ --context kind-${env.KIND_CLUSTER} -- curl -s -f http://\${REGISTRY_HOST}/v2/ > /dev/null 2>&1; then
                            echo "Registry is accessible from kind cluster - images will be pulled automatically"
                            echo "No need to load images manually"
                        else
                            echo "Registry not accessible from kind - loading images directly..."
                            kind load docker-image ${env.IMAGE_NAME}:${env.IMAGE_TAG} --name ${env.KIND_CLUSTER} || true
                            kind load docker-image ${latestTag} --name ${env.KIND_CLUSTER} || true
                        fi
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
        
        stage('Build Wheel') {
            steps {
                script {
                    echo "Building environment-specific wheel..."
                    
                    // Determine environment from branch (same logic as earlier in pipeline)
                    def wheelEnv = 'dev'
                    if (env.GIT_BRANCH == 'staging') {
                        wheelEnv = 'staging'
                    } else if (env.GIT_BRANCH == 'main' || env.GIT_BRANCH == 'master') {
                        wheelEnv = 'prod'
                    }
                    
                    echo "Building wheel for environment: ${wheelEnv}"
                    sh """
                        # Ensure setuptools and wheel are installed
                        python3 -m pip install --quiet --upgrade setuptools wheel || true
                        
                        # Build wheel (Jenkins already checked out the source)
                        # Pass environment explicitly to build-wheel.sh
                        # Note: build-wheel.sh now requires at least one platform flag (e.g., --macosx-arm64)
                        ./build-wheel.sh ${wheelEnv} --macosx-arm64 || {
                            echo "⚠️  Wheel build failed. Check output above for details."
                            exit 1
                        }
                        
                        # Install wheel to Airflow wheels directory
                        .ops/.airflow/install-wheel.sh ${wheelEnv} || {
                            echo "⚠️  Wheel installation failed. Check output above for details."
                            exit 1
                        }
                        
                        echo "✓ Wheel built and installed successfully for ${wheelEnv} environment"
                    """
                }
            }
        }
    }
    
    post {
        success {
            echo "✓ Pipeline succeeded! Image ${env.IMAGE_NAME}:${env.IMAGE_TAG} deployed to ${env.KIND_CLUSTER} (${env.NAMESPACE})"
            
            // Post success status to GitHub
            script {
                try {
                    // Extract repo owner/name from git URL
                    def repoUrl = env.GIT_URL.replace('.git', '').replace('git@github.com:', 'https://github.com/').replace('https://github.com/', '')
                    def repoParts = repoUrl.split('/')
                    def repoOwner = repoParts.size() >= 1 ? repoParts[0] : 'vittorioapi91'
                    def repoName = repoParts.size() >= 2 ? repoParts[1] : 'TradingPythonAgent'
                    
                    // Get GitHub token from credentials (configure in Jenkins)
                    def githubToken = env.GITHUB_TOKEN ?: ''
                    
                    if (githubToken) {
                        // Post status using curl (works without GitHub plugin)
                        sh """
                            curl -X POST \\
                              -H "Authorization: token ${githubToken}" \\
                              -H "Accept: application/vnd.github.v3+json" \\
                              "https://api.github.com/repos/${repoOwner}/${repoName}/statuses/${env.GIT_COMMIT}" \\
                              -d '{
                                "state": "success",
                                "target_url": "${env.BUILD_URL}",
                                "description": "Jenkins pipeline passed",
                                "context": "jenkins/pipeline"
                              }' || echo "Warning: Could not post status to GitHub"
                        """
                    } else {
                        echo "Warning: GITHUB_TOKEN not set. Cannot post status to GitHub."
                        echo "Configure GITHUB_TOKEN environment variable in Jenkins job settings"
                    }
                } catch (Exception e) {
                    echo "Warning: Could not post status to GitHub: ${e.message}"
                }
            }
        }
        failure {
            echo "✗ Pipeline failed. Check logs for details."
            
            // Post failure status to GitHub
            script {
                try {
                    def repoUrl = env.GIT_URL.replace('.git', '').replace('git@github.com:', 'https://github.com/').replace('https://github.com/', '')
                    def repoParts = repoUrl.split('/')
                    def repoOwner = repoParts.size() >= 1 ? repoParts[0] : 'vittorioapi91'
                    def repoName = repoParts.size() >= 2 ? repoParts[1] : 'TradingPythonAgent'
                    
                    def githubToken = env.GITHUB_TOKEN ?: ''
                    
                    if (githubToken) {
                        sh """
                            curl -X POST \\
                              -H "Authorization: token ${githubToken}" \\
                              -H "Accept: application/vnd.github.v3+json" \\
                              "https://api.github.com/repos/${repoOwner}/${repoName}/statuses/${env.GIT_COMMIT}" \\
                              -d '{
                                "state": "failure",
                                "target_url": "${env.BUILD_URL}",
                                "description": "Jenkins pipeline failed",
                                "context": "jenkins/pipeline"
                              }' || echo "Warning: Could not post status to GitHub"
                        """
                    } else {
                        echo "Warning: GITHUB_TOKEN not set. Cannot post status to GitHub."
                    }
                } catch (Exception e) {
                    echo "Warning: Could not post status to GitHub: ${e.message}"
                }
            }
        }
        always {
            // Clean up old images (optional - keep last 10 builds)
            // Note: Base images (tagged with :base) are NEVER deleted
            script {
                def pattern = env.ENV_SUFFIX ? "^${env.ENV_SUFFIX}-[0-9]+-" : "^[0-9]+-"
            sh """
                    # Clean up old incremental images, but preserve base images
                    docker images ${env.IMAGE_NAME} --format '{{.Repository}}:{{.Tag}}' | \\
                        grep -v ':base\$' | \\
                        sed 's/.*://' | \\
                        grep -E '${pattern}' | \\
                        sort -t- -k${env.ENV_SUFFIX ? '2' : '1'} -nr | \\
                        tail -n +11 | \\
                        xargs -r -I {} docker rmi ${env.IMAGE_NAME}:{} || true
            """
            }
        }
    }
}

