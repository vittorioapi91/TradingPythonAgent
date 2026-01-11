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
                    
                    // Determine environment based on branch
                    if (env.GIT_BRANCH == 'dev' || env.GIT_BRANCH.startsWith('dev/')) {
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
                    
                    echo "Building for branch: ${env.GIT_BRANCH}"
                    echo "Environment: ${env.ENV_SUFFIX ?: 'production'}"
                    echo "Image: ${env.IMAGE_NAME}:${env.IMAGE_TAG}"
                    echo "Namespace: ${env.NAMESPACE}"
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

