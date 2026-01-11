pipeline {
    agent any
    
    environment {
        IMAGE_NAME = 'hmm-model-training'
        IMAGE_TAG = "${env.BUILD_NUMBER}-${env.GIT_COMMIT.take(7)}"
        KIND_CLUSTER = 'trading-cluster'
        NAMESPACE = 'trading-monitoring'
        JOB_NAME = 'hmm-model-calibration'
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
                    env.IMAGE_TAG = "${env.BUILD_NUMBER}-${env.GIT_COMMIT_SHORT}"
                }
            }
        }
        
        stage('Build Docker Image') {
            steps {
                script {
                    echo "Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
                    sh """
                        docker build \
                            -f .ops/.kubernetes/Dockerfile.model-training \
                            -t ${IMAGE_NAME}:${IMAGE_TAG} \
                            -t ${IMAGE_NAME}:latest \
                            .
                    """
                }
            }
        }
        
        stage('Load Image into Kind Cluster') {
            steps {
                script {
                    echo "Loading image into kind cluster: ${KIND_CLUSTER}"
                    sh """
                        kind load docker-image ${IMAGE_NAME}:${IMAGE_TAG} --name ${KIND_CLUSTER}
                        kind load docker-image ${IMAGE_NAME}:latest --name ${KIND_CLUSTER}
                    """
                }
            }
        }
        
        stage('Update Kubernetes Job') {
            steps {
                script {
                    echo "Updating Kubernetes Job with new image"
                    sh """
                        # Set the image in the job (if it exists)
                        kubectl set image job/${JOB_NAME} \
                            model-training=${IMAGE_NAME}:${IMAGE_TAG} \
                            -n ${NAMESPACE} \
                            2>/dev/null || echo "Job doesn't exist yet, will be created on apply"
                        
                        # Apply the job manifest (will create or update)
                        kubectl apply -f .ops/.kubernetes/hmm-model-training-job.yaml
                    """
                }
            }
        }
        
        stage('Verify Deployment') {
            steps {
                script {
                    echo "Verifying job deployment"
                    sh """
                        kubectl get job ${JOB_NAME} -n ${NAMESPACE} || true
                        kubectl get pods -l app=hmm-model,component=training -n ${NAMESPACE} || true
                    """
                }
            }
        }
    }
    
    post {
        success {
            echo "✓ Pipeline succeeded! Image ${IMAGE_NAME}:${IMAGE_TAG} deployed to ${KIND_CLUSTER}"
        }
        failure {
            echo "✗ Pipeline failed. Check logs for details."
        }
        always {
            // Clean up old images (optional - keep last 10 builds)
            sh """
                docker images ${IMAGE_NAME} --format '{{.Tag}}' | \\
                    grep -E '^[0-9]+-' | \\
                    sort -t- -k1 -nr | \\
                    tail -n +11 | \\
                    xargs -r -I {} docker rmi ${IMAGE_NAME}:{} || true
            """
        }
    }
}

