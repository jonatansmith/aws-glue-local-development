pipeline {
    agent any

    environment {
        AWS_REGION = 'us-east-1' // Set your AWS region
        STACK_NAME = 'my-glue-job-stack' // Replace with your CloudFormation stack name
        TEMPLATE_FILE = 'cloudformation-template.yaml' // Replace with your CloudFormation template file
        GLUE_SCRIPT_PATH = 'awssample.py' // Path to the Glue job script in the repo
        S3_BUCKET = 'aws-glue-temporary-515951668509-us-east-1' // Replace with your S3 bucket name
        S3_KEY = 'glue/glue-job-script.py' // Path in S3
    }

    stages {
        stage('CloudFormation Create/Update') {
            steps {
                script {
                    sh """
                    aws cloudformation deploy \
                        --region $AWS_REGION \
                        --stack-name $STACK_NAME \
                        --template-file $TEMPLATE_FILE \
                        --capabilities CAPABILITY_NAMED_IAM \
                        --parameter-overrides S3BucketName=$S3_BUCKET \
                        GlueJobName="my-glue-job" GlueScriptS3Path=$S3_KEY
                    """
                }
            }
        }

        stage('Upload Glue Job Script to S3') {
            steps {
                script {
                    sh """
                    aws s3 cp $GLUE_SCRIPT_PATH s3://$S3_BUCKET/$S3_KEY
                    """
                }
            }
        }

        stage('Update Glue Job Script in Glue Job') {
            steps {
                script {
                    sh """
                    aws glue update-job \
                        --region $AWS_REGION \
                        --job-name my-glue-job \
                        --job-update '{"Command": {"ScriptLocation": "s3://$S3_BUCKET/$S3_KEY"}}'
                    """
                }
            }
        }
    }

    post {
        success {
            echo 'Pipeline completed successfully!'
        }
        failure {
            echo 'Pipeline failed. Please check the logs.'
        }
    }
}
