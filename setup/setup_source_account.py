import boto3
import json
import argparse
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_valid_aws_account_id(account_id):
    """Validate AWS account ID format"""
    return account_id and account_id.isdigit() and len(account_id) == 12

def get_aws_details():
    """Get current AWS account ID and region"""
    try:
        # Get region first
        region = os.environ.get('AWS_REGION')
        if not region or region == 'aws-global':
            region = os.environ.get('AWS_DEFAULT_REGION')
        
        # If no environment variables, try boto3 session
        if not region or region == 'aws-global':
            session = boto3.session.Session()
            region = session.region_name

        if not region or region == 'aws-global':
            raise ValueError("Unable to determine AWS region. Assign AWS region to the AWS_REGION env variable")

        # Create regular STS client to get account ID
        sts_client = boto3.client('sts', region_name=region)
        account_id = sts_client.get_caller_identity()['Account']
        
        return region, account_id
    except Exception as e:
        raise Exception(f"Failed to get AWS details: {str(e)}") from e

def create_metrics_uploader_role(iam, current_account_id, central_account_id):
    """Create PostgresMetricsUploader role with necessary permissions"""
    # Create policy document for cross-account access and metrics collection
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Resource": [
                    f"arn:aws:iam::{central_account_id}:role/CrossAccountS3Access"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "rds:DescribeDBInstances",
                    "rds:DescribeDBClusters",
                    "cloudwatch:GetMetricStatistics",
                    "pricing:GetProducts",
                    "pricing:DescribeServices"                    
                ],
                "Resource": "*"
            }
        ]
    }

    # EC2 service trust statement
    ec2_trust_statement = {
        "Effect": "Allow",
        "Principal": {
            "Service": "ec2.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
    }

    try:
        try:
            # Try to create the role first
            role = iam.create_role(
                RoleName='PostgresMetricsUploader',
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [ec2_trust_statement]
                })
            )
            logger.info("Created PostgresMetricsUploader role")
            
        except iam.exceptions.EntityAlreadyExistsException:
            logger.info("PostgresMetricsUploader role already exists")
            
            # Get existing trust policy
            existing_role = iam.get_role(RoleName='PostgresMetricsUploader')
            existing_trust_policy = existing_role['Role']['AssumeRolePolicyDocument']
            
            # Check if ec2.amazonaws.com service is already in the trust policy
            ec2_service_exists = False
            for statement in existing_trust_policy['Statement']:
                if (statement.get('Principal', {}).get('Service') == 'ec2.amazonaws.com' and 
                    statement.get('Action') == 'sts:AssumeRole' and 
                    statement.get('Effect') == 'Allow'):
                    ec2_service_exists = True
                    break
            
            # If ec2.amazonaws.com service is not in the trust policy, add it
            if not ec2_service_exists:
                logger.info("Adding EC2 service to trust policy")
                existing_trust_policy['Statement'].append(ec2_trust_statement)
                
                # Update the trust policy
                iam.update_assume_role_policy(
                    RoleName='PostgresMetricsUploader',
                    PolicyDocument=json.dumps(existing_trust_policy)
                )
                logger.info("Updated trust policy for PostgresMetricsUploader role")
            else:
                logger.info("EC2 service already exists in trust policy")

        # Attach or update the role policy
        iam.put_role_policy(
            RoleName='PostgresMetricsUploader',
            PolicyName='CrossAccountPolicy',
            PolicyDocument=json.dumps(policy_document)
        )
        logger.info("Attached policy to PostgresMetricsUploader role")

    except Exception as e:
        raise Exception(f"Failed to create/update metrics uploader role: {str(e)}") from e

def setup_source_account(central_account_id):
    """Set up resources in the source account"""
    try:
        if not is_valid_aws_account_id(central_account_id):
            raise ValueError(f"Invalid AWS account ID: {central_account_id}. Must be 12 digits.")

        # Get current account details
        region, current_account_id = get_aws_details()
        if not current_account_id:
            raise ValueError("Unable to determine current AWS account ID")

        # Create IAM client
        iam = boto3.client('iam')

        # Create role with necessary permissions
        create_metrics_uploader_role(iam, current_account_id, central_account_id)

        logger.info("Source account setup completed successfully")

    except Exception as e:
        raise Exception(f"Failed to set up source account: {str(e)}") from e

def main():
    parser = argparse.ArgumentParser(description='Set up source account resources for PostgreSQL metrics collection')
    parser.add_argument('--central-account-id', required=True, help='Central AWS account ID')
    args = parser.parse_args()

    setup_source_account(args.central_account_id)

if __name__ == "__main__":
    main()