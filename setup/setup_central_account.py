import boto3
import json
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_valid_aws_account_id(account_id):
    """Validate AWS account ID format"""
    return account_id and account_id.isdigit() and len(account_id) == 12

def create_s3_bucket(s3, bucket_name):
    """Create S3 bucket if it doesn't exist"""
    try:
        if s3.head_bucket(Bucket=bucket_name):
            logger.info(f"Bucket {bucket_name} already exists")
    except Exception as e:
        logger.info(f"Bucket {bucket_name} does not exist, creating it")
        try:
            s3.create_bucket(Bucket=bucket_name)
        except Exception as create_error:
            # Log warning and continue since this is handled
            logger.warning(f"Failed to create bucket {bucket_name}: {str(create_error)}")
            raise Exception(f"Failed to create S3 bucket {bucket_name}: {str(create_error)}") from create_error

def create_direct_access_role(iam, bucket_name, account_id, region):
    """Create PostgresMetricsUploader role"""
    direct_access_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:ListBucket"                    
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            },
            {
                # Add permission for pricing and instance specification file access
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/reference/rds_aurora_pricing.xlsx",
                    f"arn:aws:s3:::{bucket_name}/reference/instance_specifications.json"
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
            },
            {
                "Effect": "Allow",
                "Action": [
                    "athena:GetDatabase",
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:GetWorkGroup",
                    "athena:StopQueryExecution",
                    "athena:ListWorkGroups"
                ],
                "Resource": [
                    f"arn:aws:athena:{region}:{account_id}:workgroup/primary"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreateDatabase",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:BatchDeleteTable",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition"
                ],
                "Resource": [
                    f"arn:aws:glue:{region}:{account_id}:catalog",
                    f"arn:aws:glue:{region}:{account_id}:catalog/awsdatacatalog",
                    f"arn:aws:glue:{region}:{account_id}:database/awsdatacatalog/serverless_migration_db",
                    f"arn:aws:glue:{region}:{account_id}:table/awsdatacatalog/serverless_migration_db/*",
                    f"arn:aws:glue:{region}:{account_id}:database/serverless_migration_db",
				    f"arn:aws:glue:{region}:{account_id}:table/serverless_migration_db/*"
                ]
            }            
        ]
    }

    # EC2 service trust policy statement
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
            direct_role = iam.create_role(
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
            PolicyName='DirectAccessPolicy',
            PolicyDocument=json.dumps(direct_access_policy)
        )
        logger.info("Attached policy to PostgresMetricsUploader role")

    except Exception as e:
        raise Exception(f"Error creating/updating PostgresMetricsUploader role: {str(e)}") from e

def create_cross_account_role(iam, bucket_name, account_id):
    """Create CrossAccountS3Access role"""
    cross_account_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:PutObjectAcl"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            },
            {
                # Add permission for pricing file access
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/reference/rds_aurora_pricing.xlsx",
                    f"arn:aws:s3:::{bucket_name}/reference/instance_specifications.json"
                ]
            }
        ]
    }

    # Create initial trust policy with the PostgresMetricsUploader role
    initial_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{account_id}:role/PostgresMetricsUploader"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        cross_account_role = iam.create_role(
            RoleName='CrossAccountS3Access',
            AssumeRolePolicyDocument=json.dumps(initial_trust_policy)
        )
        logger.info("Created CrossAccountS3Access role")
    except iam.exceptions.EntityAlreadyExistsException:
        logger.info("CrossAccountS3Access role already exists")
    except Exception as e:
        raise Exception(f"Failed to create CrossAccountS3Access role: {str(e)}") from e        

    try:
        iam.put_role_policy(
            RoleName='CrossAccountS3Access',
            PolicyName='CrossAccountPolicy',
            PolicyDocument=json.dumps(cross_account_policy)
        )
        logger.info("Attached policy to CrossAccountS3Access role")
    except Exception as e:
        raise Exception(f"Failed to attach policy to CrossAccountS3Access role: {str(e)}") from e

def setup_central_account(central_account_id, region):
    """Set up resources in the central account"""
    try:
        if not is_valid_aws_account_id(central_account_id):
            raise ValueError(f"Invalid AWS account ID: {central_account_id}. Must be 12 digits.")

        # Create IAM client
        iam = boto3.client('iam')
        s3 = boto3.client('s3')
        
        # Create S3 bucket
        bucket_name = f"postgres-cw-metrics-central-{central_account_id}"
        create_s3_bucket(s3, bucket_name)

        # Create roles
        create_direct_access_role(iam, bucket_name, central_account_id, region)
        create_cross_account_role(iam, bucket_name, central_account_id)

        logger.info("Central account setup completed successfully")

    except Exception as e:
        raise Exception(f"Error setting up central account: {str(e)}") from e

def main():
    parser = argparse.ArgumentParser(description='Set up central account resources for PostgreSQL metrics collection')
    parser.add_argument('--account-id', required=True, help='Central AWS account ID')
    parser.add_argument('--region', required=True, help='AWS region where central resources are located')      
    args = parser.parse_args()

    setup_central_account(args.account_id, args.region)

if __name__ == "__main__":
    main()