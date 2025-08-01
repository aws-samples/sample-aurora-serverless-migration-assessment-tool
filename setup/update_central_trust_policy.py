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

def get_existing_trust_policy(iam):
    """Get existing trust policy and extract account IDs"""
    try:
        role = iam.get_role(RoleName='CrossAccountS3Access')
        trust_policy = role['Role']['AssumeRolePolicyDocument']
        
        # Extract existing account IDs from ARNs
        existing_accounts = set()
        for statement in trust_policy['Statement']:
            if isinstance(statement['Principal']['AWS'], list):
                for arn in statement['Principal']['AWS']:
                    account_id = arn.split(':')[4]  # Extract account ID from ARN
                    existing_accounts.add(account_id)
            else:
                # Handle single ARN case
                arn = statement['Principal']['AWS']
                account_id = arn.split(':')[4]
                existing_accounts.add(account_id)
                
        return trust_policy, existing_accounts
    except Exception as e:
        raise Exception(f"Failed to get existing trust policy: {str(e)}") from e

def update_trust_policy(source_account_ids):
    """Update trust policy to include new account IDs"""
    try:
        # Validate account IDs
        for account_id in source_account_ids:
            if not is_valid_aws_account_id(account_id):
                raise ValueError(f"Invalid AWS account ID: {account_id}. Must be 12 digits.")

        # Create IAM client
        iam = boto3.client('iam')

        # Get existing trust policy and account IDs
        current_policy, existing_accounts = get_existing_trust_policy(iam)

        # Identify new accounts to add
        new_accounts = set(source_account_ids) - existing_accounts
        
        if not new_accounts:
            logger.info(f"No new accounts to add. Trust policy already includes accounts: {', '.join(source_account_ids)}")
            return

        # Combine existing and new accounts
        all_accounts = existing_accounts | new_accounts

        # Create updated trust policy
        updated_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            f"arn:aws:iam::{account_id}:role/PostgresMetricsUploader"
                            for account_id in all_accounts
                        ]
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        # Update the trust policy
        iam.update_assume_role_policy(
            RoleName='CrossAccountS3Access',
            PolicyDocument=json.dumps(updated_policy)
        )

        logger.info(f"Successfully added new accounts to trust policy: {', '.join(new_accounts)}")
        logger.info(f"Trust policy now includes all accounts: {', '.join(all_accounts)}")

    except Exception as e:
        raise

def main():
    parser = argparse.ArgumentParser(description='Update trust policy for cross-account access')
    parser.add_argument('--source-account-ids', required=True, nargs='+', 
                      help='List of AWS account IDs to add to trust policy')
    args = parser.parse_args()

    update_trust_policy(args.source_account_ids)

if __name__ == "__main__":
    main()