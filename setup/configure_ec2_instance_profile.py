import boto3
import json
import argparse
from botocore.exceptions import ClientError
import time
import sys

def get_instance_profile(ec2_client, instance_id):
    """Check if instance profile exists for the EC2 instance."""
    try:
        response = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance = response['Reservations'][0]['Instances'][0]
        return instance.get('IamInstanceProfile')
    except ClientError as e:
        print(f"Error getting instance details: {e}")
        raise

def create_assume_role_policy(account_id):
    """Create policy document for assuming PostgresMetricsUploader role."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": f"arn:aws:iam::{account_id}:role/PostgresMetricsUploader"
            }
        ]
    }

def create_ec2_trust_policy():
    """Create trust policy document for EC2."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

def update_metrics_uploader_trust_policy(iam_client, role_arn, account_id):
    """
    Update PostgresMetricsUploader trust policy to allow assumption from the specified role
    while preserving existing trust relationships.
    """
    try:
        # Get current trust policy
        response = iam_client.get_role(RoleName='PostgresMetricsUploader')
        current_policy = response['Role']['AssumeRolePolicyDocument']

        # Initialize lists to store service principals and AWS principals
        service_principals = []
        aws_principals = []

        # Extract existing principals
        for statement in current_policy.get('Statement', []):
            if statement.get('Effect') == 'Allow' and statement.get('Action') == 'sts:AssumeRole':
                principal = statement.get('Principal', {})
                
                # Collect service principals
                if 'Service' in principal:
                    if isinstance(principal['Service'], str):
                        service_principals.append(principal['Service'])
                    elif isinstance(principal['Service'], list):
                        service_principals.extend(principal['Service'])

                # Collect AWS principals
                if 'AWS' in principal:
                    if isinstance(principal['AWS'], str):
                        aws_principals.append(principal['AWS'])
                    elif isinstance(principal['AWS'], list):
                        aws_principals.extend(principal['AWS'])

        # Add ec2.amazonaws.com if not present
        if 'ec2.amazonaws.com' not in service_principals:
            service_principals.append('ec2.amazonaws.com')

        # Add new role ARN if not present
        if role_arn not in aws_principals:
            aws_principals.append(role_arn)

        # Create new policy document
        new_policy = {
            "Version": "2012-10-17",
            "Statement": []
        }

        # Add service principals statement if any exist
        if service_principals:
            new_policy['Statement'].append({
                "Effect": "Allow",
                "Principal": {
                    "Service": service_principals[0] if len(service_principals) == 1 else service_principals
                },
                "Action": "sts:AssumeRole"
            })

        # Add AWS principals statement if any exist
        if aws_principals:
            new_policy['Statement'].append({
                "Effect": "Allow",
                "Principal": {
                    "AWS": aws_principals[0] if len(aws_principals) == 1 else aws_principals
                },
                "Action": "sts:AssumeRole"
            })

        # Update the trust policy
        iam_client.update_assume_role_policy(
            RoleName='PostgresMetricsUploader',
            PolicyDocument=json.dumps(new_policy)
        )
        print(f"Successfully updated PostgresMetricsUploader trust policy")
        
        # Verify the update
        verification = iam_client.get_role(RoleName='PostgresMetricsUploader')
        print(f"Verified trust policy:\n{json.dumps(verification['Role']['AssumeRolePolicyDocument'], indent=2)}")
        
        return True

    except ClientError as e:
        print(f"Warning: Error updating PostgresMetricsUploader trust policy: {e}")
        raise

def handle_existing_profile(iam_client, instance_profile_arn, account_id):
    """Handle case where instance profile exists."""
    print("Instance profile exists. Adding policy to existing role...")
    
    try:
        # Get instance profile name from ARN
        profile_name = instance_profile_arn.split('/')[-1]
        
        # Get role name from instance profile
        response = iam_client.get_instance_profile(InstanceProfileName=profile_name)
        role_name = response['InstanceProfile']['Roles'][0]['RoleName']
        role_arn = response['InstanceProfile']['Roles'][0]['Arn']
        print(f"Found role: {role_name}")

        # Create and attach policy
        policy_document = create_assume_role_policy(account_id)
        try:
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='AssumeMetricsUploaderPolicy',
                PolicyDocument=json.dumps(policy_document)
            )
            print(f"Successfully attached AssumeMetricsUploaderPolicy to role {role_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print("Policy already exists. Updating policy...")
                iam_client.delete_role_policy(
                    RoleName=role_name,
                    PolicyName='AssumeMetricsUploaderPolicy'
                )
                iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName='AssumeMetricsUploaderPolicy',
                    PolicyDocument=json.dumps(policy_document)
                )
                print("Policy updated successfully")
            else:
                raise

        # Update PostgresMetricsUploader trust policy
        update_metrics_uploader_trust_policy(iam_client, role_arn, account_id)

    except ClientError as e:
        print(f"Error handling existing profile: {e}")
        raise

def wait_for_iam_role(iam_client, role_name, max_attempts=20):
    start_time = time.time()
    last_check_time = start_time
    timeout = 120  # 2 minutes timeout
    poll_interval = 20  # Check every 20 seconds

    print("Waiting for IAM changes to propagate...")
    while time.time() - start_time < timeout:
        current_time = time.time()
        
        # Ensure we wait at least poll_interval seconds between checks
        if current_time - last_check_time < poll_interval:
            continue
            
        last_check_time = current_time
        elapsed_time = int(current_time - start_time)
        
        try:
            # Get the role
            role_response = iam_client.get_role(RoleName=role_name)
            role_arn = role_response['Role']['Arn']
            
            # Test if we can get the role policy
            try:
                iam_client.get_role_policy(
                    RoleName=role_name,
                    PolicyName='AssumeMetricsUploaderPolicy'
                )
            except iam_client.exceptions.NoSuchEntityException:
                print(f"Waiting for role policies to propagate (elapsed time: {elapsed_time}s)")
                continue

            # Test if we can get the role by ARN
            try:
                # Try to get the role using get_role to verify ARN is valid
                sts_client = boto3.client('sts')
                sts_client.get_caller_identity()
                print(f"Role {role_name} and its policies are now fully available (elapsed time: {elapsed_time}s)")
                print(f"Verified role ARN: {role_arn}")
                return True, role_arn
            except ClientError as e:
                print(f"Waiting for role ARN to propagate (elapsed time: {elapsed_time}s)")
                continue

        except iam_client.exceptions.NoSuchEntityException:
            print(f"Waiting for role to be available (elapsed time: {elapsed_time}s)")
        except Exception as e:
            print(f"Unexpected error while waiting for role (elapsed time: {elapsed_time}s): {e}")

    total_time = int(time.time() - start_time)
    print(f"Timed out after {total_time} seconds waiting for role {role_name}")
    return False, None      

def wait_for_instance_profile(iam_client, profile_name):
    """Wait for instance profile to be available"""
    start_time = time.time()
    last_check_time = start_time
    timeout = 120  # 2 minutes timeout
    poll_interval = 20  # Check every 20 seconds

    print("Waiting for IAM changes to propagate...")
    while time.time() - start_time < timeout:
        current_time = time.time()
        
        # Ensure we wait at least poll_interval seconds between checks
        if current_time - last_check_time < poll_interval:
            continue
            
        last_check_time = current_time
        elapsed_time = int(current_time - start_time)
        
        try:
            iam_client.get_instance_profile(InstanceProfileName=profile_name)
            print(f"Instance profile {profile_name} is now available (elapsed time: {elapsed_time}s)")
            return True
        except iam_client.exceptions.NoSuchEntityException:
            print(f"Waiting for instance profile to be available (elapsed time: {elapsed_time}s)")

    total_time = int(time.time() - start_time)
    print(f"Timed out after {total_time} seconds waiting for instance profile {profile_name}")
    return False

def wait_for_role_association(iam_client, profile_name, role_name):
    """Wait for role to be associated with instance profile"""
    start_time = time.time()
    last_check_time = start_time
    timeout = 120  # 2 minutes timeout
    poll_interval = 20  # Check every 20 seconds

    print("Waiting for IAM changes to propagate...")
    while time.time() - start_time < timeout:
        current_time = time.time()
        
        # Ensure we wait at least poll_interval seconds between checks        
        if current_time - last_check_time < poll_interval:            
            continue
            
        last_check_time = current_time
        elapsed_time = int(current_time - start_time)
        
        try:
            response = iam_client.get_instance_profile(InstanceProfileName=profile_name)
            if any(role['RoleName'] == role_name for role in response['InstanceProfile']['Roles']):
                print(f"Role {role_name} is now associated with profile (elapsed time: {elapsed_time}s)")
                return True
            print(f"Waiting for role association (elapsed time: {elapsed_time}s)")
        except iam_client.exceptions.NoSuchEntityException:
            print(f"Waiting for profile and role association (elapsed time: {elapsed_time}s)")

    total_time = int(time.time() - start_time)
    print(f"Timed out after {total_time} seconds waiting for role association")
    return False  

def check_role_exists(iam_client, role_name):
    """Check if IAM role exists and return its ARN if it does"""
    try:
        response = iam_client.get_role(RoleName=role_name)
        return True, response['Role']['Arn']
    except iam_client.exceptions.NoSuchEntityException:
        return False, None
    except Exception as e:
        print(f"Error checking role existence: {e}")
        return False, None     

def check_instance_profile_exists(iam_client, profile_name, role_name):
    """ Check if instance profile exists and if it has the correct role attached"""
    try:
        response = iam_client.get_instance_profile(InstanceProfileName=profile_name)
        
        # Check if the role is already attached
        has_role = any(role['RoleName'] == role_name 
                      for role in response['InstanceProfile']['Roles'])
        
        return True, has_role, None
    except iam_client.exceptions.NoSuchEntityException:
        return False, False, None
    except Exception as e:
        return False, False, str(e)             

def create_new_profile(iam_client, ec2_client, instance_id, account_id):
    """Create new instance profile and attach to EC2 instance."""
    cleanup_required = False
    role_name = 'EC2MetricsCollectorRole'
    profile_name = 'EC2MetricsCollectorProfile'
    
    try:
        print("No instance profile found. Creating new profile...")

        # Check if role already exists
        role_exists, existing_role_arn = check_role_exists(iam_client, role_name)
        
        if role_exists:
            print(f"Found existing role {role_name}")
            role_arn = existing_role_arn
            
            # Verify role has correct policy
            try:
                iam_client.get_role_policy(
                    RoleName=role_name,
                    PolicyName='AssumeMetricsUploaderPolicy'
                )
                print("Found existing AssumeMetricsUploaderPolicy")
            except iam_client.exceptions.NoSuchEntityException:
                print("Attaching AssumeMetricsUploaderPolicy to existing role")
                policy_document = create_assume_role_policy(account_id)
                iam_client.put_role_policy(
                    RoleName=role_name,
                    PolicyName='AssumeMetricsUploaderPolicy',
                    PolicyDocument=json.dumps(policy_document)
                )
                print("Created and attached AssumeMetricsUploaderPolicy")
        else:        
        
            # Create IAM role
            create_role_response = iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(create_ec2_trust_policy())
            )
            role_arn = create_role_response['Role']['Arn']
            cleanup_required = True
            print(f"Created {role_name}")

            # Create and attach policy
            policy_document = create_assume_role_policy(account_id)
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='AssumeMetricsUploaderPolicy',
                PolicyDocument=json.dumps(policy_document)
            )
            print("Created and attached AssumeMetricsUploaderPolicy")

        # Wait for role to be available
        success, verified_role_arn = wait_for_iam_role(iam_client, role_name)
        if not success:
            raise Exception(f"Timed out waiting for role {role_name} to be available")
        
        role_arn = verified_role_arn 
        print(f"Using verified role ARN: {role_arn}")     

        # Update PostgresMetricsUploader trust policy
        if not update_metrics_uploader_trust_policy(iam_client, role_arn, account_id):
            raise Exception("Failed to update PostgresMetricsUploader trust policy")

        # Check if instance profile exists and has the correct role
        profile_exists, has_role, error = check_instance_profile_exists(iam_client, profile_name, role_name)
        
        if error:
            raise Exception(f"Error checking instance profile: {error}")

        if profile_exists:
            print(f"Found existing instance profile {profile_name}")
            if has_role:
                print(f"Instance profile already has role {role_name} attached")
            else:
                print(f"Attaching role {role_name} to existing instance profile")
                try:
                    iam_client.add_role_to_instance_profile(
                        InstanceProfileName=profile_name,
                        RoleName=role_name
                    )
                    print(f"Added {role_name} to {profile_name}")

                    # Wait for role association to be ready
                    if not wait_for_role_association(iam_client, profile_name, role_name):
                        raise Exception("Timed out waiting for role association")
                except iam_client.exceptions.LimitExceededException:
                    print("Instance profile already has maximum number of roles. Checking existing roles...")
                    response = iam_client.get_instance_profile(InstanceProfileName=profile_name)
                    current_roles = [role['RoleName'] for role in response['InstanceProfile']['Roles']]
                    print(f"Current roles in profile: {current_roles}")
                    raise Exception("Cannot attach role: instance profile has reached maximum role limit")
        else:
            # Create instance profile
            iam_client.create_instance_profile(
                InstanceProfileName=profile_name
            )
            print(f"Created {profile_name}")

            # Wait for instance profile to be ready
            if not wait_for_instance_profile(iam_client, profile_name):
                raise Exception("Timed out waiting for instance profile creation")

            # Add role to instance profile
            iam_client.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name
            )
            print(f"Added {role_name} to {profile_name}")

            # Wait for role association to be ready
            if not wait_for_role_association(iam_client, profile_name, role_name):
                raise Exception("Timed out waiting for role association")

        # Attach profile to EC2 instance
        ec2_client.associate_iam_instance_profile(
            IamInstanceProfile={'Name': profile_name},
            InstanceId=instance_id
        )
        print(f"Attached profile to instance {instance_id}")
        return True

    except Exception as e:
        print(f"Error creating new profile: {e}")
        if cleanup_required:
            try:
                print("Cleaning up partially created resources...")
                try:
                    iam_client.remove_role_from_instance_profile(
                        InstanceProfileName=profile_name,
                        RoleName=role_name
                    )
                except ClientError:
                    pass
                
                try:
                    iam_client.delete_instance_profile(
                        InstanceProfileName=profile_name
                    )
                except ClientError:
                    pass
                
                try:
                    iam_client.delete_role_policy(
                        RoleName=role_name,
                        PolicyName='AssumeMetricsUploaderPolicy'
                    )
                except ClientError:
                    pass
                
                try:
                    iam_client.delete_role(
                        RoleName=role_name
                    )
                except ClientError:
                    pass
                
                print("Cleanup completed")
            except Exception as cleanup_error:
                print(f"Error during cleanup: {cleanup_error}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Configure EC2 instance profile for PostgresMetricsUploader access')
    parser.add_argument('--instance-id', required=True, help='EC2 instance ID')
    parser.add_argument('--account-id', required=True, help='AWS account ID')
    parser.add_argument('--region', required=True, help='AWS region')
    args = parser.parse_args()

    # Create AWS clients with specified region
    ec2_client = boto3.client('ec2', region_name=args.region)
    iam_client = boto3.client('iam', region_name=args.region)

    try:
        # Check for existing instance profile
        instance_profile = get_instance_profile(ec2_client, args.instance_id)

        if instance_profile:
            handle_existing_profile(iam_client, instance_profile['Arn'], args.account_id)
        else:
            create_new_profile(iam_client, ec2_client, args.instance_id, args.account_id)

        print("\nConfiguration completed successfully!")
        print("Note: You may need to reboot the EC2 instance for changes to take effect.")

    except Exception as e:
        print(f"\nConfiguration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()