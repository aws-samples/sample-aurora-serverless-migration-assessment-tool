# Setup Guide

This guide details the setup process for the Aurora Serverless Migration Assessment Tool.

## Prerequisites

- AWS accounts with appropriate permissions:
  - Central account for metrics storage and analysis
  - Source account(s) containing RDS PostgreSQL and Aurora PostgreSQL instances
- Python 3.x installed
- AWS CLI configured with appropriate credentials
- Required IAM permissions to create roles and policies

## Setup Process

### 1. Central Account Setup

The central account will store collected metrics and host analysis resources.

```bash
python3 setup_central_account.py --account-id <CENTRAL_ACCOUNT_ID> --region <CENTRAL_AWS_REGION>
```

This script:
- Creates necessary IAM roles and policies
- Sets up S3 bucket for metrics storage
- Establishes required trust relationships

```bash
python3 configure_ec2_instance_profile.py --instance-id <EC2_INSTANCE_ID> --account-id <CENTRAL_ACCOUNT_ID> --region <CENTRAL_AWS_REGION>
```
This script:
- Adds policy to the EC2 instance profile to allow assuming the PostgresMetricsUploader role
- Updates the PostgresMetricsUploader role's trust policy to allow it to be assumed by the EC2 instance's role

### 2. Source Account Setup

Run this setup in each source account containing RDS PostgreSQL and Aurora PostgreSQL instances.

```bash
export AWS_REGION=<SOURCE_EC2_AWS_REGION>
python3 setup_source_account.py --central-account-id <CENTRAL_ACCOUNT_ID>
```

This script:
- Creates IAM roles for metrics collection
- Configures necessary permissions for CloudWatch metrics access
- Establishes trust relationship with the central account

```bash
python3 configure_ec2_instance_profile.py --instance-id <EC2_INSTANCE_ID> --account-id <SOURCE_ACCOUNT_ID> --region <SOURCE_EC2_AWS_REGION>
```
This script:
- Adds policy to the EC2 instance profile to allow assuming the PostgresMetricsUploader role
- Updates the PostgresMetricsUploader role's trust policy to allow it to be assumed by the EC2 instance's role

### 3. Trust Policy Update

When adding new source accounts, update the central account's trust policy:

```bash
python3 update_central_trust_policy.py --source-account-ids <SPACE_SEPARATED_ACCOUNT_IDS>
```

## Verification Steps
1. Verify IAM Roles:
- Check central account IAM roles
- Verify source account IAM roles
- Confirm trust relationships
2. Test S3 Access:
- Ensure central S3 bucket is accessible
- Verify cross-account access permissions

## Troubleshooting

**Common Issues and Solutions**

1. Permission Errors

   Error: AccessDenied

   Solution: Verify IAM roles and policies

2. Trust Relationship Issues

   Error: AssumeRole failed

   Solution: Check trust policy configuration

3. S3 Access Problems

   Error: Unable to write to S3

   Solution: Verify bucket permissions and policies

## Security Considerations
- Use least privilege access principles
- Implement encryption for data at rest
- Configure appropriate VPC endpoints if needed

## Next Steps

After completing the setup:

- Proceed to [Metrics Collection Guide](metrics-collection-guide.md)
- Plan your collection strategy