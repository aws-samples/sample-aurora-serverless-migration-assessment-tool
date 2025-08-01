# Metrics Collection Guide

This guide explains how to collect metrics using the Aurora Serverless Migration Assessment Tool.

## Collection Overview

The tool collects the following metrics:
- CPU Utilization (Average, P95, Maximum)
- Instance specifications
- Pricing information
- Deployment configurations

## Metric Collection Scenarios

These scenarios show how the metric collection tool gathers performance data from PostgreSQL databases across different AWS environments. The examples show how to collect metrics from single or multiple clusters, customize the collection period, and work across different AWS accounts and regions. This flexibility enables organizations to perform comprehensive assessments of their PostgreSQL databases regardless of their deployment architecture, supporting informed decision-making for Aurora Serverless v2 migration planning.

The scenarios cover:

- Single account/region collection (all clusters or specific cluster)
- Custom time period collection
- Cross-region collection
- Cross-account collection
- Combined cross-account and cross-region collection

Each scenario uses the same base script (_**get_rds_aurora_postgres_metrics.py**_) with different parameters to accommodate various collection requirements, making it a versatile tool for database administrators and architects evaluating Aurora Serverless v2 migration.

### 1. Collect metrics for all PostgreSQL clusters within a single AWS account and region

```bash
export AWS_REGION=<CENTRAL_REGION>
python3 get_rds_aurora_postgres_metrics.py \
  --cluster-identifier all \
  --central-account-id <CENTRAL_ACCOUNT_ID>
```

### 2. Collect metrics for a single PostgreSQL cluster

```bash
export AWS_REGION=<CENTRAL_REGION>
python3 get_rds_aurora_postgres_metrics.py \
  --cluster-identifier <CLUSTER_ID> \
  --central-account-id <CENTRAL_ACCOUNT_ID>
```

### 3. Collect metrics with custom sample period (e.g., 60 days)

```bash
export AWS_REGION=<CENTRAL_REGION>
python3 get_rds_aurora_postgres_metrics.py \
  --cluster-identifier all \
  --central-account-id <CENTRAL_ACCOUNT_ID> \
  --sample-period-days 60
```

### 4. Collect metrics for cross-region

```bash
export AWS_REGION=<OTHER_REGION>
python3 get_rds_aurora_postgres_metrics.py \
  --cluster-identifier all \
  --central-account-id <CENTRAL_ACCOUNT_ID>
```

### 5. Collect metrics for cross-account
> ⚠️ **Prerequisite:** Login to another AWS account and complete the source account setup

```bash
export AWS_REGION=<CENTRAL_REGION>
python3 get_rds_aurora_postgres_metrics.py \
  --cluster-identifier all \
  --central-account-id <CENTRAL_ACCOUNT_ID>
```

### 56. Collect metrics for cross-account and cross-region
> ⚠️ **Prerequisite:** Login to another AWS account and complete the source account setup

```bash
export AWS_REGION=<OTHER_REGION>
python3 get_rds_aurora_postgres_metrics.py \
  --cluster-identifier all \
  --central-account-id <CENTRAL_ACCOUNT_ID>
```

## Metrics Details

### CloudWatch Metrics
- Average CPU Utilization
- P95 CPU Utilization
- Max CPU Utilization

### Instance Information
- Instance specifications
- Current pricing
- Deployment configuration

## Best Practices

1. Collection Period
- Minimum: 30 days
- Recommended: 60-90 days
- Consider seasonal patterns

2. Timing Considerations
- Include peak usage periods
- Collect during representative timeframes
- Account for business cycles

3. Performance Impact
- Minimal impact on source databases
- CloudWatch API throttling considerations
- Parallel collection strategies

## Data Validation
### Verification Steps
- Check S3 for raw metrics
- Verify CloudWatch metrics completeness
- Validate instance information

### Common Data Issues

1. Missing Metrics
Cause: CloudWatch retention
Solution: Adjust collection period

## Troubleshooting

**Common Issues and Solutions**

1. Missing Permissions

   Error: AccessDenied

   Solution: Verify IAM roles

## Next Steps

After completing the setup:

- Proceed to [Metrics Analysis Guide](metrics-analysis-guide.md)
- Plan your analysis strategy