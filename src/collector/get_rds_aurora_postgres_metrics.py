import boto3
import pandas as pd
import requests
import json
from datetime import datetime, timedelta, timezone
import logging
import os
import sys
import argparse
import math
import io
import openpyxl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_valid_aws_account_id(account_id):
    """
    Validate if the provided string is a valid AWS account ID
    AWS account IDs are 12 digits long
    """
    if not account_id:
        return False
    return account_id.isdigit() and len(account_id) == 12

def init(central_account_id, analyze_usage_pattern):
    global AWS_ACCOUNT_ID, AWS_REGION, ACU_PRICE_STANDARD, ACU_PRICE_IO_OPTIMIZED, MAX_ASV2_ACU, PROVISIONED_SERVERLESS_RATIO, RUN_DATE
    global CENTRAL_S3_BUCKET, CENTRAL_S3_REGION, CENTRAL_ACCOUNT_ID, METRICS_CREDENTIALS, CACHED_PRICING_DATA
    global ON_DEMAND_PRICING, RI_PRICING

    try:
        # Validate central account ID
        if not is_valid_aws_account_id(central_account_id):
            raise ValueError(f"Invalid AWS account ID: {central_account_id}. Must be 12 digits.")

        # Set values
        AWS_REGION, AWS_ACCOUNT_ID = get_aws_details() 
        # Check if AWS_REGION is valid immediately
        if not AWS_REGION:
            raise ValueError("Unable to determine AWS region. Please set the AWS_REGION environment variable.")

        # Set central configuration
        CENTRAL_S3_REGION = 'us-east-1'  # Fixed central region
        CENTRAL_ACCOUNT_ID = central_account_id
        CENTRAL_S3_BUCKET = f"postgres-cw-metrics-central-{CENTRAL_ACCOUNT_ID}"   

        # Set instance pricing
        ON_DEMAND_PRICING ='OnDemand'
        RI_PRICING = 'RI'         

        # Initialize pricing data cache
        CACHED_PRICING_DATA = None                

        # Get credentials by assuming PostgresMetricsUploader role
        METRICS_CREDENTIALS = get_credentials_for_metrics()
        logger.info("Successfully obtained PostgresMetricsUploader credentials")              

        # Get ACU pricing for both standard and I/O optimized
        ACU_PRICE_STANDARD = get_acu_pricing(storage_type='Standard')
        ACU_PRICE_IO_OPTIMIZED = get_acu_pricing(storage_type='I/O Optimized')   

        # Add new constants
        MAX_ASV2_ACU = 256  # Maximum Aurora Serverless v2 capacity in ACUs
        
        # Provisioned to Serverless ratio dictionary
        PROVISIONED_SERVERLESS_RATIO = {
            'memory': 4,    # memory-optimized (r-series)
            'compute': 1,   # compute-optimized (c-series)
            'general': 2    # general-purpose and burstable (m-series, t-series)
        }

        # Get current date in local timezone for run_date_local
        RUN_DATE = datetime.now().strftime('%Y-%m-%d')             
        
        logging.info("Global variables initialized successfully")
        
    except Exception as e:
        logging.error(f"Error initializing global variables: {str(e)}")
        raise

def get_credentials_for_metrics():
    """Get credentials by assuming PostgresMetricsUploader role"""
    try:
        sts_client = boto3.client('sts', region_name=AWS_REGION)
        
        # Assume PostgresMetricsUploader role
        assumed_role = sts_client.assume_role(
            RoleArn=f'arn:aws:iam::{AWS_ACCOUNT_ID}:role/PostgresMetricsUploader',
            RoleSessionName='MetricsCollection'
        )
        
        logger.info(f"Successfully assumed PostgresMetricsUploader role in account {AWS_ACCOUNT_ID}")
        return assumed_role['Credentials']
    except Exception as e:
        raise Exception(f"Failed to assume PostgresMetricsUploader role: {str(e)}") from e       

def get_aws_details():
    try:   
        # Get region from environment variable or boto3
        # Try environment variables first for region
        region = os.environ.get('AWS_REGION')
        if not region or region == 'aws-global':
            region = os.environ.get('AWS_DEFAULT_REGION')
        
        # If no environment variables, try boto3 session
        if not region or region == 'aws-global':
            session = boto3.session.Session()
            region = session.region_name

        # If no environment variables, try boto3 session
        if not region or region == 'aws-global':
            region = sts_client.meta.region_name            
            
        if not region or region == 'aws-global':
            raise ValueError("Unable to determine AWS region. Assign AWS region to the AWS_REGION env variable")   

        # Get account ID
        sts_client = boto3.client('sts', region_name=region)
        account_id = sts_client.get_caller_identity()['Account']                 
        
        return region, account_id
    except Exception as e:
        print(f"Error getting AWS details: {e}")
        return None, None

def get_boto3_client(service_name, region_name=None):
    """Get boto3 client with assumed role credentials"""
    return boto3.client(
        service_name,
        region_name=region_name or AWS_REGION,
        aws_access_key_id=METRICS_CREDENTIALS['AccessKeyId'],
        aws_secret_access_key=METRICS_CREDENTIALS['SecretAccessKey'],
        aws_session_token=METRICS_CREDENTIALS['SessionToken']
    )        

def upload_cw_metrics_file_to_s3(file_path, cluster_identifier, source_region):
    """
    Upload file to centralized S3 bucket
    
    Args:
        file_path: Local path to the file to upload
        cluster_identifier: Identifier of the cluster
        source_region: Source AWS region where the data was collected
    """
    try:
        # Determine if we need cross-account access
        needs_cross_account = AWS_ACCOUNT_ID != CENTRAL_ACCOUNT_ID
        
        if needs_cross_account:
            # Assume role in central account
            logger.info(f"Cross-account upload required. Current account: {AWS_ACCOUNT_ID}, Central account: {CENTRAL_ACCOUNT_ID}")
            sts_client = get_boto3_client('sts', region_name=AWS_REGION)
            assumed_role = sts_client.assume_role(
                RoleArn=f'arn:aws:iam::{CENTRAL_ACCOUNT_ID}:role/CrossAccountS3Access',
                RoleSessionName='S3UploadSession'
            )
            
            # Create S3 client with assumed role credentials
            s3_client = boto3.client(
                's3',
                region_name=CENTRAL_S3_REGION,
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken']
            )
            logger.info(f"Successfully assumed role in central account {CENTRAL_ACCOUNT_ID}")
        else:
            # Use regular S3 client in central region
            logger.info(f"Using direct S3 access in account {AWS_ACCOUNT_ID}")
            s3_client = get_boto3_client('s3', region_name=CENTRAL_S3_REGION)
        
        # Get filename and extension
        filename = os.path.basename(file_path)
        _, file_extension = os.path.splitext(filename)        
        
        # Determine file type and set appropriate prefix
        if file_extension.lower() == '.csv':
            if cluster_identifier == 'all':
                prefix = f'cloudwatch_detail_metrics/raw/{source_region}/{AWS_ACCOUNT_ID}/all_clusters/'
            else:
                prefix = f'cloudwatch_detail_metrics/raw/{source_region}/{AWS_ACCOUNT_ID}/single_cluster/'
            
            content_type = 'text/csv'
        else:
            raise ValueError(f"Unsupported file type: {file_extension}. Only .csv files are supported.")
        
        # Create S3 key with timestamp and region info
        s3_key = f"{prefix}{filename}"
        
        # Upload file to central bucket
        s3_client.upload_file(
            Filename=file_path,
            Bucket=CENTRAL_S3_BUCKET,
            Key=s3_key,
            ExtraArgs={
                'ContentType': content_type,
                'ServerSideEncryption': 'AES256'
            }
        )
        
        logger.info(f"Successfully uploaded file to central S3: s3://{CENTRAL_S3_BUCKET}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        return False    

def get_pricing_url():
    """
    Dynamically generate the pricing API URL based on the AWS region code
    """
    # Define the base URL format with the API path
    base_url = "https://pricing.{}.amazonaws.com/api/v1/products"
    
    # All regions now use us-east-1 for pricing
    pricing_region = "us-east-1"
    
    # Generate URL
    pricing_url = base_url.format(pricing_region)
    
    # Log the pricing URL generation details
    logger.info(f"Current region: {AWS_REGION}")
    logger.info(f"Mapped pricing region: {pricing_region}")
    logger.info(f"Generated pricing URL: {pricing_url}")
    
    return pricing_url

def get_region_name():
    """
    Map region code to region name used in pricing API
    """
    region_mapping = {
        'us-east-1': 'US East (N. Virginia)',
        'us-east-2': 'US East (Ohio)',
        'us-west-1': 'US West (N. California)',
        'us-west-2': 'US West (Oregon)',
        'af-south-1': 'Africa (Cape Town)',
        'ap-east-1': 'Asia Pacific (Hong Kong)',
        'ap-south-1': 'Asia Pacific (Mumbai)',
        'ap-northeast-1': 'Asia Pacific (Tokyo)',
        'ap-northeast-2': 'Asia Pacific (Seoul)',
        'ap-northeast-3': 'Asia Pacific (Osaka)',
        'ap-southeast-1': 'Asia Pacific (Singapore)',
        'ap-southeast-2': 'Asia Pacific (Sydney)',
        'ap-southeast-3': 'Asia Pacific (Jakarta)',
        'ca-central-1': 'Canada (Central)',
        'eu-central-1': 'EU (Frankfurt)',
        'eu-west-1': 'EU (Ireland)',
        'eu-west-2': 'EU (London)',
        'eu-west-3': 'EU (Paris)',
        'eu-north-1': 'EU (Stockholm)',
        'eu-south-1': 'EU (Milan)',
        'me-south-1': 'Middle East (Bahrain)',
        'sa-east-1': 'South America (Sao Paulo)'
    }
    return region_mapping.get(AWS_REGION, '')

def get_instance_specifications(instance_class):
    """
    Get vCPU and Memory specifications for Aurora PostgreSQL and RDS PostgreSQL instance classes
    from S3 stored JSON file
    
    Args:
        instance_class (str): The DB instance class (e.g., 'db.r6g.xlarge')
        
    Returns:
        dict: Dictionary containing vcpu and memory specifications
    """
    try:
        # Create STS client with PostgresMetricsUploader credentials
        sts_client = boto3.client('sts',
            aws_access_key_id=METRICS_CREDENTIALS['AccessKeyId'],
            aws_secret_access_key=METRICS_CREDENTIALS['SecretAccessKey'],
            aws_session_token=METRICS_CREDENTIALS['SessionToken'],
            region_name=AWS_REGION
        )
        
        # Assume CrossAccountS3Access role
        assumed_role = sts_client.assume_role(
            RoleArn=f'arn:aws:iam::{CENTRAL_ACCOUNT_ID}:role/CrossAccountS3Access',
            RoleSessionName='InstanceSpecAccess'
        )
        
        # Create S3 client with assumed CrossAccountS3Access role credentials
        s3_client = boto3.client(
            's3',
            region_name=CENTRAL_S3_REGION,
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken']
        )
        
        # Get JSON file from S3
        response = s3_client.get_object(
            Bucket=CENTRAL_S3_BUCKET,
            Key='reference/instance_specifications.json'
        )
        
        # Load the JSON content
        instance_specs = json.loads(response['Body'].read().decode('utf-8'))
        
        # Return the specifications for the requested instance class
        return instance_specs.get(instance_class, {'vcpu': 'Unknown', 'memory': 'Unknown'})
            
    except Exception as e:
        logger.error(f"Error reading instance specifications from S3: {str(e)}")
        return {'vcpu': 'Unknown', 'memory': 'Unknown'}

def load_pricing_data():
    """
    Load pricing data from Excel file in S3 and cache it
    """
    global CACHED_PRICING_DATA
    
    if CACHED_PRICING_DATA is not None:
        return CACHED_PRICING_DATA
        
    try:
        # Create STS client with PostgresMetricsUploader credentials
        sts_client = boto3.client('sts',
            aws_access_key_id=METRICS_CREDENTIALS['AccessKeyId'],
            aws_secret_access_key=METRICS_CREDENTIALS['SecretAccessKey'],
            aws_session_token=METRICS_CREDENTIALS['SessionToken'],
            region_name=AWS_REGION
        )
        
        # Assume CrossAccountS3Access role
        assumed_role = sts_client.assume_role(
            RoleArn=f'arn:aws:iam::{CENTRAL_ACCOUNT_ID}:role/CrossAccountS3Access',
            RoleSessionName='PricingDataAccess'
        )
        
        # Create S3 client with assumed CrossAccountS3Access role credentials
        s3_client = boto3.client(
            's3',
            region_name=CENTRAL_S3_REGION,
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken']
        )
        
        # Get Excel file from S3
        response = s3_client.get_object(
            Bucket=CENTRAL_S3_BUCKET,
            Key='reference/rds_aurora_pricing.xlsx'
        )
        
        # Read Excel file into dictionary of DataFrames
        excel_data = response['Body'].read()
        
        # Create dictionary to store DataFrames for each sheet
        pricing_data = {
            'Aurora': pd.read_excel(io.BytesIO(excel_data), sheet_name='Aurora'),
            'RDS-SingleAZ': pd.read_excel(io.BytesIO(excel_data), sheet_name='RDS-SingleAZ'),
            'RDS-MultiAZ': pd.read_excel(io.BytesIO(excel_data), sheet_name='RDS-MultiAZ'),
            'RDS-MultiAZreadable': pd.read_excel(io.BytesIO(excel_data), sheet_name='RDS-MultiAZreadable')
        }
        
        # Cache the pricing data
        CACHED_PRICING_DATA = pricing_data
        logger.info("Successfully loaded and cached pricing data from Excel file")
        
        return pricing_data
            
    except Exception as e:
        logger.error(f"Error loading pricing data from Excel: {str(e)}")
        return None    

def get_fallback_instance_pricing(platform_type, dbinstance_class, region, deployment_option=None, storage_type=None, instance_pricing='OnDemand'):
    """
    Lookup RDS/Aurora instance pricing from Excel file stored in S3 when API pricing info is unavailable
    
    Args:
        platform_type (str): 'Aurora' or 'RDS'
        dbinstance_class (str): The DB instance class (e.g. 'db.r5.large') 
        region (str): AWS region (e.g. 'us-east-1')
        deployment_option (str): For RDS only - 'Single-AZ', 'Multi-AZ' or 'Multi-AZ (readable standbys)'
        storage_type (str): For Aurora only - 'Standard' or 'I/O-Optimized'
        instance_pricing (str): ON_DEMAND_PRICING or RI_PRICING for pricing type
        
    Returns:
        float: The hourly price for the specified instance
    """
    try:
        # Load pricing data if not already cached
        pricing_data = load_pricing_data()
        if pricing_data is None:
            return None
        
        # Determine which sheet to use based on platform type and deployment option
        if platform_type == 'Aurora':
            sheet_name = 'Aurora'
        elif platform_type == 'RDS':
            if deployment_option == 'Single-AZ':
                sheet_name = 'RDS-SingleAZ'
            elif deployment_option == 'Multi-AZ':
                sheet_name = 'RDS-MultiAZ'
            elif deployment_option == 'Multi-AZ (readable standbys)':
                sheet_name = 'RDS-MultiAZreadable'
            else:
                return None
        else:
            return None
            
        # Get the appropriate DataFrame
        df = pricing_data[sheet_name]
        
        # Filter for matching instance class, region and pricing type
        df = df[
            (df['dbinstance_class'] == dbinstance_class) & 
            (df['aws_region'] == region) &
            (df['instance_pricing'] == instance_pricing)
        ]
        
        if df.empty:
            logger.warning(f"No pricing data found for {instance_pricing} pricing type for {dbinstance_class} in {region}")
            return None
        
        # Get price based on platform type and storage type
        if platform_type == 'Aurora':
            if storage_type == 'Standard':
                price = float(df.iloc[0]['standard_price'])  # Standard storage price
            else:  # I/O-Optimized
                price = float(df.iloc[0]['io_price'])  # I/O-Optimized storage price
        else:  # RDS
            price = float(df.iloc[0]['standard_price'])  # Standard price for RDS
            
        logger.debug(f"Retrieved {instance_pricing} price for {dbinstance_class}: ${price:.3f}/hour")
        return price
            
    except Exception as e:
        logger.error(f"Error retrieving cached pricing data: {str(e)}")
        return None 

def calculate_monthly_cost(hourly_rate):
    """
    Calculate monthly cost based on hourly rate (assuming 730 hours per month)
    """
    if isinstance(hourly_rate, (int, float)):
        return round(hourly_rate * 730, 2)
    return None

def get_acu_pricing(storage_type='Standard'):
    """
    Get Aurora Serverless v2 ACU pricing using AWS Price List API.
    This function is called once during initialization to set the global ACU prices.
    Args:
        storage_type: Either 'Standard' or 'I/O Optimized'
    """
    try:
        pricing_client = get_boto3_client('pricing', region_name='us-east-1')
        region_name = get_region_name()
        instance_class = 'db.serverless'
        deployment_option = 'Single-AZ'
        
        # Set usage type based on storage type
        usage_type = 'Aurora:ServerlessV2IOOptimizedUsage' if storage_type == 'I/O Optimized' else 'Aurora:ServerlessV2Usage'
        
        # Define filters for Aurora PostgreSQL ACU pricing
        filters = [
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
            {'Type': 'TERM_MATCH', 'Field': 'usagetype', 'Value': usage_type},
            {'Type': 'TERM_MATCH', 'Field': 'engineCode', 'Value': '21'}  # For PostgreSQL
        ]

        logger.debug(f"ACU Pricing API Filters for {storage_type}: {json.dumps(filters, indent=2)}")

        response = pricing_client.get_products(
            ServiceCode='AmazonRDS',
            Filters=filters
        )
        
        logger.debug(f"ACU Pricing API Response for {storage_type}: {json.dumps(response, indent=2)}")
            
        for price_item in response.get('PriceList', []):
            price_data = json.loads(price_item)
            
            # Check attributes first
            attributes = price_data.get('product', {}).get('attributes', {})
            if (attributes.get('usagetype') == usage_type and 
                attributes.get('operation') == 'CreateDBInstance:0021'):
                
                # Get pricing information
                terms = price_data.get('terms', {}).get('OnDemand', {})
                for term_info in terms.values():
                    price_dimensions = term_info.get('priceDimensions', {})
                    for dimension_info in price_dimensions.values():
                        unit = dimension_info.get('unit', '')
                        if unit == 'ACU-Hr':
                            description = dimension_info.get('description', '')
                            if 'Aurora PostgreSQL Serverless v2' in description:
                                price = float(dimension_info.get('pricePerUnit', {}).get('USD', 0))
                                logger.info(f"ACU price for {storage_type} storage type in {AWS_REGION}: ${price}/ACU-Hr")
                                return price
        
        logger.warning(f"No ACU pricing found for {storage_type} storage type in {AWS_REGION}")

        # Try fallback pricing if API pricing not found
        fallback_price = get_fallback_instance_pricing(
            'Aurora',
            instance_class,
            AWS_REGION,
            deployment_option,
            storage_type,
            ON_DEMAND_PRICING
        )
        if fallback_price:
            logger.info(f"ACU fallback price for {storage_type} storage type in {AWS_REGION}: ${fallback_price}/ACU-Hr")
            return fallback_price

    except Exception as e:
        logger.error(f"Error getting ACU pricing for {storage_type} storage type in {AWS_REGION}: {str(e)}")
        logger.info(f"Trying fallback pricing after API error for {instance_class} in {AWS_REGION}")
        fallback_price = get_fallback_instance_pricing(
            'Aurora',
            instance_class,
            AWS_REGION,
            deployment_option,
            storage_type,
            ON_DEMAND_PRICING
        )
        if fallback_price:
            logger.info(f"ACU fallback price for {storage_type} storage type in {AWS_REGION}: ${fallback_price}/ACU-Hr")
            return fallback_price

        return 0.0

def get_instance_pricing(instance_class, engine_type='Aurora PostgreSQL', storage_type='Standard', deployment_option='Single-AZ'):
    """
    Get on-demand pricing for RDS instance using AWS Price List API
    
    Args:
        instance_class: The instance class (e.g., db.r6g.xlarge)
        engine_type: Either 'Aurora PostgreSQL' or 'PostgreSQL'
        storage_type: Either 'Standard' or 'I/O Optimized'
        deployment_option: 'Single-AZ', 'Multi-AZ', or 'Multi-AZ (readable standbys)'
    """
    try:
        pricing_client = get_boto3_client('pricing', region_name='us-east-1')
        region_name = get_region_name()
        
        # Normalize engine type for pricing API
        database_engine = 'Aurora PostgreSQL' if 'aurora' in engine_type.lower() else 'PostgreSQL'
        
        # Determine usage type prefix based on engine type and deployment option
        if 'aurora' in engine_type.lower():
            # For Aurora PostgreSQL
            usage_type_prefix = 'InstanceUsageIOOptimized:' if storage_type == 'I/O Optimized' else 'InstanceUsage:'
        else:
            # For RDS PostgreSQL
            if deployment_option == 'Multi-AZ (readable standbys)':
                usage_type_prefix = 'Multi-AZClusterUsage'
            elif deployment_option == 'Multi-AZ':
                usage_type_prefix = 'Multi-AZUsage'
            else:  # Single-AZ
                usage_type_prefix = 'InstanceUsage'
        
        filters = [
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_class},
            {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': database_engine},
            {'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': 'OnDemand'}
        ]

        # Add deployment option filter
        if database_engine == 'PostgreSQL':
            if deployment_option == 'Multi-AZ (readable standbys)':
                filters.append({'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Multi-AZ (readable standbys)'})
            elif deployment_option == 'Multi-AZ':
                filters.append({'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Multi-AZ'})
            else:
                filters.append({'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'})

        logger.debug(f"Instance Pricing API Filters: {json.dumps(filters, indent=2)}")
        logger.debug(f"Looking for usage type prefix: {usage_type_prefix}")

        response = pricing_client.get_products(
            ServiceCode='AmazonRDS',
            Filters=filters
        )
        
        logger.debug(f"Instance Pricing API Response: {json.dumps(response, indent=2)}")
            
        for price_item in response.get('PriceList', []):
            price_data = json.loads(price_item)
            attributes = price_data.get('product', {}).get('attributes', {})
            
            usage_type = attributes.get('usagetype', '')
            logger.debug(f"Checking usage type: {usage_type}")
            
            # Check if the usage type matches our expected prefix
            if usage_type.startswith(usage_type_prefix):
                terms = price_data.get('terms', {}).get('OnDemand', {})
                for term_info in terms.values():
                    price_dimensions = term_info.get('priceDimensions', {})
                    for dimension_info in price_dimensions.values():
                        price = float(dimension_info.get('pricePerUnit', {}).get('USD', 0))
                        logger.debug(f"Found price {price} for {instance_class} ({database_engine}) with {storage_type} storage and {deployment_option} deployment")
                        return price
        
        logger.warning(f"No pricing found for {instance_class} ({database_engine}) with {storage_type} storage and {deployment_option} deployment in {AWS_REGION}, trying fallback pricing")
        
        # Try fallback pricing if API pricing not found
        fallback_price = get_fallback_instance_pricing(
            'Aurora' if 'aurora' in engine_type.lower() else 'RDS',
            instance_class,
            AWS_REGION,
            deployment_option,
            storage_type,
            ON_DEMAND_PRICING
        )
        if fallback_price:
            logger.info(f"Using fallback price {fallback_price} for {instance_class} in {AWS_REGION}")
            return fallback_price

    except Exception as e:
        logger.error(f"Error getting pricing for {instance_class} ({engine_type}) in {AWS_REGION}: {str(e)}")
        logger.info(f"Trying fallback pricing after API error for {instance_class} in {AWS_REGION}")
        fallback_price = get_fallback_instance_pricing(
            'Aurora' if 'aurora' in engine_type.lower() else 'RDS',
            instance_class,
            AWS_REGION,
            deployment_option,
            storage_type,
            ON_DEMAND_PRICING
        )
        if fallback_price:
            logger.info(f"Using fallback price {fallback_price} for {instance_class} in {AWS_REGION}")
            return fallback_price

        return None

def get_ri_pricing(instance_class, engine_type='Aurora PostgreSQL', storage_type='Standard', deployment_option='Single-AZ'):
    """
    Get Reserved Instance pricing for 1-year no upfront option
    
    Args:
        instance_class: The instance class (e.g., db.r6g.xlarge)
        engine_type: Either 'Aurora PostgreSQL' or 'PostgreSQL'
        storage_type: Either 'Standard' or 'I/O Optimized'
        deployment_option: 'Single-AZ', 'Multi-AZ', or 'Multi-AZ (readable standbys)'
    """
    try:
        pricing_client = get_boto3_client('pricing', region_name='us-east-1')
        region_name = get_region_name()
        
        # Normalize engine type for pricing API
        database_engine = 'Aurora PostgreSQL' if 'aurora' in engine_type.lower() else 'PostgreSQL'
        
        # Determine usage type prefix based on engine type and deployment option
        if 'aurora' in engine_type.lower():
            # For Aurora PostgreSQL
            usage_type_prefix = 'InstanceUsageIOOptimized:' if storage_type == 'I/O Optimized' else 'InstanceUsage:'
        else:
            # For RDS PostgreSQL
            if deployment_option == 'Multi-AZ (readable standbys)':
                usage_type_prefix = 'Multi-AZClusterUsage'
            elif deployment_option == 'Multi-AZ':
                usage_type_prefix = 'Multi-AZUsage'
            else:  # Single-AZ
                usage_type_prefix = 'InstanceUsage'
        
        filters = [
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region_name},
            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_class},
            {'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': database_engine},
            {'Type': 'TERM_MATCH', 'Field': 'termType', 'Value': 'Reserved'},
            {'Type': 'TERM_MATCH', 'Field': 'leaseContractLength', 'Value': '1yr'},
            {'Type': 'TERM_MATCH', 'Field': 'offeringClass', 'Value': 'standard'}
        ]

        # Add deployment option filter
        if database_engine == 'PostgreSQL':
            if deployment_option == 'Multi-AZ (readable standbys)':
                filters.append({'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Multi-AZ (readable standbys)'})
            elif deployment_option == 'Multi-AZ':
                filters.append({'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Multi-AZ'})
            else:
                filters.append({'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': 'Single-AZ'})

        logger.debug(f"RI Pricing API Filters: {json.dumps(filters, indent=2)}")
        logger.debug(f"Looking for usage type prefix: {usage_type_prefix}")

        response = pricing_client.get_products(
            ServiceCode='AmazonRDS',
            Filters=filters
        )
        
        logger.debug(f"RI Pricing API Response: {json.dumps(response, indent=2)}")
        
        for price_item in response.get('PriceList', []):
            price_data = json.loads(price_item)
            attributes = price_data.get('product', {}).get('attributes', {})
            
            usage_type = attributes.get('usagetype', '')
            logger.debug(f"Checking usage type: {usage_type}")
            
            # Check if the usage type matches our expected prefix
            if usage_type.startswith(usage_type_prefix):
                terms = price_data.get('terms', {}).get('Reserved', {})
                for term_info in terms.values():
                    offering_class = term_info.get('termAttributes', {}).get('OfferingClass')
                    lease_contract_length = term_info.get('termAttributes', {}).get('LeaseContractLength')
                    purchase_option = term_info.get('termAttributes', {}).get('PurchaseOption')
                    
                    # Look for 1-year No Upfront price
                    if (lease_contract_length == '1yr' and 
                        purchase_option == 'No Upfront' and
                        offering_class == 'standard'):
                        
                        price_dimensions = term_info.get('priceDimensions', {})
                        for dimension_info in price_dimensions.values():
                            if dimension_info.get('unit') == 'Hrs':
                                price = float(dimension_info.get('pricePerUnit', {}).get('USD', 0))
                                logger.debug(f"Found RI price {price} for {instance_class} ({database_engine}) with {storage_type} storage and {deployment_option} deployment")
                                return price

        logger.warning(f"No RI pricing found for {instance_class} ({database_engine}) with {storage_type} storage and {deployment_option} deployment in {AWS_REGION}")

        # Try fallback pricing if API pricing not found
        fallback_price = get_fallback_instance_pricing(
            'Aurora' if 'aurora' in engine_type.lower() else 'RDS',
            instance_class,
            AWS_REGION,
            deployment_option,
            storage_type,
            RI_PRICING
        )
        if fallback_price:
            logger.info(f"Using fallback price {fallback_price} for {instance_class} in {AWS_REGION}")
            return fallback_price

    except Exception as e:
        logger.error(f"Error getting RI pricing for {instance_class} ({engine_type}) in {AWS_REGION}: {str(e)}")
        logger.info(f"Trying fallback pricing after API error for {instance_class} in {AWS_REGION}")
        fallback_price = get_fallback_instance_pricing(
            'Aurora' if 'aurora' in engine_type.lower() else 'RDS',
            instance_class,
            AWS_REGION,
            deployment_option,
            storage_type,
            RI_PRICING
        )
        if fallback_price:
            logger.info(f"Using fallback price {fallback_price} for {instance_class} in {AWS_REGION}")
            return fallback_price

        return None

def get_serverless_cost_estimates(acu_price, min_acu=0.5, max_acu=None):
    """
    Calculate cost estimates for Aurora Serverless v2 based on ACU range
    """
    if max_acu is None:
        max_acu = min_acu

    min_hourly = acu_price * min_acu
    max_hourly = acu_price * max_acu

    min_monthly = calculate_monthly_cost(min_hourly)
    max_monthly = calculate_monthly_cost(max_hourly)

    return {
        'min_acu': min_acu,
        'max_acu': max_acu,
        'acu_price': acu_price,
        'min_hourly_cost': round(min_hourly, 3),
        'max_hourly_cost': round(max_hourly, 3),
        'min_monthly_cost': round(min_monthly, 2),
        'max_monthly_cost': round(max_monthly, 2)
    }

def get_instance_details(rds_client, instance_identifier):
    """
    Get detailed information about a DB instance including pricing
    Args:
        rds_client: The RDS client instance
        instance_identifier: The DB instance identifier
    Returns:
        Dictionary containing instance details and pricing information
    """
    try:
        response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_identifier)
        instance = response['DBInstances'][0]
        instance_class = instance['DBInstanceClass']
        specs = get_instance_specifications(instance_class)
        
        engine = instance['Engine']
        
        # Determine if Aurora or RDS PostgreSQL
        if 'aurora' in engine.lower():
            engine_type = 'Aurora PostgreSQL'
            storage_type = 'Standard'
            if 'StorageType' in instance:
                storage_type = 'I/O Optimized' if instance['StorageType'].lower() == 'aurora-iopt1' else 'Standard'
        else:
            engine_type = 'PostgreSQL'
            storage_type = instance.get('StorageType', 'Standard')
            
        logger.debug(f"Instance {instance_identifier} storage type: {storage_type}")
        
        # Initialize deployment flags
        is_multi_az_cluster = instance.get('DBClusterIdentifier') and not 'aurora' in engine.lower()
        is_read_replica = instance.get('ReadReplicaSourceDBInstanceIdentifier') or instance.get('ReadReplicaSourceDBClusterIdentifier')
        is_multi_az = instance.get('MultiAZ', False)
        
        # Get cluster details for cluster instances
        if is_multi_az_cluster:
            try:
                cluster_response = rds_client.describe_db_clusters(DBClusterIdentifier=instance['DBClusterIdentifier'])
                cluster = cluster_response['DBClusters'][0]
                is_cluster_writer = False
                for member in cluster['DBClusterMembers']:
                    if member['DBInstanceIdentifier'] == instance_identifier:
                        is_cluster_writer = member['IsClusterWriter']
                        break
            except Exception as e:
                logger.error(f"Error getting cluster details for {instance_identifier}: {str(e)}")
                is_cluster_writer = False
        
        # Determine deployment option and description
        if is_multi_az_cluster:
            # For Multi-AZ DB cluster
            if is_read_replica:
                # Read replica of Multi-AZ DB cluster - check its Multi-AZ status
                deployment_option = 'Multi-AZ' if is_multi_az else 'Single-AZ'
                deployment_description = f"Multi-AZ DB Cluster Read Replica ({deployment_option})"
            else:
                # Regular Multi-AZ DB cluster instance
                deployment_option = 'Multi-AZ (readable standbys)'
                deployment_description = f"Multi-AZ DB Cluster ({'writer' if is_cluster_writer else 'reader'})"
        else:
            # For regular RDS PostgreSQL instances
            if is_read_replica:
                # Read replica of regular instance - check its Multi-AZ status
                deployment_option = 'Multi-AZ' if is_multi_az else 'Single-AZ'
                
                # Get source instance details if available
                source_identifier = instance.get('ReadReplicaSourceDBInstanceIdentifier')
                if source_identifier:
                    try:
                        source_response = rds_client.describe_db_instances(DBInstanceIdentifier=source_identifier)
                        source_multi_az = source_response['DBInstances'][0].get('MultiAZ', False)
                        source_type = "Multi-AZ" if source_multi_az else "Single-AZ"
                        deployment_description = f"{source_type} Read Replica ({deployment_option})"
                    except Exception as e:
                        logger.error(f"Error getting source instance details for replica {instance_identifier}: {str(e)}")
                        deployment_description = f"Read Replica ({deployment_option})"
                else:
                    deployment_description = f"Read Replica ({deployment_option})"
            else:
                # Regular instance
                deployment_option = 'Multi-AZ' if is_multi_az else 'Single-AZ'
                deployment_description = deployment_option
            
        logger.debug(f"Instance {instance_identifier} deployment option: {deployment_option}, description: {deployment_description}")
        
        is_serverless = instance_class == 'db.serverless'
        
        # Use the appropriate ACU price based on storage type
        if storage_type == 'I/O Optimized':
            acu_price = ACU_PRICE_IO_OPTIMIZED
        else:
            acu_price = ACU_PRICE_STANDARD
        
        if is_serverless:
            min_acu = instance.get('ServerlessV2ScalingConfiguration', {}).get('MinCapacity', 0.5)
            max_acu = instance.get('ServerlessV2ScalingConfiguration', {}).get('MaxCapacity', min_acu)
            serverless_pricing = get_serverless_cost_estimates(acu_price, min_acu, max_acu)
            
            return {
                'DBInstanceClass': instance_class,
                'Engine': engine,
                'EngineVersion': instance['EngineVersion'],
                'DBInstanceStatus': instance['DBInstanceStatus'],
                'StorageType': storage_type,
                'DeploymentOption': deployment_description,
                'vCPU': 'Serverless',
                'MemoryGiB': 'Serverless',
                'IsServerless': True,
                'ACUPricePerHour': acu_price,
                'MinACU': serverless_pricing['min_acu'],
                'MaxACU': serverless_pricing['max_acu'],
                'ServerlessMinHourlyCost': serverless_pricing['min_hourly_cost'],
                'ServerlessMaxHourlyCost': serverless_pricing['max_hourly_cost'],
                'ServerlessMinMonthlyCost': serverless_pricing['min_monthly_cost'],
                'ServerlessMaxMonthlyCost': serverless_pricing['max_monthly_cost'],
                'OnDemandHourlyRate': None,
                'OnDemandMonthlyEstimate': None,
                'RI_1yr_NoUpfront_HourlyRate': None,
                'RI_1yr_NoUpfront_MonthlyEstimate': None
            }
        else:
            hourly_rate = get_instance_pricing(instance_class, engine_type, storage_type, deployment_option)
            monthly_cost = calculate_monthly_cost(hourly_rate)
            
            # Get RI pricing as a single value
            ri_hourly_rate = get_ri_pricing(instance_class, engine_type, storage_type, deployment_option)
            ri_monthly_cost = calculate_monthly_cost(ri_hourly_rate) 
            
            return {
                'DBInstanceClass': instance_class,
                'Engine': engine,
                'EngineVersion': instance['EngineVersion'],
                'DBInstanceStatus': instance['DBInstanceStatus'],
                'StorageType': storage_type,
                'DeploymentOption': deployment_description,
                'vCPU': specs['vcpu'],
                'MemoryGiB': specs['memory'],
                'IsServerless': False,
                'ACUPricePerHour': acu_price,
                'MinACU': None,
                'MaxACU': None,
                'ServerlessMinHourlyCost': None,
                'ServerlessMaxHourlyCost': None,
                'ServerlessMinMonthlyCost': None,
                'ServerlessMaxMonthlyCost': None,
                'OnDemandHourlyRate': hourly_rate,
                'OnDemandMonthlyEstimate': monthly_cost,
                'RI_1yr_NoUpfront_HourlyRate': ri_hourly_rate,
                'RI_1yr_NoUpfront_MonthlyEstimate': ri_monthly_cost
            }

    except Exception as e:
        logger.error(f"Error getting instance details for {instance_identifier}: {str(e)}")
        return None
    
def get_all_postgres_clusters(cluster_identifier='all'):
    """
    Get all PostgreSQL clusters/instances in the specified region
    """
    rds = get_boto3_client('rds', region_name=AWS_REGION)
    clusters = []
    processed_instances = set()

    try:
        # Get clusters list based on identifier
        if cluster_identifier.lower() == 'all':
            # Use paginator for all clusters
            paginator = rds.get_paginator('describe_db_clusters')
            clusters_to_process = []
            for page in paginator.paginate():
                clusters_to_process.extend(page['DBClusters'])
        else:
            # Get single cluster
            try:
                response = rds.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
                clusters_to_process = response['DBClusters']
            except rds.exceptions.DBClusterNotFoundFault:
                # If not found as cluster, try as standalone instance
                try:
                    response = rds.describe_db_instances(DBInstanceIdentifier=cluster_identifier)
                    instance = response['DBInstances'][0]
                    
                    # Only process if it's PostgreSQL and not part of a cluster
                    if 'postgres' in instance['Engine'].lower() and not instance.get('DBClusterIdentifier'):
                        clusters.append({
                            'cluster_identifier': cluster_identifier,
                            'engine': instance['Engine'],
                            'is_aurora': False,
                            'is_multi_az': instance.get('MultiAZ', False),
                            'is_multi_az_cluster': False
                        })
                        logger.info(f"Found {'Multi-AZ' if instance.get('MultiAZ', False) else 'Single-AZ'} RDS instance: {cluster_identifier}")
                        
                        # Log read replicas if any
                        if instance.get('ReadReplicaDBInstanceIdentifiers'):
                            for replica_id in instance['ReadReplicaDBInstanceIdentifiers']:
                                logger.info(f"Found read replica: {replica_id} for instance {cluster_identifier}")
                                
                except rds.exceptions.DBInstanceNotFoundFault:
                    logger.error(f"No PostgreSQL cluster or instance found with identifier {cluster_identifier}")
                return clusters

        # Process all clusters (either all or single cluster)
        for cluster in clusters_to_process:
            # Check if it's a PostgreSQL cluster (either Aurora or Multi-AZ DB cluster)
            if cluster['Engine'].lower() in ['postgres', 'aurora-postgresql']:
                is_aurora = 'aurora' in cluster['Engine'].lower()
                
                # Add cluster to the list
                clusters.append({
                    'cluster_identifier': cluster['DBClusterIdentifier'],
                    'engine': cluster['Engine'],
                    'is_aurora': is_aurora,
                    'is_multi_az_cluster': not is_aurora
                })
                
                # Log cluster details
                if is_aurora:
                    logger.info(f"Found Aurora cluster: {cluster['DBClusterIdentifier']}")
                else:
                    logger.info(f"Found Multi-AZ DB cluster: {cluster['DBClusterIdentifier']} with {len(cluster['DBClusterMembers'])} instances")
                    # Log member instances
                    for member in cluster['DBClusterMembers']:
                        role = "writer" if member['IsClusterWriter'] else "reader"
                        logger.info(f"Found {role} instance: {member['DBInstanceIdentifier']}")
                        processed_instances.add(member['DBInstanceIdentifier'])

        # If looking for all clusters, also get standalone instances
        if cluster_identifier.lower() == 'all':
            # Get standalone RDS PostgreSQL instances
            paginator = rds.get_paginator('describe_db_instances')
            for page in paginator.paginate():
                for instance in page['DBInstances']:
                    # Skip if already processed as part of a cluster
                    if instance['DBInstanceIdentifier'] in processed_instances:
                        continue
                        
                    # Check if it's PostgreSQL and not part of any cluster
                    if ('postgres' in instance['Engine'].lower() and 
                        not instance.get('DBClusterIdentifier') and 
                        not instance.get('ReadReplicaSourceDBInstanceIdentifier')):
                        
                        clusters.append({
                            'cluster_identifier': instance['DBInstanceIdentifier'],
                            'engine': instance['Engine'],
                            'is_aurora': False,
                            'is_multi_az': instance.get('MultiAZ', False),
                            'is_multi_az_cluster': False
                        })
                        logger.info(f"Found {'Multi-AZ' if instance.get('MultiAZ', False) else 'Single-AZ'} RDS instance: {instance['DBInstanceIdentifier']}")
                        
                        # Log read replicas if any
                        if instance.get('ReadReplicaDBInstanceIdentifiers'):
                            for replica_id in instance['ReadReplicaDBInstanceIdentifiers']:
                                logger.info(f"Found read replica: {replica_id} for instance {instance['DBInstanceIdentifier']}")

        logger.info(f"Found {len(clusters)} PostgreSQL clusters/instances matching the criteria in region {AWS_REGION}")
        return clusters

    except Exception as e:
        logger.error(f"Error getting PostgreSQL clusters in region {AWS_REGION}: {str(e)}")
        return []

def determine_migration_path(platform_type, instance_class):
    """
    Determine the ASV2 migration path based on platform type and instance class
    """
    if instance_class == 'db.serverless':
        return 'NoAction'
    return 'In-Place' if platform_type == 'Aurora' else 'Platform'

def get_instance_ratio(instance_class):
    """
    Get the Provisioned:Serverless ratio for a given instance class
    """
    if instance_class == 'db.serverless':
        return 1
    elif instance_class.startswith('db.r'):  # memory-optimized
        return PROVISIONED_SERVERLESS_RATIO['memory']
    elif instance_class.startswith('db.c'):  # compute-optimized
        return PROVISIONED_SERVERLESS_RATIO['compute']
    elif instance_class.startswith(('db.m', 'db.t')):  # general-purpose or burstable
        return PROVISIONED_SERVERLESS_RATIO['general']
    return 0  # for unknown instances

def round_up_to_half(value):
    """
    Round up a value to the nearest 0.5
    Examples:
    0.7724 -> 1.0
    1.11 -> 1.5
    2.3 -> 2.5
    2.7 -> 3.0
    """
    double = value * 2          # Convert 0.5 steps to whole numbers
    ceiling = math.ceil(double) # Round up to next whole number
    result = ceiling / 2        # Convert back to 0.5 steps
    return result

def calculate_growth_capacity_factor(instance_class, vcpu):
    """
    Calculate growth capacity factor based on maximum available ACUs
    """
    if instance_class == 'db.serverless':
        return None
    
    if not isinstance(vcpu, (int, float)):
        return None
        
    ratio = get_instance_ratio(instance_class)
    current_provisioned_acu = vcpu * ratio
    
    if current_provisioned_acu <= 0:
        return None
        
    remaining_capacity = MAX_ASV2_ACU - current_provisioned_acu
    return round(remaining_capacity / current_provisioned_acu, 2)

def calculate_vcpu_utilization(p95_cpu, vcpu, instance_class):
    """
    Calculate vCPU utilization percentage
    """
    if instance_class == 'db.serverless':
        return None
        
    if p95_cpu is None or not isinstance(vcpu, (int, float)):
        return None
    return round((p95_cpu / 100) * vcpu, 2)

def calculate_actual_estimate_acu(vcpu_utilization, instance_class):
    """
    Calculate actual estimate ACU based on vCPU utilization
    """
    if instance_class == 'db.serverless':
        return None
        
    if vcpu_utilization is None:
        return None

    # Calculate raw ACU value
    raw_acu = vcpu_utilization * get_instance_ratio(instance_class)
    
    # Round up to nearest 0.5
    return round_up_to_half(raw_acu)

def calculate_actual_estimate_acu_cost(actual_estimate_acu, acu_price, instance_class):
    """
    Calculate actual estimate ACU cost per hour
    """
    if instance_class == 'db.serverless':
        return None
        
    if actual_estimate_acu is None:
        return None
    return round(actual_estimate_acu * acu_price, 2)

def calculate_adjusted_estimate_acu(p95_cpu, max_cpu, vcpu, instance_class):
    """
    Calculate adjusted estimate ACU using weighted CPU values
    """
    if instance_class == 'db.serverless':
        return None
        
    if None in (p95_cpu, max_cpu) or not isinstance(vcpu, (int, float)):
        return None

    # Calculate raw ACU value
    weighted_cpu = ((p95_cpu * 0.95) + (max_cpu * 0.05)) / 100
    raw_acu = weighted_cpu * vcpu * get_instance_ratio(instance_class)
    
    # Round up to nearest 0.5
    return round_up_to_half(raw_acu)

def calculate_adjusted_estimate_acu_cost(adjusted_estimate_acu, acu_price, instance_class):
    """
    Calculate adjusted estimate ACU cost per hour
    """
    if instance_class == 'db.serverless':
        return None
        
    if adjusted_estimate_acu is None:
        return None
    return round(adjusted_estimate_acu * acu_price, 2)       

def calculate_migration_metrics(metrics_data, platform_type):
    """
    Calculate all migration-related metrics for each row in the metrics data
    
    Args:
        metrics_data (list): List of dictionaries containing metric data
        platform_type (str): 'Aurora' or 'RDS'
        
    Returns:
        list: Updated metrics data with migration-related calculations
    """
    for metric in metrics_data:
        instance_class = metric['dbinstance_class']
        
        # Migration path
        metric['asv2_migration_path'] = determine_migration_path(
            platform_type,
            instance_class
        )
        
        # Growth capacity factor
        metric['growth_capacity_factor'] = calculate_growth_capacity_factor(
            instance_class, 
            metric['vcpu']
        )
        
        # vCPU utilization
        metric['vcpu_utilization'] = calculate_vcpu_utilization(
            metric['p95_cpu_utilization'],
            metric['vcpu'],
            instance_class
        )
        
        # Actual estimate ACU
        metric['actual_estimate_acu'] = calculate_actual_estimate_acu(
            metric['vcpu_utilization'],
            instance_class
        )
        
        # Actual estimate ACU cost
        metric['actual_estimate_acu_price_per_hour'] = calculate_actual_estimate_acu_cost(
            metric['actual_estimate_acu'],
            metric['acu_price_per_hour'],
            instance_class
        )
        
        # Adjusted estimate ACU
        metric['adjusted_estimate_acu'] = calculate_adjusted_estimate_acu(
            metric['p95_cpu_utilization'],
            metric['max_cpu_utilization'],
            metric['vcpu'],
            instance_class
        )
        
        # Adjusted estimate ACU cost
        metric['adjusted_estimate_acu_price_per_hour'] = calculate_adjusted_estimate_acu_cost(
            metric['adjusted_estimate_acu'],
            metric['acu_price_per_hour'],
            instance_class
        )
    
    return metrics_data   

def find_outliers(df, column):
    """
    Find outliers in a DataFrame column using IQR method
    
    Args:
        df (DataFrame): Input DataFrame
        column (str): Column name to analyze
        
    Returns:
        DataFrame: Rows containing outlier values
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[
        (df[column] < (Q1 - 1.5 * IQR)) | 
        (df[column] > (Q3 + 1.5 * IQR))
    ]
    return outliers

def determine_usage_pattern(instance_raw_data, avg_cpu, p95_cpu, max_cpu, std_dev):
    """
    Determine the usage pattern using CPU metrics.
    Args:
        instance_raw_data: DataFrame containing raw metrics data
        avg_cpu: Average CPU utilization
        p95_cpu: 95th percentile CPU utilization
        max_cpu: Maximum CPU utilization
        std_dev: Standard deviation of CPU utilization
    Returns:
        tuple: (pattern, notes)
    """
    try:
        # Group by hour and day of week for both metrics
        hourly_by_dow_avg = instance_raw_data.groupby(['day_of_week', 'hour'])['avg_cpu'].mean()
        hourly_by_dow_p95 = instance_raw_data.groupby(['day_of_week', 'hour'])['p95_cpu'].mean()
        
        # Calculate variations
        hourly_variation_avg = hourly_by_dow_avg.std()
        hourly_variation_p95 = hourly_by_dow_p95.std()
        
        # Enhanced outlier analysis
        def analyze_outliers(df, column):
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            outlier_threshold = Q3 + (1.5 * IQR)
            outliers = df[df[column] > outlier_threshold].copy()
            
            if len(outliers) > 0:
                # Analyze outlier patterns
                outlier_hours = outliers.groupby('hour')[column].count()
                most_common_hour = outlier_hours.idxmax()
                hour_count = outlier_hours[most_common_hour]
                
                outlier_days = outliers.groupby('day_of_week')[column].count()
                most_common_day = outlier_days.idxmax()
                day_count = outlier_days[most_common_day]
                
                # Get time ranges with high concentration of outliers
                hour_ranges = []
                current_range = []
                for hour in range(24):
                    if hour in outlier_hours.index and outlier_hours[hour] > 0:
                        current_range.append(hour)
                    elif current_range:
                        if len(current_range) > 1:
                            hour_ranges.append(f"{current_range[0]:02d}:00-{current_range[-1]:02d}:00")
                        else:
                            hour_ranges.append(f"{current_range[0]:02d}:00")
                        current_range = []
                
                # Get the max value during outliers
                max_value = outliers[column].max()
                
                return {
                    'count': len(outliers),
                    'max_value': max_value,
                    'common_hour': most_common_hour,
                    'hour_count': hour_count,
                    'common_day': most_common_day,
                    'day_count': day_count,
                    'hour_ranges': hour_ranges
                }
            return None

        # Find outliers
        outliers_avg = analyze_outliers(instance_raw_data, 'avg_cpu')
        outliers_p95 = analyze_outliers(instance_raw_data, 'p95_cpu')
        
        # Calculate daily patterns
        daily_avg = instance_raw_data.groupby('day_of_week')['avg_cpu'].mean()
        daily_p95 = instance_raw_data.groupby('day_of_week')['p95_cpu'].mean()

        # Calculate business hours vs non-business hours
        business_hours_df = instance_raw_data[instance_raw_data['hour'].between(9, 17)]
        non_business_df = instance_raw_data[~instance_raw_data['hour'].between(9, 17)]
        business_hours = business_hours_df['avg_cpu'].mean() if not business_hours_df.empty else 0
        non_business = non_business_df['avg_cpu'].mean() if not non_business_df.empty else 0

        # Calculate weekday vs weekend with safer indexing
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        weekend_days = ['Saturday', 'Sunday']
        
        # Filter for available days only
        available_weekdays = [day for day in weekdays if day in daily_avg.index]
        available_weekend_days = [day for day in weekend_days if day in daily_avg.index]
        
        weekday_avg = daily_avg[available_weekdays].mean() if available_weekdays else 0
        weekend_avg = daily_avg[available_weekend_days].mean() if available_weekend_days else 0

        # Determine pattern
        if outliers_p95 and outliers_p95['count'] > len(instance_raw_data) * 0.15:  # More than 15% outliers
            pattern = "Outliers"
            
            # Create detailed outlier description
            time_pattern = ""
            if outliers_p95['hour_ranges']:
                time_pattern = f" primarily during {', '.join(outliers_p95['hour_ranges'])}"
            
            day_pattern = ""
            if outliers_p95['common_day']:
                day_pattern = f", most frequently on {outliers_p95['common_day']}s ({outliers_p95['day_count']} occurrences)"
            
            notes = [f"Detected {outliers_p95['count']} significant spikes{time_pattern}{day_pattern}. Max CPU reached {max_cpu:.1f}% vs average {avg_cpu:.1f}%. P95 CPU is {p95_cpu:.1f}%."
            ]
            
        elif std_dev < 5:
            pattern = "Consistent"
            notes = [f"Very stable usage with average CPU {avg_cpu:.1f}%. Standard deviation {std_dev:.1f}% indicates minimal variation. P95 CPU remains steady at {p95_cpu:.1f}%."
            ]
            
        elif weekday_avg > 0 and weekend_avg > 0 and (weekday_avg / weekend_avg > 1.5):
            pattern = "Peaks and Valleys"
            notes = [f"Clear business hours pattern with {business_hours:.1f}% CPU during business hours vs {non_business:.1f}% outside. Weekday utilization {weekday_avg:.1f}% vs weekend {weekend_avg:.1f}%."
            ]
            
        else:
            pattern = "Random"
            notes = [f"No clear pattern detected. Average CPU {avg_cpu:.1f}% with standard deviation {std_dev:.1f}%. CPU varies between {daily_avg.min():.1f}% and {daily_avg.max():.1f}% with P95 at {p95_cpu:.1f}%."
            ]
        
        return pattern, notes
        
    except Exception as e:
        logger.error(f"Error determining usage pattern: {str(e)}")
        return "Unknown", ["Insufficient data for pattern analysis."]

def analyze_usage_patterns(metrics_data, raw_hourly_df):
    """
    Analyze instance usage patterns based on detailed CPU utilization metrics
    
    Args:
        metrics_data (list): List of dictionaries containing metric data
        raw_hourly_df (DataFrame): DataFrame containing raw hourly metrics
        
    Returns:
        list: Updated metrics data with usage pattern analysis
    """
    for metric in metrics_data:
        instance_id = metric['dbinstance_identifier']
        cluster_id = metric['cluster_identifier']
        
        # Get raw data for this instance
        instance_raw_data = raw_hourly_df[
            (raw_hourly_df['dbinstance_identifier'] == instance_id) & 
            (raw_hourly_df['cluster_identifier'] == cluster_id)
        ]
        
        if instance_raw_data.empty:
            metric['usage_pattern'] = None
            metric['usage_pattern_notes'] = "Insufficient data for pattern analysis"
            continue
            
        # Calculate key metrics
        avg_cpu = instance_raw_data['avg_cpu'].mean()
        p95_cpu = instance_raw_data['p95_cpu'].mean()
        max_cpu = instance_raw_data['avg_cpu'].max()
        std_dev = instance_raw_data['avg_cpu'].std()
        
        # Use traditional pattern analysis
        pattern, notes = determine_usage_pattern(
            instance_raw_data,
            avg_cpu,
            p95_cpu,
            max_cpu,
            std_dev
        )
        metric['usage_pattern'] = pattern
        metric['usage_pattern_notes'] = '\n'.join(notes)

    return metrics_data
    
def collect_instance_hourly_metrics(cloudwatch, instance_id, instance_details, start_time, end_time, sample_period_days, exec_timestamp_utc, cluster_identifier=None):
    """
    Helper function to collect hourly metrics for a single instance
    """
    metrics_data = []
    raw_hourly_data = []
    platform_type = 'Aurora' if 'aurora' in instance_details['Engine'].lower() else 'RDS'

    # Get current date in local timezone for run_date_local
    run_date_local = RUN_DATE
    # Format start and end dates in UTC
    start_date_utc = start_time.strftime('%Y-%m-%d')
    end_date_utc = end_time.strftime('%Y-%m-%d')    
    
    logger.debug(f"Collecting metrics from {start_time} to {end_time} ({sample_period_days} days)")
    
    for hour in range(24):
        # Calculate start and end times for this specific hour
        hour_metrics = []
        
        # Iterate through each day in the sample period
        for day in range(sample_period_days):
            day_start = start_time + timedelta(days=day)
            day_start = day_start.replace(hour=hour, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(hours=1)
            
            # First call for standard statistics
            response_standard = cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': instance_id
                    }
                ],
                StartTime=day_start,
                EndTime=day_end,
                Period=3600,
                Statistics=['Average', 'Maximum']
            )

            # Second call for extended statistics
            response_extended = cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': instance_id
                    }
                ],
                StartTime=day_start,
                EndTime=day_end,
                Period=3600,
                ExtendedStatistics=['p95']
            )

            # Merge the datapoints from both responses
            for std_dp in response_standard['Datapoints']:
                # Find matching extended datapoint by timestamp
                ext_dp = next((dp for dp in response_extended['Datapoints'] 
                             if dp['Timestamp'] == std_dp['Timestamp']), None)
                
                if ext_dp:
                    merged_dp = {
                        'timestamp': std_dp['Timestamp'],
                        'day_of_week': std_dp['Timestamp'].strftime('%A'),
                        'hour': std_dp['Timestamp'].hour,
                        'avg_cpu': std_dp['Average'],
                        'max_cpu': std_dp['Maximum'],
                        'p95_cpu': ext_dp['ExtendedStatistics']['p95'],
                        'dbinstance_identifier': instance_id,
                        'cluster_identifier': cluster_identifier if cluster_identifier else instance_id
                    }
                    raw_hourly_data.append(merged_dp)
                    
            # Add datapoints for hourly aggregation with merged data
            if response_standard['Datapoints'] and response_extended['Datapoints']:
                for std_dp in response_standard['Datapoints']:
                    ext_dp = next((dp for dp in response_extended['Datapoints'] 
                                 if dp['Timestamp'] == std_dp['Timestamp']), None)
                    if ext_dp:
                        merged_metric = dict(std_dp)
                        merged_metric['ExtendedStatistics'] = ext_dp['ExtendedStatistics']
                        hour_metrics.append(merged_metric)
        
        # Log the datapoints for debugging
        logger.debug(f"\nCollecting data for hour {hour:02d}:00")
        logger.debug(f"Number of datapoints for hour {hour}: {len(hour_metrics)}")
        if hour_metrics:
            logger.debug("Datapoints timestamps:")
            for dp in hour_metrics:
                logger.debug(f"  {dp['Timestamp']}")

        # Format hour in 12-hour format
        hour_time = start_time.replace(hour=hour)
        formatted_hour = hour_time.strftime('%I:00:00 %p')

        base_metrics = {
            'exec_timestamp_utc': exec_timestamp_utc,
            'run_date_local': run_date_local,
            'start_date_utc': start_date_utc,
            'end_date_utc': end_date_utc,            
            'aws_account_id': AWS_ACCOUNT_ID,
            'aws_region': AWS_REGION,
            'cluster_identifier': cluster_identifier if cluster_identifier else instance_id,
            'dbinstance_identifier': instance_id,
            'dbinstance_class': instance_details['DBInstanceClass'],
            'engine': instance_details['Engine'],
            'engine_version': instance_details['EngineVersion'],
            'storage_type': instance_details['StorageType'],
            'deployment_option': instance_details['DeploymentOption'],
            'dbinstance_status': instance_details['DBInstanceStatus'],
            'vcpu': instance_details['vCPU'],
            'memory_gib': instance_details['MemoryGiB'],
            'is_serverless': instance_details['IsServerless'],
            'acu_price_per_hour': instance_details['ACUPricePerHour'],
            'min_acu': instance_details['MinACU'],
            'max_acu': instance_details['MaxACU'],
            'serverless_min_hourly_cost': instance_details['ServerlessMinHourlyCost'],
            'serverless_max_hourly_cost': instance_details['ServerlessMaxHourlyCost'],
            'serverless_min_monthly_cost': instance_details['ServerlessMinMonthlyCost'],
            'serverless_max_monthly_cost': instance_details['ServerlessMaxMonthlyCost'],
            'on_demand_hourly_rate': instance_details['OnDemandHourlyRate'],
            'on_demand_monthly_estimate': instance_details['OnDemandMonthlyEstimate'],
            'ri_1yr_no_upfront_hourly': instance_details['RI_1yr_NoUpfront_HourlyRate'],
            'ri_1yr_no_upfront_monthly': instance_details['RI_1yr_NoUpfront_MonthlyEstimate'],
            'sample_period_days': sample_period_days,
            'observation_days': len(hour_metrics),
            'utc_hour': formatted_hour,
            'platform_type': platform_type
        }

        if hour_metrics:
            # Calculate aggregates for this hour across all observed days
            avg_cpu = sum(d['Average'] for d in hour_metrics) / len(hour_metrics)
            max_cpu = max(d['Maximum'] for d in hour_metrics)
            p95_cpu = sum(d['ExtendedStatistics']['p95'] for d in hour_metrics) / len(hour_metrics)
            
            metrics_data.append({
                **base_metrics,
                'avg_cpu_utilization': round(avg_cpu, 2),
                'max_cpu_utilization': round(max_cpu, 2),
                'p95_cpu_utilization': round(p95_cpu, 2)
            })
        else:
            metrics_data.append({
                **base_metrics,
                'avg_cpu_utilization': None,
                'max_cpu_utilization': None,
                'p95_cpu_utilization': None
            })

    # Calculate migration metrics
    metrics_data = calculate_migration_metrics(metrics_data, platform_type)    

    # Create DataFrame from raw hourly data
    raw_hourly_df = pd.DataFrame(raw_hourly_data)    

    # Analyze usage patterns
    if not raw_hourly_df.empty:
        metrics_data = analyze_usage_patterns(metrics_data, raw_hourly_df)            
    
    return metrics_data

def collect_cloudwatch_metrics(cluster_identifier, sample_period_days, start_time, end_time, exec_timestamp_utc):
    """
    Collect CloudWatch metrics for Aurora PostgreSQL instances
    """
    try:
        cloudwatch = get_boto3_client('cloudwatch', region_name=AWS_REGION)
        rds = get_boto3_client('rds', region_name=AWS_REGION)
        
        response = rds.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        instance_list = response['DBClusters'][0]['DBClusterMembers']
        
        metrics_data = []
        processed_instances = set()  # Keep track of processed instances
        
        for instance in instance_list:
            instance_identifier = instance['DBInstanceIdentifier']
            
            if instance_identifier not in processed_instances:
                logger.info(f"Collecting metrics for instance {instance_identifier} in cluster {cluster_identifier}")
                
                try:
                    instance_details = get_instance_details(rds, instance_identifier)
                    processed_instances.add(instance_identifier)
                    
                    # Use the shared function to collect hourly metrics
                    instance_metrics = collect_instance_hourly_metrics(
                        cloudwatch,
                        instance_identifier,
                        instance_details,
                        start_time,
                        end_time,
                        sample_period_days,
                        exec_timestamp_utc,
                        cluster_identifier  # Pass cluster_identifier for Aurora instances
                    )
                    metrics_data.extend(instance_metrics)

                except Exception as e:
                    logger.error(f"Error collecting metrics for instance {instance_identifier}: {str(e)}")
                    continue

        return pd.DataFrame(metrics_data)

    except Exception as e:
        logger.error(f"Error collecting metrics for cluster {cluster_identifier}: {str(e)}")
        return pd.DataFrame()

def collect_rds_instance_metrics(instance_identifier, sample_period_days, start_time, end_time, exec_timestamp_utc):
    """
    Collect CloudWatch metrics for RDS PostgreSQL instances including Multi-AZ DB clusters and read replicas
    """
    try:
        cloudwatch = get_boto3_client('cloudwatch', region_name=AWS_REGION)
        rds = get_boto3_client('rds', region_name=AWS_REGION)
        
        metrics_data = []
        processed_instances = set()  # Keep track of processed instances
        
        # First try to get cluster details to check if this is a Multi-AZ DB cluster
        try:
            cluster_response = rds.describe_db_clusters(DBClusterIdentifier=instance_identifier)
            cluster = cluster_response['DBClusters'][0]
            
            # If we get here, this is a Multi-AZ DB cluster
            # Get all instances in the cluster
            cluster_instances = cluster['DBClusterMembers']
            
            # Log cluster details
            writer_instances = [member['DBInstanceIdentifier'] for member in cluster_instances if member['IsClusterWriter']]
            reader_instances = [member['DBInstanceIdentifier'] for member in cluster_instances if not member['IsClusterWriter']]
            
            logger.info(f"Found Multi-AZ DB cluster: {instance_identifier} with {len(cluster_instances)} instances")
            for reader_id in reader_instances:
                logger.info(f"Found reader instance: {reader_id}")
            for writer_id in writer_instances:
                logger.info(f"Found writer instance: {writer_id}")
            
            # Process each instance in the cluster
            for member in cluster_instances:
                instance_id = member['DBInstanceIdentifier']
                if instance_id not in processed_instances:
                    try:
                        role = "writer" if member['IsClusterWriter'] else "reader"
                        logger.info(f"Collecting metrics for {role} instance {instance_id}")
                        
                        # Get instance details using describe_db_instances
                        instance_response = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
                        instance = instance_response['DBInstances'][0]
                        
                        # Get detailed instance information
                        instance_details = get_instance_details(rds, instance_id)
                        if instance_details:
                            processed_instances.add(instance_id)
                            
                            # Add role information to instance details
                            instance_details['DeploymentOption'] = f"Multi-AZ DB Cluster ({role})"
                            
                            # Collect metrics for this instance
                            metrics_data.extend(collect_instance_hourly_metrics(
                                cloudwatch,
                                instance_id,
                                instance_details,
                                start_time,
                                end_time,
                                sample_period_days,
                                exec_timestamp_utc,
                                instance_identifier
                            ))
                        else:
                            logger.error(f"Could not get details for instance {instance_id}")
                    except Exception as e:
                        logger.error(f"Error collecting metrics for cluster instance {instance_id}: {str(e)}")
                        continue

            # Check for read replicas of the cluster
            if cluster.get('ReadReplicaIdentifiers'):
                for replica_arn in cluster['ReadReplicaIdentifiers']:
                    try:
                        # Extract instance identifier from ARN
                        replica_id = replica_arn.split(':')[-1]
                        logger.info(f"Found read replica instance: {replica_id}")
                        
                        if replica_id not in processed_instances:
                            try:
                                logger.info(f"Collecting metrics for read replica instance {replica_id}")
                                replica_instance_details = get_instance_details(rds, replica_id)
                                if replica_instance_details:
                                    processed_instances.add(replica_id)
                                    
                                    # Add role information to instance details
                                    replica_instance_details['DeploymentOption'] = "Multi-AZ DB Cluster Read Replica"
                                    
                                    # Collect metrics for replica instance
                                    metrics_data.extend(collect_instance_hourly_metrics(
                                        cloudwatch,
                                        replica_id,
                                        replica_instance_details,
                                        start_time,
                                        end_time,
                                        sample_period_days,
                                        exec_timestamp_utc,
                                        instance_identifier  # Use primary cluster as identifier
                                    ))
                            except Exception as e:
                                logger.error(f"Error collecting metrics for read replica instance {replica_id}: {str(e)}")
                                continue
                    except Exception as e:
                        logger.error(f"Error processing read replica ARN {replica_arn}: {str(e)}")
                        continue
                        
            return pd.DataFrame(metrics_data)
            
        except rds.exceptions.DBClusterNotFoundFault:
            # This is a standalone RDS instance
            if instance_identifier not in processed_instances:
                # Get instance details
                instance_response = rds.describe_db_instances(DBInstanceIdentifier=instance_identifier)
                instance = instance_response['DBInstances'][0]
                
                # Get details of the requested instance
                instance_details = get_instance_details(rds, instance_identifier)
                processed_instances.add(instance_identifier)
                
                # Determine if this is a primary instance or read replica
                is_read_replica = instance.get('ReadReplicaSourceDBInstanceIdentifier') is not None
                is_multi_az = instance.get('MultiAZ', False)
                primary_identifier = instance.get('ReadReplicaSourceDBInstanceIdentifier', instance_identifier)
                
                logger.info(f"Collecting metrics for {('Multi-AZ ' if is_multi_az else '')}{'read replica' if is_read_replica else 'instance'} {instance_identifier}")
                
                # Collect metrics for the current instance
                metrics_data.extend(collect_instance_hourly_metrics(
                    cloudwatch,
                    instance_identifier,
                    instance_details,
                    start_time,
                    end_time,
                    sample_period_days,
                    exec_timestamp_utc,
                    primary_identifier if is_read_replica else None
                ))
                
                # If this is a primary instance, process its read replicas
                if not is_read_replica and instance.get('ReadReplicaDBInstanceIdentifiers'):
                    for replica_id in instance.get('ReadReplicaDBInstanceIdentifiers', []):
                        if replica_id not in processed_instances:
                            try:
                                logger.info(f"Collecting metrics for read replica {replica_id}")
                                replica_details = get_instance_details(rds, replica_id)
                                processed_instances.add(replica_id)
                                
                                # Collect metrics for read replica
                                metrics_data.extend(collect_instance_hourly_metrics(
                                    cloudwatch,
                                    replica_id,
                                    replica_details,
                                    start_time,
                                    end_time,
                                    sample_period_days,
                                    exec_timestamp_utc,
                                    instance_identifier  # Use primary instance as cluster identifier
                                ))
                            except Exception as e:
                                logger.error(f"Error collecting metrics for read replica {replica_id}: {str(e)}")
                                continue

            return pd.DataFrame(metrics_data)

    except Exception as e:
        logger.error(f"Error collecting metrics for instance {instance_identifier}: {str(e)}")
        return pd.DataFrame()

def collect_all_cluster_metrics(sample_period_days, cluster_identifier='all'):
    """
    Collect metrics for all PostgreSQL clusters in the region or a specific cluster
    """

    # Set end time to current time rounded down to the nearest hour
    end_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=sample_period_days + 1)    
    exec_timestamp_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')    

    clusters = get_all_postgres_clusters(cluster_identifier)
    all_metrics = []

    if not clusters:
        logger.warning(f"No clusters found matching identifier: {cluster_identifier}")
        return pd.DataFrame()

    for cluster in clusters:
        try:
            if cluster['is_aurora']:
                logger.info(f"Collecting metrics for Aurora cluster {cluster['cluster_identifier']}")
                df = collect_cloudwatch_metrics(cluster['cluster_identifier'], sample_period_days, start_time, end_time, exec_timestamp_utc)
            else:
                logger.info(f"Collecting metrics for RDS instance {cluster['cluster_identifier']}")
                df = collect_rds_instance_metrics(cluster['cluster_identifier'], sample_period_days, start_time, end_time, exec_timestamp_utc)
            
            if not df.empty:
                all_metrics.append(df)
                logger.info(f"Successfully collected metrics for {cluster['cluster_identifier']}")
            else:
                logger.warning(f"No metrics found for {cluster['cluster_identifier']}")
        except Exception as e:
            logger.error(f"Error collecting metrics for {cluster['cluster_identifier']}: {str(e)}")

    if all_metrics:
        return pd.concat(all_metrics, ignore_index=True)
    else:
        return pd.DataFrame()

def main():
    """
    Main execution function 
    """
    parser = argparse.ArgumentParser(description='Collect RDS/Aurora PostgreSQL metrics')
    parser.add_argument('--cluster-identifier', required=True, 
                      help='Cluster identifier or "all" to process all clusters')
    parser.add_argument('--central-account-id', required=True,
                      help='AWS account ID where the central S3 bucket is located')
    parser.add_argument('--analyze-usage-pattern', 
                      default='rule-based',
                      choices=['rule-based'],
                      help='Analysis method to use (default: rule-based)')                                       
    parser.add_argument('--sample-period-days', type=int, default=30,
                      help='Number of days to collect metrics for (default: 30)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set logging level based on debug flag
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('boto3').setLevel(logging.DEBUG)
        logging.getLogger('botocore').setLevel(logging.DEBUG)
    
    try:
        # Initialize global variables with central account ID
        init(args.central_account_id, args.analyze_usage_pattern)
        
        logger.info(f"Starting metrics collection for region {AWS_REGION}")
        logger.info(f"Time interval: {args.sample_period_days} days")
        logger.info(f"Cluster identifier: {args.cluster_identifier}")
        logger.info(f"Central Account ID: {CENTRAL_ACCOUNT_ID}")

        # Collect metrics
        df = collect_all_cluster_metrics(args.sample_period_days, args.cluster_identifier)
        
        if not df.empty:
            # Save to CSV with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cluster_identifier = args.cluster_identifier.lower()            
            csv_filename = f'postgres_metrics_{AWS_REGION}_{cluster_identifier}_{timestamp}.csv'
            df.to_csv(csv_filename, index=False)
            
            # Upload CSV to S3
            upload_success = upload_cw_metrics_file_to_s3(
                csv_filename, 
                cluster_identifier,
                AWS_REGION         # Pass current region for folder organization
            )
            
            if upload_success:
                logger.info(f"Successfully uploaded {csv_filename} to S3")
                # Clean up local file
                os.remove(csv_filename)
            else:
                logger.warning(f"Failed to upload {csv_filename} to S3, file retained locally")
            
            # Summary statistics calculation
            total_clusters = df['cluster_identifier'].nunique()
            # Count unique instance identifiers for each type
            aurora_instances = df[df['platform_type'] == 'Aurora']['dbinstance_identifier'].nunique()
            rds_instances = df[df['platform_type'] == 'RDS']['dbinstance_identifier'].nunique()
            serverless_instances = df[df['is_serverless'] == True]['dbinstance_identifier'].nunique()
            
            # Print summary statistics            
            print("\nSummary Statistics:")
            print(f"Total Clusters: {total_clusters}")
            print(f"Aurora PostgreSQL Instances: {aurora_instances}")
            print(f"RDS PostgreSQL Instances: {rds_instances}")
            print(f"Aurora Serverless v2 Instances: {serverless_instances}")
            
            # Additional logging for debugging
            logger.debug("\nDetailed instance information:")
            for _, row in df.iterrows():
                logger.debug(f"Cluster: {row['cluster_identifier']}, Instance: {row['dbinstance_identifier']}, Engine: {row['engine']}, Serverless: {row['is_serverless']}")
            
        else:
            logger.warning(f"No metrics found for specified PostgreSQL cluster(s) in {AWS_REGION}")

    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        if args.debug:
            logger.exception("Detailed error traceback:")
        sys.exit(1)

if __name__ == "__main__":
    main()