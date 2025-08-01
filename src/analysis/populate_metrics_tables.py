import boto3
import time
import argparse
import logging
import re
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AWSCredentialsManager:
    """Manages AWS credentials and client creation"""
    
    def __init__(self, account_id: str, region: str):
        """
        Initialize AWS Credentials Manager
        
        Args:
            account_id (str): AWS account ID where PostgresMetricsUploader role exists
            region (str): AWS region where central resources are located
        """
        self.account_id = account_id
        self.region = region
        self.credentials = None
        
    def assume_role(self):
        """Assume PostgresMetricsUploader role"""
        try:
            sts_client = boto3.client('sts', region_name=self.region)
            response = sts_client.assume_role(
                RoleArn=f'arn:aws:iam::{self.account_id}:role/PostgresMetricsUploader',
                RoleSessionName='AthenaMetricsSession'
            )
            self.credentials = response['Credentials']
            logger.info(f"Successfully assumed PostgresMetricsUploader role in account {self.account_id} in region {self.region}")
        except Exception as e:
            logger.warning(f"Error assuming PostgresMetricsUploader role: {str(e)}")
            raise

    def get_client(self, service_name: str, region: str = None) -> Any:
        """
        Get boto3 client with assumed role credentials
        
        Args:
            service_name (str): AWS service name (e.g., 's3', 'athena')
            region (str, optional): AWS region. If not provided, uses the region specified during initialization
            
        Returns:
            boto3.client: AWS service client with assumed role credentials
        """
        if not self.credentials:
            self.assume_role()
            
        return boto3.client(
            service_name,
            region_name=region or self.region,
            aws_access_key_id=self.credentials['AccessKeyId'],
            aws_secret_access_key=self.credentials['SecretAccessKey'],
            aws_session_token=self.credentials['SessionToken']
        )

@dataclass
class TableConfig:
    """Configuration for a table including its properties and location"""
    name: str
    location: str
    type: str  # 'raw', 'iceberg', or 'view'

class ConfigManager:
    """Manages configuration and constants for the analytics setup"""
    
    def __init__(self, account_id: str, region: str):
        self.account_id = account_id
        self.region = region
        self.database = "serverless_migration_db"
        self.s3_bucket = f"postgres-cw-metrics-central-{account_id}"

    def get_s3_locations(self) -> Dict[str, str]:
        """Get S3 locations for different components"""
        base = f"s3://{self.s3_bucket}/cloudwatch_detail_metrics"
        return {
            'raw': f"{base}/raw/",
            'iceberg': f"{base}/iceberg/",
            'athena_results': f"s3://{self.s3_bucket}/athena_query_results/"
        }

    def get_table_configs(self) -> Dict[str, TableConfig]:
        """Get configurations for all tables"""
        locations = self.get_s3_locations()
        return {
            'raw': TableConfig(
                name="serverless_migration_metrics_raw",
                location=locations['raw'],
                type='raw'
            ),
            'iceberg': TableConfig(
                name="serverless_migration_metrics_iceberg",
                location=locations['iceberg'],
                type='iceberg'
            ),
            'latest_view': TableConfig(
                name="serverless_migration_metrics_iceberg_latest_run",
                location="",  # Views don't have locations
                type='view'
            )
        }

class QueryBuilder:
    """Builds SQL queries for table operations with enhanced input validation and query construction"""

    def __init__(self, database: str, table_configs: Dict[str, TableConfig]):
        self.database = self._sanitize_identifier(database)
        self.configs = self._validate_configs(table_configs)
        self.column_definitions = self.get_column_definitions()

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        """Sanitize database, table and column identifiers"""
        if not identifier or not isinstance(identifier, str):
            raise ValueError("Identifier must be a non-empty string")
        
        # Allow only alphanumeric characters and underscores
        if not re.match(r'^[a-zA-Z0-9_]+$', identifier):
            raise ValueError(f"Invalid identifier '{identifier}'. Only alphanumeric characters and underscores are allowed.")
        return identifier

    @staticmethod
    def _sanitize_s3_location(location: str) -> str:
        """Sanitize S3 location"""
        if not location or not isinstance(location, str):
            raise ValueError("S3 location must be a non-empty string")
            
        # Validate S3 path format
        if not re.match(r'^s3://[a-zA-Z0-9\-\._/]+$', location):
            raise ValueError(f"Invalid S3 location: {location}")
        return location

    def _validate_configs(self, configs: Dict[str, TableConfig]) -> Dict[str, TableConfig]:
        """Validate table configurations"""
        validated_configs = {}
        for key, config in configs.items():
            if not isinstance(config, TableConfig):
                raise ValueError(f"Invalid configuration type for {key}")
            
            validated_configs[key] = TableConfig(
                name=self._sanitize_identifier(config.name),
                location=self._sanitize_s3_location(config.location) if config.location else "",
                type=config.type
            )
        return validated_configs

    def _get_table_name(self, config_key: str) -> str:
        """Get sanitized table name from config"""
        if config_key not in self.configs:
            raise ValueError(f"Invalid config key: {config_key}")
        return self.configs[config_key].name

    def _build_query(self, query_parts: List[str]) -> str:
        """Safely build a query from parts"""
        return "\n".join(query_parts)

    def get_column_definitions(self) -> str:
        """Get column definitions for tables"""
        columns = [
            "exec_timestamp_utc timestamp",
            "run_date_local date",
            "start_date_utc date",
            "end_date_utc date",
            "aws_account_id string",
            "aws_region string",
            "cluster_identifier string",
            "dbinstance_identifier string",
            "dbinstance_class string",
            "engine string",
            "engine_version string",
            "storage_type string",
            "deployment_option string",
            "dbinstance_status string",
            "vcpu int",
            "memory_gib double",
            "is_serverless boolean",
            "acu_price_per_hour double",
            "min_acu double",
            "max_acu double",
            "serverless_min_hourly_cost double",
            "serverless_max_hourly_cost double",
            "serverless_min_monthly_cost double",
            "serverless_max_monthly_cost double",
            "on_demand_hourly_rate double",
            "on_demand_monthly_estimate double",
            "ri_1yr_no_upfront_hourly double",
            "ri_1yr_no_upfront_monthly double",
            "sample_period_days int",
            "observation_days int",
            "utc_hour string",
            "platform_type string",
            "avg_cpu_utilization double",
            "max_cpu_utilization double",
            "p95_cpu_utilization double",
            "asv2_migration_path string",
            "growth_capacity_factor double",
            "vcpu_utilization double",
            "actual_estimate_acu double",
            "actual_estimate_acu_price_per_hour double",
            "adjusted_estimate_acu double",
            "adjusted_estimate_acu_price_per_hour double",
            "usage_pattern string",
            "usage_pattern_notes string"
        ]
        return ", ".join(columns)

    def _get_column_list(self) -> str:
        """Get a comma-separated list of columns"""
        columns = [
            "exec_timestamp_utc", "run_date_local", "start_date_utc", "end_date_utc",
            "aws_account_id", "aws_region", "cluster_identifier", "dbinstance_identifier",
            "dbinstance_class", "engine", "engine_version", "storage_type",
            "deployment_option", "dbinstance_status", "vcpu", "memory_gib",
            "is_serverless", "acu_price_per_hour", "min_acu", "max_acu",
            "serverless_min_hourly_cost", "serverless_max_hourly_cost",
            "serverless_min_monthly_cost", "serverless_max_monthly_cost",
            "on_demand_hourly_rate", "on_demand_monthly_estimate",
            "ri_1yr_no_upfront_hourly", "ri_1yr_no_upfront_monthly",
            "sample_period_days", "observation_days", "utc_hour", "platform_type",
            "avg_cpu_utilization", "max_cpu_utilization", "p95_cpu_utilization",
            "asv2_migration_path", "growth_capacity_factor", "vcpu_utilization",
            "actual_estimate_acu", "actual_estimate_acu_price_per_hour",
            "adjusted_estimate_acu", "adjusted_estimate_acu_price_per_hour",
            "usage_pattern", "usage_pattern_notes"
        ]
        return ", ".join(columns)

    def _get_column_list_with_alias(self, alias: str) -> str:
        """Get a comma-separated list of columns with alias"""
        sanitized_alias = self._sanitize_identifier(alias)
        columns = self._get_column_list().split(", ")
        return ", ".join([f"{sanitized_alias}.{col}" for col in columns])

    def _get_merge_conditions(self) -> str:
        """Get the merge conditions"""
        conditions = [
            "t.exec_timestamp_utc = s.exec_timestamp_utc",
            "t.run_date_local = s.run_date_local",
            "t.aws_region = s.aws_region",
            "t.cluster_identifier = s.cluster_identifier",
            "t.dbinstance_identifier = s.dbinstance_identifier",
            "t.utc_hour = s.utc_hour"
        ]
        return " AND ".join(conditions)

    def _get_update_set_clause(self) -> str:
        """Get the update set clause"""
        columns = self._get_column_list().split(", ")
        # Exclude key columns from update
        update_columns = [col for col in columns if col not in 
                         ["exec_timestamp_utc", "run_date_local", "aws_region", 
                          "cluster_identifier", "dbinstance_identifier", "utc_hour"]]
        return ", ".join([f"{col} = s.{col}" for col in update_columns])

    def _get_insert_clause(self) -> str:
        """Get the insert clause"""
        columns = self._get_column_list().split(", ")
        return f"({', '.join(columns)}) VALUES ({', '.join(['s.' + col for col in columns])})"

    def get_create_database_query(self) -> str:
        """Get query to create the database"""
        db = self._sanitize_identifier(self.database)
        return f"CREATE DATABASE IF NOT EXISTS {db}"

    def get_create_raw_table_query(self) -> str:
        """Get query to create the raw table"""
        db = self._sanitize_identifier(self.database)
        config = self.configs['raw']
        table = self._sanitize_identifier(config.name)
        location = self._sanitize_s3_location(config.location)

        query_parts = [
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}",
            f"({self.column_definitions})",
            "ROW FORMAT DELIMITED",
            "FIELDS TERMINATED BY ','",
            "STORED AS TEXTFILE",
            f"LOCATION '{location}'",
            "TBLPROPERTIES ('skip.header.line.count'='1')"
        ]
        
        return self._build_query(query_parts)

    def get_create_iceberg_table_query(self) -> str:
        """Get query to create the Iceberg table"""
        db = self._sanitize_identifier(self.database)
        config = self.configs['iceberg']
        table = self._sanitize_identifier(config.name)
        location = self._sanitize_s3_location(config.location)

        query_parts = [
            f"CREATE TABLE IF NOT EXISTS {db}.{table}",
            f"({self.column_definitions})",
            "PARTITIONED BY (run_date_local, aws_region, cluster_identifier)",
            f"LOCATION '{location}'",
            "TBLPROPERTIES ( ",
            "    'format' = 'PARQUET',",
            "    'table_type' = 'ICEBERG',",
            "    'write_compression' = 'SNAPPY')"
    
        ]
        
        return self._build_query(query_parts)

    def get_merge_query(self) -> str:
        """Get query to merge data from raw to Iceberg table"""
        db = self._sanitize_identifier(self.database)
        iceberg_table = self._sanitize_identifier(self._get_table_name('iceberg'))
        raw_table = self._sanitize_identifier(self._get_table_name('raw'))

        query_parts = [
            f"MERGE INTO {db}.{iceberg_table} AS t",
            "USING (",
            "    SELECT",
            "    " + self._get_column_list(),
            f"    FROM {db}.{raw_table}",
            ") AS s",
            "ON " + self._get_merge_conditions(),
            "WHEN MATCHED THEN",
            "    UPDATE SET",
            "    " + self._get_update_set_clause(),
            "WHEN NOT MATCHED THEN",
            "    INSERT " + self._get_insert_clause()
        ]
        
        return self._build_query(query_parts)

    def get_latest_view_query(self) -> str:
        """Get query to create the latest run view"""
        db = self._sanitize_identifier(self.database)
        latest_view = self._sanitize_identifier(self._get_table_name('latest_view'))
        iceberg_table = self._sanitize_identifier(self._get_table_name('iceberg'))

        query_parts = [
            f"CREATE OR REPLACE VIEW {db}.{latest_view} AS",
            "WITH latest_runs AS (",
            "    SELECT",
            "        aws_account_id,",
            "        cluster_identifier,",
            "        MAX(exec_timestamp_utc) as latest_exec_timestamp",
            f"    FROM {db}.{iceberg_table}",
            "    GROUP BY",
            "        aws_account_id,",
            "        cluster_identifier",
            ")",
            "SELECT",
            "    " + self._get_column_list_with_alias('i'),
            f"FROM {db}.{iceberg_table} i",
            "JOIN latest_runs l",
            "    ON i.aws_account_id = l.aws_account_id",
            "    AND i.cluster_identifier = l.cluster_identifier",
            "    AND i.exec_timestamp_utc = l.latest_exec_timestamp"
        ]
        
        return self._build_query(query_parts)

    def get_migration_guidance_summary_view_query(self) -> str:
        """Get query to create the migration guidance summary view"""
        db = self._sanitize_identifier(self.database)
        latest_view = self._sanitize_identifier(self._get_table_name('latest_view'))

        query_parts = [
            f"CREATE OR REPLACE VIEW {db}.serverless_migration_guidance_summary AS",
            "WITH metrics_summary AS (",
            "    SELECT",
            "        aws_account_id,",
            "        aws_region,",
            "        cluster_identifier,",
            "        dbinstance_identifier,",
            "        platform_type,",
            "        asv2_migration_path,",
            "        usage_pattern,",
            "        usage_pattern_notes,",
            "        COUNT(utc_hour) as sample_hours,",
            "        MAX(sample_period_days) as sample_period_days,",
            "        MAX(observation_days) as observation_days,",
            "        MAX(vcpu) as source_provisioned_vcpu,",
            "        MAX(memory_gib) as source_provisioned_memory_gib,",
            "        AVG(p95_cpu_utilization) as avg_p95_cpu,",
            "        SUM(on_demand_hourly_rate) as on_demand_provisioned_usd_cost_daily,",
            "        MIN(adjusted_estimate_acu) as min_adjusted_acu,",
            "        MAX(adjusted_estimate_acu) as max_adjusted_acu,",
            "        SUM(adjusted_estimate_acu_price_per_hour) as adjusted_serverless_usd_cost_daily",
            f"    FROM {db}.{latest_view}",
            "    WHERE dbinstance_class <> 'db.serverless'",
            "    GROUP BY",
            "        aws_account_id,",
            "        aws_region,",
            "        cluster_identifier,",
            "        dbinstance_identifier,",
            "        platform_type,",
            "        asv2_migration_path,",
            "        usage_pattern,",
            "        usage_pattern_notes",
            ")",
            "SELECT",
            "    *,",
            "    max_adjusted_acu * 2 as target_serverless_memory_gib,",
            "    ROUND(on_demand_provisioned_usd_cost_daily - adjusted_serverless_usd_cost_daily, 2) as estimated_usd_cost_diff_daily,",
            "    CONCAT(",
            "        CAST(ROUND(",
            "            ((on_demand_provisioned_usd_cost_daily - adjusted_serverless_usd_cost_daily) /",
            "            NULLIF(on_demand_provisioned_usd_cost_daily, 0)) * 100,",
            "            2",
            "        ) AS VARCHAR),",
            "        CASE",
            "            WHEN ((on_demand_provisioned_usd_cost_daily - adjusted_serverless_usd_cost_daily) /",
            "                NULLIF(on_demand_provisioned_usd_cost_daily, 0)) >= 0",
            "            THEN '% cost savings'",
            "            ELSE '% cost increase'",
            "        END",
            "    ) as estimated_pct_savings_daily,",
            "    CASE",
            "        WHEN ((on_demand_provisioned_usd_cost_daily - adjusted_serverless_usd_cost_daily) /",
            "            NULLIF(on_demand_provisioned_usd_cost_daily, 0)) * 100 > 20",
            "            AND source_provisioned_memory_gib >= 16",
            "            AND avg_p95_cpu <= 30",
            "                THEN 'Excellent Candidate - Estimated more than 20% cost savings and low instance utilization'",
            "        WHEN ((on_demand_provisioned_usd_cost_daily - adjusted_serverless_usd_cost_daily) /",
            "            NULLIF(on_demand_provisioned_usd_cost_daily, 0)) * 100 > 0",
            "        THEN 'Good Candidate based on estimated cost savings'",
            "        WHEN LOWER(dbinstance_identifier) LIKE '%dev%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%development%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%sb%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%sandbox%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%corp%'",
            "        THEN 'Requires further evaluation - Consider cluster consolidation, retirement, or migration to Aurora Serverless v2 using auto pause (minACU=0) or setting minACU to 0.5'",
            "        WHEN LOWER(dbinstance_identifier) LIKE '%qa%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%staging%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%stage%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%uat%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%test%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%stg%'",
            "            OR LOWER(dbinstance_identifier) LIKE '%nonprod%'",
            "        THEN 'Requires further evaluation - Consider evaluating operational benefits and modernization goals against increase in cost'",
            "        ELSE 'Requires further evaluation - Consider using reserved instances and performing cluster/workload optimization'",
            "    END as asv2_suitability_guidance",
            "FROM metrics_summary",
            "ORDER BY",
            "    aws_account_id,",
            "    aws_region,",
            "    cluster_identifier,",
            "    dbinstance_identifier"
        ]
        
        return self._build_query(query_parts)

    def get_test_queries(self) -> List[str]:
        """Get test queries to validate the setup"""
        db = self._sanitize_identifier(self.database)
        iceberg_table = self._sanitize_identifier(self._get_table_name('iceberg'))
        latest_view = self._sanitize_identifier(self._get_table_name('latest_view'))
        
        queries = []
        
        # Test basic aggregations
        queries.append(self._build_query([
            "SELECT",
            "    run_date_local,",
            "    cluster_identifier,",
            "    aws_region,",
            "    COUNT(DISTINCT dbinstance_identifier) as instance_count,",
            "    ROUND(AVG(avg_cpu_utilization), 2) as avg_cpu,",
            "    ROUND(MAX(max_cpu_utilization), 2) as max_cpu,",
            "    ROUND(AVG(p95_cpu_utilization), 2) as p95_cpu",
            f"FROM {db}.{iceberg_table}",
            "GROUP BY",
            "    run_date_local,",
            "    cluster_identifier,",
            "    aws_region",
            "ORDER BY",
            "    run_date_local DESC,",
            "    cluster_identifier"
        ]))
        
        # Test latest view
        queries.append(self._build_query([
            "SELECT",
            "    cluster_identifier,",
            "    COUNT(DISTINCT dbinstance_identifier) as instance_count,",
            "    MIN(exec_timestamp_utc) as exec_timestamp,",
            "    ROUND(AVG(avg_cpu_utilization), 2) as avg_cpu",
            f"FROM {db}.{latest_view}",
            "GROUP BY",
            "    cluster_identifier"
        ]))
        
        # Test for duplicates
        queries.append(self._build_query([
            "SELECT",
            "    run_date_local,",
            "    aws_region,",
            "    cluster_identifier,",
            "    dbinstance_identifier,",
            "    utc_hour,",
            "    COUNT(*) as record_count",
            f"FROM {db}.{iceberg_table}",
            "GROUP BY",
            "    run_date_local,",
            "    aws_region,",
            "    cluster_identifier,",
            "    dbinstance_identifier,",
            "    utc_hour",
            "HAVING COUNT(*) > 1"
        ]))
        
        return queries

class AthenaTableManager:
    """Manages Athena table operations"""

    def __init__(self, region: str, database: str, table_configs: Dict[str, TableConfig], 
                 query_builder: QueryBuilder, athena_results_location: str,
                 credentials_manager: AWSCredentialsManager):
        self.credentials_manager = credentials_manager  
        self.region = region            
        self.database = database
        self.configs = table_configs
        self.query_builder = query_builder
        self.results_location = athena_results_location
        self.client = self.credentials_manager.get_client('athena', region=self.region)        

    def run_query(self, query: str) -> bool:
        """Execute an Athena query and wait for completion"""
        try:
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.results_location}
            )
            
            query_execution_id = response['QueryExecutionId']

            start_time = time.time()
            timeout = 10  # 10 seconds timeout
            poll_interval = 5  # Check every 5 seconds

            while time.time() - start_time < timeout:
                query_details = self.client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )

                query_status = query_details['QueryExecution']['Status']['State']
            
                if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    if query_status == 'SUCCEEDED':
                        logger.info(f"Query completed successfully")
                        return True
                    else:
                        error_details = query_details['QueryExecution']['Status'].get('StateChangeReason', 'No error message provided')
                        logger.warning(f"Query failed with status {query_status}. Error: {error_details}")
                        return False
                    
                logger.debug(f"Query is still running with status {query_status}, waiting...")            

            logger.error(f"Query execution timed out after {timeout} seconds")
            try:
                # Try to cancel the query if it's still running
                self.client.stop_query_execution(
                    QueryExecutionId=query_execution_id
                )
                logger.info(f"Cancelled timed out query {query_execution_id}")
            except Exception as cancel_error:
                logger.warning(f"Failed to cancel query {query_execution_id}: {str(cancel_error)}")
        
            return False

        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return False

    def check_database_exists(self) -> bool:
        """Check if the database exists"""
        try:
            glue_client = self.credentials_manager.get_client('glue', region=self.region)   

            try:
                glue_client.get_database(Name=self.database)
                logger.info(f"Database {self.database} exists in AwsDataCatalog")
                return True

            except glue_client.exceptions.EntityNotFoundException:
                logger.info(f"Database {self.database} does not exist")
                return False    
    
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDeniedException':
                logger.error(f"Access denied checking database existence: {str(e)}")
                return False
            else:
                logger.error(f"Error checking database existence: {str(e)}")
                return False

    def check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            glue_client = self.credentials_manager.get_client('glue', region=self.region)
        
            try:
                glue_client.get_table(
                    DatabaseName=self.database,
                    Name=table_name
                )
                logger.info(f"Table {table_name} exists in database {self.database} in AwsDataCatalog")
                return True
            
            except glue_client.exceptions.EntityNotFoundException:
                logger.info(f"Table {table_name} does not exist in database {self.database} in AwsDataCatalog")
                return False
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDeniedException':
                logger.error(f"Access denied checking table existence: {str(e)}")
                raise
            else:
                logger.error(f"Error checking table existence: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error checking table existence: {str(e)}")
            raise

    def setup_database(self) -> bool:
        """Create database if it doesn't exist"""
        if not self.check_database_exists():
            query = self.query_builder.get_create_database_query()
            return self.run_query(query)
        return True

    def delete_table_and_data(self, table_name: str) -> bool:
        """Delete table and its data"""
        try:
            glue_client = self.credentials_manager.get_client('glue', region=self.region)
        
            logger.info(f"Deleting table {table_name} using glue API")
            try:
                try:
                    table_response = glue_client.get_table(
                        DatabaseName=self.database,
                        Name=table_name
                    )
                except glue_client.exceptions.EntityNotFoundException:
                    logger.info(f"Table {table_name} does not exist in Glue catalog")
                    return True    
                                
                # Delete the table and its data
                glue_client.delete_table(
                    DatabaseName=self.database,
                    Name=table_name
                )
                logger.info(f"Successfully deleted table {table_name} and its data")

                start_time = time.time()
                timeout = 10  # 10 seconds timeout
                poll_interval = 2  # Check every 2 seconds

                while time.time() - start_time < timeout:
                    try:
                        glue_client.get_table(
                            DatabaseName=self.database,
                            Name=table_name
                        )
                        logger.info(f"Table {table_name} still exists, waiting...")
                    except glue_client.exceptions.EntityNotFoundException:
                        logger.info(f"Confirmed table {table_name} has been deleted")
                        return True

                logger.warning(f"Timed out waiting for table {table_name} to be deleted")
                return False
            
            except glue_client.exceptions.EntityNotFoundException:
                logger.info(f"Table {table_name} does not exist in Glue catalog")
                return True
            
        except Exception as e:
            logger.error(f"Error deleting table {table_name}: {str(e)}")
            return False

    def setup_tables(self, restatement: bool = False) -> bool:
        """Set up all required tables"""
        try:
            # Create database if needed
            if not self.setup_database():
                return False

            # Create or replace tables
            for config in self.configs.values():
                if config.type == 'view':
                    continue  # Skip views for now
                    
                if restatement:
                    # Always drop the table if it exists during restatement
                    logger.info(f"Dropping table {config.name} for restatement")
                    if not self.delete_table_and_data(config.name):
                        return False

                logger.info(f"Creating table {config.name}")
                query = (self.query_builder.get_create_raw_table_query() 
                        if config.type == 'raw' 
                        else self.query_builder.get_create_iceberg_table_query())
                
                if not self.run_query(query):
                    return False

            # Merge data into Iceberg table
            logger.info("Merging data into Iceberg table")
            if not self.run_query(self.query_builder.get_merge_query()):
                return False

            # Create latest view
            logger.info("Creating latest run view")
            if not self.run_query(self.query_builder.get_latest_view_query()):
                return False

            # Create migration guidance summary view
            logger.info("Creating migration guidance summary view")
            if not self.run_query(self.query_builder.get_migration_guidance_summary_view_query()):
                return False                

            return True

        except Exception as e:
            logger.error(f"Error in table setup: {str(e)}")
            return False

    def run_test_queries(self) -> bool:
        """Run test queries to validate the setup"""
        try:
            all_successful = True
            for i, query in enumerate(self.query_builder.get_test_queries(), 1):
                logger.info(f"Running test query {i}")
                if not self.run_query(query):
                    logger.error(f"Test query {i} failed")
                    all_successful = False
            return all_successful
        except Exception as e:
            logger.error(f"Error running test queries: {str(e)}")
            return False

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Create Athena tables for Aurora migration analytics')
    parser.add_argument('--central-account-id', required=True,
                       help='AWS account ID where the central S3 bucket is located')
    parser.add_argument('--region', required=True,
                       help='AWS region where central resources are located')                       
    parser.add_argument('--table-restatement', action='store_true',
                       help='Drop and recreate tables if they exist')
    parser.add_argument('--run-test-queries', action='store_true',
                       help='Execute test queries after table creation')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize configuration
        config_manager = ConfigManager(args.central_account_id, args.region)
        table_configs = config_manager.get_table_configs()
        s3_locations = config_manager.get_s3_locations()

        # Create credentials manager with same region
        credentials_manager = AWSCredentialsManager(args.central_account_id, args.region)        
        
        # Initialize components
        query_builder = QueryBuilder(config_manager.database, table_configs)
        table_manager = AthenaTableManager(
            region=config_manager.region,
            database=config_manager.database,
            table_configs=table_configs,
            query_builder=query_builder,
            athena_results_location=s3_locations['athena_results'],
            credentials_manager=credentials_manager
        )
        
        # Setup tables
        logger.info("Setting up tables...")
        if not table_manager.setup_tables(args.table_restatement):
            raise Exception("Table setup failed")
        
        # Run test queries if requested
        if args.run_test_queries:
            logger.info("Running test queries...")
            if not table_manager.run_test_queries():
                raise Exception("Test queries failed")
        
        logger.info("Setup completed successfully")
        
    except Exception as e:
        logger.warning(f"Setup failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()