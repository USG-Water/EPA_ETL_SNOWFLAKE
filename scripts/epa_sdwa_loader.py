#!/usr/bin/env python3
"""
EPA ECHO SDWA (Safe Drinking Water Act) Data Download and Snowflake ETL Script

This script downloads EPA drinking water data from the ECHO system and loads it into Snowflake
using key pair authentication. The SDWA dataset contains multiple CSV files with data about
public water systems, violations, enforcement actions, and reference tables.

This version is designed for automated execution via GitHub Actions with incremental loading.
"""

import os
import sys
import logging
import zipfile
import tempfile
import shutil
from typing import Dict, List, Optional
from datetime import datetime
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce Snowflake connector verbosity
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.ocsp_snowflake').setLevel(logging.ERROR)
logging.getLogger('snowflake.connector.vendored.urllib3').setLevel(logging.WARNING)

class EPASDWADownloader:
    """Downloads and processes EPA SDWA data from ECHO system."""
    
    def __init__(self, download_dir: str = None):
        self.download_dir = download_dir or tempfile.mkdtemp(prefix="epa_data_")
        self.base_url = "https://echo.epa.gov/files/echodownloads/"
        self.possible_filenames = [
            "sdwa_downloads.zip",
            "ECHO_SDWA.zip", 
            "sdwa_dataset.zip",
            "SDWA_dataset.zip"
        ]
        
        os.makedirs(self.download_dir, exist_ok=True)
        logger.info(f"Using download directory: {self.download_dir}")
    
    def download_sdwa_data(self) -> str:
        """Download the EPA SDWA zip file."""
        logger.info("Searching for EPA SDWA dataset...")
        
        # Try to find the exact download URL from the data downloads page
        try:
            import re
            response = requests.get("https://echo.epa.gov/tools/data-downloads")
            if response.status_code == 200:
                sdwa_links = re.findall(r'href="([^"]*(?:sdwa|SDWA)[^"]*\.zip)"', 
                                       response.text, re.IGNORECASE)
                if sdwa_links:
                    download_url = sdwa_links[0]
                    if not download_url.startswith('http'):
                        download_url = f"https://echo.epa.gov{download_url}"
                    
                    logger.info(f"Found SDWA download URL: {download_url}")
                    
                    zip_filename = download_url.split('/')[-1]
                    zip_path = os.path.join(self.download_dir, zip_filename)
                    
                    logger.info(f"Downloading SDWA data from {download_url}")
                    
                    response = requests.get(download_url, stream=True)
                    response.raise_for_status()
                    
                    with open(zip_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    logger.info(f"Successfully downloaded {zip_path}")
                    return zip_path
        except Exception as e:
            logger.warning(f"Could not find SDWA URL from main page: {e}")
        
        # Fallback: try common filename patterns
        for filename in self.possible_filenames:
            download_url = f"{self.base_url}{filename}"
            zip_path = os.path.join(self.download_dir, filename)
            
            logger.info(f"Trying download URL: {download_url}")
            
            try:
                response = requests.get(download_url, stream=True)
                response.raise_for_status()
                
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Successfully downloaded {zip_path}")
                return zip_path
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to download from {download_url}: {e}")
                continue
        
        raise Exception(
            "Could not download SDWA data. Please check EPA ECHO data downloads page."
        )
    
    def extract_csv_files(self, zip_path: str) -> List[str]:
        """Extract CSV files from the downloaded zip file."""
        extract_dir = os.path.join(self.download_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        
        csv_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                logger.info(f"Found {len(file_list)} files in zip archive")
                
                for file_name in file_list:
                    if file_name.lower().endswith('.csv'):
                        zip_ref.extract(file_name, extract_dir)
                        csv_path = os.path.join(extract_dir, file_name)
                        csv_files.append(csv_path)
                        logger.info(f"Extracted: {file_name}")
                
            logger.info(f"Extracted {len(csv_files)} CSV files")
            return csv_files
            
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract zip file: {e}")
            raise
    
    def cleanup(self):
        """Clean up temporary download directory."""
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)
            logger.info(f"Cleaned up temporary directory: {self.download_dir}")

class SnowflakeKeyPairLoader:
    """Handles loading data into Snowflake using key pair authentication."""
    
    def __init__(self):
        """Initialize Snowflake connection parameters from environment variables."""
        # Get configuration from environment variables
        self.account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.user = os.environ.get('SNOWFLAKE_USER')
        self.warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
        self.database = os.environ.get('SNOWFLAKE_DATABASE')
        self.schema = os.environ.get('SNOWFLAKE_SCHEMA')
        self.role = os.environ.get('SNOWFLAKE_ROLE')
        
        # Get private key from environment
        private_key_str = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
        
        if not all([self.account, self.user, self.warehouse, self.database, 
                   self.schema, self.role, private_key_str]):
            missing = []
            if not self.account: missing.append('SNOWFLAKE_ACCOUNT')
            if not self.user: missing.append('SNOWFLAKE_USER')
            if not self.warehouse: missing.append('SNOWFLAKE_WAREHOUSE')
            if not self.database: missing.append('SNOWFLAKE_DATABASE')
            if not self.schema: missing.append('SNOWFLAKE_SCHEMA')
            if not self.role: missing.append('SNOWFLAKE_ROLE')
            if not private_key_str: missing.append('SNOWFLAKE_PRIVATE_KEY')
            
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Parse the private key
        self.private_key = self._parse_private_key(private_key_str)
        self.conn = None
        
        logger.info(f"Initialized Snowflake config - Account: {self.account}, User: {self.user}")
    
    def _parse_private_key(self, private_key_str: str):
        """Parse the private key from string."""
        try:
            # Handle multiline key properly
            private_key_bytes = private_key_str.encode('utf-8')
            
            # Check if passphrase is provided
            passphrase = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
            if passphrase:
                private_key = serialization.load_pem_private_key(
                    private_key_bytes,
                    password=passphrase.encode('utf-8'),
                    backend=default_backend()
                )
            else:
                private_key = serialization.load_pem_private_key(
                    private_key_bytes,
                    password=None,
                    backend=default_backend()
                )
            
            # Convert to DER format for Snowflake
            private_key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            return private_key_der
            
        except Exception as e:
            logger.error(f"Failed to parse private key: {e}")
            raise
    
    def connect(self):
        """Establish connection to Snowflake using key pair authentication."""
        try:
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                private_key=self.private_key,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role
            )
            
            logger.info("Successfully connected to Snowflake using key pair authentication")
            
            # Set context
            cursor = self.conn.cursor()
            cursor.execute(f"USE WAREHOUSE {self.warehouse}")
            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            cursor.execute(f"USE ROLE {self.role}")
            cursor.close()
            
            logger.info(f"Context set: {self.database}.{self.schema}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in Snowflake."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.schema.upper()}' 
                  AND TABLE_NAME = '{table_name.upper()}'
            """)
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def create_or_alter_table(self, df: pd.DataFrame, table_name: str) -> str:
        """Create table if not exists, or verify columns if it does."""
        table_name = table_name.upper()
        
        # Check if table exists
        if self.table_exists(table_name):
            logger.info(f"Table {table_name} already exists - verifying columns")
            
            cursor = self.conn.cursor()
            
            # Get existing columns
            cursor.execute(f"DESCRIBE TABLE \"{table_name}\"")
            existing_columns = {row[0].upper(): row[1] for row in cursor.fetchall()}
            
            # Check if metadata columns exist
            required_columns = ['LOAD_ID', 'LOAD_TIMESTAMP', 'LOAD_DATE']
            missing_columns = [col for col in required_columns if col not in existing_columns]
            
            if missing_columns:
                logger.info(f"Missing columns in {table_name}: {missing_columns}")
                
                # Add columns without default values to avoid syntax errors
                if 'LOAD_ID' in missing_columns:
                    logger.info(f"Adding LOAD_ID column to {table_name}")
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "LOAD_ID" STRING')
                
                if 'LOAD_TIMESTAMP' in missing_columns:
                    logger.info(f"Adding LOAD_TIMESTAMP column to {table_name}")
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "LOAD_TIMESTAMP" TIMESTAMP')
                
                if 'LOAD_DATE' in missing_columns:
                    logger.info(f"Adding LOAD_DATE column to {table_name}")
                    cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "LOAD_DATE" DATE')
            else:
                logger.info(f"All required columns exist in {table_name}")
            
            cursor.close()
            return "EXISTS"
        
        # Create new table with all columns as STRING for flexibility
        columns = []
        for col_name in df.columns:
            clean_col_name = col_name.replace(' ', '_').replace('-', '_').upper()
            columns.append(f'"{clean_col_name}" STRING')
        
        ddl = f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {', '.join(columns)},
            "LOAD_TIMESTAMP" TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            "LOAD_DATE" DATE DEFAULT CURRENT_DATE(),
            "LOAD_ID" STRING
        );
        '''
        
        cursor = self.conn.cursor()
        cursor.execute(ddl)
        cursor.close()
        
        logger.info(f"Created table: {table_name}")
        return "CREATED"
    
    def load_csv_to_table(self, csv_path: str, table_name: str, 
                         chunk_size: int = 10000, load_id: str = None) -> int:
        """
        Load CSV data into Snowflake table (append mode with deduplication support).
        """
        table_name = table_name.upper()
        total_rows = 0
        load_id = load_id or datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # First, get a sample to understand structure
            df_sample = pd.read_csv(csv_path, nrows=1000, dtype=str, na_filter=False)
            
            # Create or check table
            status = self.create_or_alter_table(df_sample, table_name)
            
            # Read and process CSV in chunks
            chunk_count = 0
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str, na_filter=False):
                chunk_count += 1
                logger.info(f"Processing chunk {chunk_count} of {table_name}")
                
                # Clean column names
                chunk.columns = [col.replace(' ', '_').replace('-', '_').upper() 
                               for col in chunk.columns]
                
                # Add metadata columns
                chunk['LOAD_TIMESTAMP'] = datetime.now()
                chunk['LOAD_DATE'] = datetime.now().date()
                chunk['LOAD_ID'] = load_id
                
                # Replace 'nan' strings with None
                chunk = chunk.replace('nan', None)
                
                # Write to Snowflake
                success, nchunks, nrows, _ = write_pandas(
                    self.conn, 
                    chunk, 
                    table_name,
                    auto_create_table=False,
                    quote_identifiers=True
                )
                
                if success:
                    total_rows += len(chunk)
                    logger.info(f"Loaded {len(chunk)} rows to {table_name} (Total: {total_rows})")
                else:
                    logger.error(f"Failed to load chunk {chunk_count} to {table_name}")
                    
        except Exception as e:
            logger.error(f"Error loading {csv_path} to {table_name}: {e}")
            raise
        
        logger.info(f"Total rows loaded to {table_name}: {total_rows}")
        return total_rows
    
    def deduplicate_table(self, table_name: str):
        """Remove duplicate rows keeping only the latest load."""
        table_name = table_name.upper()
        
        try:
            # Create a temporary table with deduplicated data
            dedupe_sql = f"""
            CREATE OR REPLACE TABLE "{table_name}_TEMP" AS
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {self._get_dedup_columns(table_name)}
                        ORDER BY LOAD_TIMESTAMP DESC
                    ) AS rn
                FROM "{table_name}"
            ) WHERE rn = 1;
            
            ALTER TABLE "{table_name}" RENAME TO "{table_name}_BACKUP_{datetime.now().strftime('%Y%m%d')}";
            ALTER TABLE "{table_name}_TEMP" RENAME TO "{table_name}";
            
            DROP TABLE "{table_name}_BACKUP_{datetime.now().strftime('%Y%m%d')}";
            """
            
            cursor = self.conn.cursor()
            for statement in dedupe_sql.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            cursor.close()
            
            logger.info(f"Successfully deduplicated table {table_name}")
            
        except Exception as e:
            logger.warning(f"Could not deduplicate {table_name}: {e}")
    
    def _get_dedup_columns(self, table_name: str) -> str:
        """Get columns for deduplication (all except load metadata)."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DESCRIBE TABLE \"{table_name}\"")
            columns = cursor.fetchall()
            cursor.close()
            
            # Filter out load metadata columns
            dedup_cols = [f'"{col[0]}"' for col in columns 
                         if col[0] not in ['LOAD_TIMESTAMP', 'LOAD_DATE', 'LOAD_ID', 'RN']]
            
            return ', '.join(dedup_cols) if dedup_cols else '"LOAD_ID"'
            
        except Exception as e:
            logger.warning(f"Could not get columns for deduplication: {e}")
            return '"LOAD_ID"'
    
    def get_load_summary(self) -> Dict:
        """Get summary of the latest load."""
        try:
            cursor = self.conn.cursor()
            
            # Get list of EPA tables
            cursor.execute(f"""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.schema.upper()}'
                  AND TABLE_NAME LIKE 'EPA_SDWA_%'
            """)
            tables = cursor.fetchall()
            
            summary = {}
            for (table_name,) in tables:
                try:
                    # First check if LOAD_ID column exists
                    cursor.execute(f"DESCRIBE TABLE \"{table_name}\"")
                    columns = [row[0].upper() for row in cursor.fetchall()]
                    
                    if 'LOAD_ID' in columns:
                        cursor.execute(f"""
                            SELECT 
                                COUNT(*) as total_rows,
                                MAX(LOAD_TIMESTAMP) as last_load,
                                COUNT(DISTINCT LOAD_ID) as load_count
                            FROM "{table_name}"
                        """)
                    else:
                        # If no LOAD_ID column, just get row count
                        cursor.execute(f"""
                            SELECT 
                                COUNT(*) as total_rows,
                                NULL as last_load,
                                0 as load_count
                            FROM "{table_name}"
                        """)
                    
                    result = cursor.fetchone()
                    if result:
                        summary[table_name] = {
                            'total_rows': result[0],
                            'last_load': result[1],
                            'load_count': result[2]
                        }
                except Exception as e:
                    logger.warning(f"Could not get summary for {table_name}: {e}")
                    summary[table_name] = {'error': str(e)}
            
            cursor.close()
            return summary
            
        except Exception as e:
            logger.error(f"Error getting load summary: {e}")
            return {}

def get_table_name_from_filename(filename: str) -> str:
    """Generate a clean table name from CSV filename."""
    # Remove .csv extension and clean name
    table_name = filename.replace('.csv', '').replace('.CSV', '')
    table_name = table_name.replace('-', '_').replace(' ', '_').upper()
    
    # For SDWA files, keep the EPA_SDWA prefix but avoid duplication
    if table_name.startswith('SDWA_'):
        table_name = f"EPA_{table_name}"
    elif not table_name.startswith('EPA_'):
        table_name = f"EPA_SDWA_{table_name}"
    
    return table_name

def main():
    """Main ETL process with key pair authentication."""
    
    downloader = None
    sf_loader = None
    
    try:
        # Generate unique load ID for this run
        load_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info(f"Starting EPA SDWA ETL Process - Load ID: {load_id}")
        
        # Step 1: Download EPA SDWA data
        logger.info("=== Downloading EPA SDWA Data ===")
        downloader = EPASDWADownloader()
        zip_path = downloader.download_sdwa_data()
        
        # Step 2: Extract CSV files
        csv_files = downloader.extract_csv_files(zip_path)
        
        if not csv_files:
            logger.error("No CSV files found in the downloaded archive")
            return 1
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Step 3: Connect to Snowflake
        logger.info("=== Connecting to Snowflake ===")
        sf_loader = SnowflakeKeyPairLoader()
        sf_loader.connect()
        
        # Step 4: Process each CSV file
        logger.info("=== Processing CSV Files ===")
        
        total_tables_processed = 0
        total_rows_loaded = 0
        errors = []
        
        for csv_path in csv_files:
            filename = os.path.basename(csv_path)
            table_name = get_table_name_from_filename(filename)
            
            logger.info(f"\nProcessing {filename} -> {table_name}")
            
            try:
                # Load data (append mode)
                rows_loaded = sf_loader.load_csv_to_table(
                    csv_path, 
                    table_name, 
                    load_id=load_id
                )
                
                # Skip deduplication for now - can be run separately if needed
                # if rows_loaded > 0:
                #     sf_loader.deduplicate_table(table_name)
                
                total_tables_processed += 1
                total_rows_loaded += rows_loaded
                
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                errors.append({'file': filename, 'error': str(e)})
                continue
        
        # Step 5: Get load summary
        logger.info("=== Getting Load Summary ===")
        summary = sf_loader.get_load_summary()
        
        # Step 6: Print results
        logger.info(f"\n=== ETL COMPLETION SUMMARY ===")
        logger.info(f"Load ID: {load_id}")
        logger.info(f"Tables processed: {total_tables_processed}/{len(csv_files)}")
        logger.info(f"Total rows loaded: {total_rows_loaded:,}")
        
        if errors:
            logger.warning(f"Errors encountered: {len(errors)}")
            for error in errors:
                logger.warning(f"  - {error['file']}: {error['error']}")
        
        if summary:
            logger.info("\n=== TABLE STATUS ===")
            for table_name, info in summary.items():
                logger.info(f"{table_name}:")
                logger.info(f"  Total rows: {info['total_rows']:,}")
                logger.info(f"  Last load: {info['last_load']}")
                logger.info(f"  Load count: {info['load_count']}")
        
        # Return exit code
        return 0 if not errors else 1
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        return 1
        
    finally:
        # Cleanup
        if sf_loader:
            sf_loader.disconnect()
        
        if downloader:
            downloader.cleanup()
        
        logger.info("ETL process completed")

if __name__ == "__main__":
    sys.exit(main())
