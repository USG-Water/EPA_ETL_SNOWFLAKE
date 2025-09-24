#!/usr/bin/env python3
"""
EPA FRS (Facility Registry Service) Data Download and Snowflake ETL Script

This script downloads EPA FRS data and loads it into Snowflake using key pair authentication.
The FRS dataset contains comprehensive facility information across all EPA programs.
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

# Try to import from common utilities if available
try:
    from common.snowflake_utils import SnowflakeConnector
except ImportError:
    # If common utilities not available, use the inline version
    pass

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

class EPAFRSDownloader:
    """Downloads and processes EPA FRS data."""
    
    def __init__(self, download_dir: str = None):
        self.download_dir = download_dir or tempfile.mkdtemp(prefix="epa_frs_data_")
        self.base_url = "https://echo.epa.gov/files/echodownloads/"
        
        # FRS specific filenames and URLs
        self.frs_urls = [
            "https://echo.epa.gov/files/echodownloads/frs_downloads.zip",
            "https://echo.epa.gov/files/echodownloads/FRS_FULL.zip",
            "https://echo.epa.gov/files/echodownloads/FRS_latest_downloads.zip",
            "https://echo.epa.gov/files/echodownloads/NATIONAL_COMBINED_FRS.zip"
        ]
        
        os.makedirs(self.download_dir, exist_ok=True)
        logger.info(f"Using download directory: {self.download_dir}")
    
    def check_last_modified(self, url: str) -> Optional[str]:
        """Check when the file at URL was last modified."""
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                last_modified = response.headers.get('Last-Modified', 'Unknown')
                content_length = response.headers.get('Content-Length', 'Unknown')
                logger.info(f"File info - Last Modified: {last_modified}, Size: {content_length} bytes")
                return last_modified
        except Exception as e:
            logger.warning(f"Could not check last modified date: {e}")
        return None
    
    def download_frs_data(self) -> str:
        """Download the EPA FRS zip file."""
        logger.info("Searching for EPA FRS dataset...")
        
        # First try to find FRS links from the main downloads page
        try:
            import re
            response = requests.get("https://echo.epa.gov/tools/data-downloads")
            if response.status_code == 200:
                # Look for FRS download links
                frs_links = re.findall(r'href="([^"]*(?:frs|FRS)[^"]*\.zip)"', 
                                      response.text, re.IGNORECASE)
                if frs_links:
                    for link in frs_links:
                        if not link.startswith('http'):
                            link = f"https://echo.epa.gov{link}"
                        self.frs_urls.insert(0, link)
                        logger.info(f"Found FRS link: {link}")
        except Exception as e:
            logger.warning(f"Could not parse FRS URLs from main page: {e}")
        
        # Try each URL
        for url in self.frs_urls:
            logger.info(f"Trying FRS download URL: {url}")
            
            # Check last modified date
            self.check_last_modified(url)
            
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                zip_filename = url.split('/')[-1]
                zip_path = os.path.join(self.download_dir, zip_filename)
                
                # Download with progress indication
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            if downloaded % (1024 * 1024) < 8192:  # Log every MB
                                logger.info(f"Download progress: {progress:.1f}%")
                
                logger.info(f"Successfully downloaded {zip_path}")
                return zip_path
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to download from {url}: {e}")
                continue
        
        raise Exception(
            "Could not download FRS data. Please check EPA ECHO data downloads page:\n"
            "https://echo.epa.gov/tools/data-downloads"
        )
    
    def extract_csv_files(self, zip_path: str) -> List[str]:
        """Extract CSV files from the downloaded zip file."""
        extract_dir = os.path.join(self.download_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        
        csv_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                logger.info(f"Found {len(file_list)} files in FRS zip archive")
                
                # Log all files found
                for file_name in file_list:
                    logger.debug(f"  - {file_name}")
                
                # Extract CSV files
                for file_name in file_list:
                    if file_name.lower().endswith('.csv'):
                        zip_ref.extract(file_name, extract_dir)
                        csv_path = os.path.join(extract_dir, file_name)
                        csv_files.append(csv_path)
                        
                        # Get file size
                        file_size = os.path.getsize(csv_path) / (1024 * 1024)
                        logger.info(f"Extracted: {file_name} ({file_size:.2f} MB)")
                
            logger.info(f"Extracted {len(csv_files)} CSV files from FRS archive")
            return csv_files
            
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract zip file: {e}")
            raise
    
    def cleanup(self):
        """Clean up temporary download directory."""
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)
            logger.info(f"Cleaned up temporary directory: {self.download_dir}")

class SnowflakeFRSLoader:
    """Handles loading FRS data into Snowflake using key pair authentication."""
    
    def __init__(self):
        """Initialize Snowflake connection parameters from environment variables."""
        self.account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.user = os.environ.get('SNOWFLAKE_USER')
        self.warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
        self.database = os.environ.get('SNOWFLAKE_DATABASE')
        self.schema = os.environ.get('SNOWFLAKE_SCHEMA')
        self.role = os.environ.get('SNOWFLAKE_ROLE')
        
        private_key_str = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
        
        if not all([self.account, self.user, self.warehouse, self.database, 
                   self.schema, self.role, private_key_str]):
            raise ValueError("Missing required Snowflake environment variables")
        
        self.private_key = self._parse_private_key(private_key_str)
        self.conn = None
        
        logger.info(f"Initialized Snowflake config - Account: {self.account}, User: {self.user}")
    
    def _parse_private_key(self, private_key_str: str):
        """Parse the private key from string."""
        try:
            private_key_bytes = private_key_str.encode('utf-8')
            
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
        
        if self.table_exists(table_name):
            logger.info(f"Table {table_name} already exists - verifying columns")
            
            cursor = self.conn.cursor()
            
            # Get existing columns
            cursor.execute(f"DESCRIBE TABLE \"{table_name}\"")
            existing_columns = {row[0].upper(): row[1] for row in cursor.fetchall()}
            
            # Check if metadata columns exist, don't try to add them if they do
            required_columns = ['LOAD_ID', 'LOAD_TIMESTAMP', 'LOAD_DATE']
            missing_columns = [col for col in required_columns if col not in existing_columns]
            
            if missing_columns:
                logger.info(f"Missing columns in {table_name}: {missing_columns}")
                
                # Only add truly missing columns
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
        
        # Create new table
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
        """Load CSV data into Snowflake table (append mode)."""
        table_name = table_name.upper()
        total_rows = 0
        load_id = load_id or datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Get a sample to understand structure
            df_sample = pd.read_csv(csv_path, nrows=1000, dtype=str, na_filter=False)
            
            # Create or check table
            status = self.create_or_alter_table(df_sample, table_name)
            
            # If table exists, check for column compatibility
            if status == "EXISTS":
                cursor = self.conn.cursor()
                cursor.execute(f"DESCRIBE TABLE \"{table_name}\"")
                existing_cols = {row[0].upper() for row in cursor.fetchall()}
                cursor.close()
                
                # Clean new column names
                new_cols = {col.replace(' ', '_').replace('-', '_').upper() for col in df_sample.columns}
                
                # Check for mismatches
                missing_in_table = new_cols - existing_cols
                missing_in_csv = existing_cols - new_cols - {'LOAD_TIMESTAMP', 'LOAD_DATE', 'LOAD_ID'}
                
                if missing_in_table and not missing_in_csv:
                    # New columns in CSV that aren't in table - need to add them
                    logger.warning(f"New columns in CSV not in table: {missing_in_table}")
                    cursor = self.conn.cursor()
                    for col in missing_in_table:
                        try:
                            logger.info(f"Adding new column {col} to {table_name}")
                            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" STRING')
                        except Exception as e:
                            logger.warning(f"Could not add column {col}: {e}")
                    cursor.close()
                elif missing_in_csv:
                    logger.warning(f"Table has columns not in CSV (will be NULL): {missing_in_csv}")
            
            # Process CSV in chunks
            chunk_count = 0
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str, na_filter=False):
                chunk_count += 1
                logger.info(f"Processing chunk {chunk_count} of {table_name}")
                
                # Clean column names
                chunk.columns = [col.replace(' ', '_').replace('-', '_').upper() 
                               for col in chunk.columns]
                
                # Add metadata
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

def get_table_name_from_filename(filename: str) -> str:
    """Generate a clean table name from CSV filename."""
    table_name = filename.replace('.csv', '').replace('.CSV', '')
    table_name = table_name.replace('-', '_').replace(' ', '_').upper()
    
    # Ensure EPA_FRS prefix
    if table_name.startswith('FRS_'):
        table_name = f"EPA_{table_name}"
    elif not table_name.startswith('EPA_FRS_'):
        table_name = f"EPA_FRS_{table_name}"
    
    return table_name

def main():
    """Main ETL process for EPA FRS data."""
    
    downloader = None
    sf_loader = None
    
    try:
        # Generate unique load ID
        load_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info(f"Starting EPA FRS ETL Process - Load ID: {load_id}")
        
        # Step 1: Download EPA FRS data
        logger.info("=== Downloading EPA FRS Data ===")
        downloader = EPAFRSDownloader()
        zip_path = downloader.download_frs_data()
        
        # Step 2: Extract CSV files
        csv_files = downloader.extract_csv_files(zip_path)
        
        if not csv_files:
            logger.error("No CSV files found in the FRS archive")
            return 1
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Step 3: Connect to Snowflake
        logger.info("=== Connecting to Snowflake ===")
        sf_loader = SnowflakeFRSLoader()
        sf_loader.connect()
        
        # Step 4: Process each CSV file
        logger.info("=== Processing FRS CSV Files ===")
        
        total_tables_processed = 0
        total_rows_loaded = 0
        errors = []
        
        for csv_path in csv_files:
            filename = os.path.basename(csv_path)
            table_name = get_table_name_from_filename(filename)
            
            logger.info(f"\nProcessing {filename} -> {table_name}")
            
            try:
                # Load data
                rows_loaded = sf_loader.load_csv_to_table(
                    csv_path, 
                    table_name, 
                    load_id=load_id
                )
                
                total_tables_processed += 1
                total_rows_loaded += rows_loaded
                
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                errors.append({'file': filename, 'error': str(e)})
                continue
        
        # Step 5: Print results
        logger.info(f"\n=== FRS ETL COMPLETION SUMMARY ===")
        logger.info(f"Load ID: {load_id}")
        logger.info(f"Tables processed: {total_tables_processed}/{len(csv_files)}")
        logger.info(f"Total rows loaded: {total_rows_loaded:,}")
        
        if errors:
            logger.warning(f"Errors encountered: {len(errors)}")
            for error in errors:
                logger.warning(f"  - {error['file']}: {error['error']}")
        
        return 0 if not errors else 1
        
    except Exception as e:
        logger.error(f"FRS ETL process failed: {e}")
        return 1
        
    finally:
        if sf_loader:
            sf_loader.disconnect()
        
        if downloader:
            downloader.cleanup()
        
        logger.info("FRS ETL process completed")

if __name__ == "__main__":
    sys.exit(main())
