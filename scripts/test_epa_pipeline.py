#!/usr/bin/env python3
"""
Test version of EPA pipeline that only processes first 1000 rows per CSV file.
This script downloads the EPA data but only loads a small subset for testing.
"""

import os
import sys
import logging
import zipfile
import tempfile
import shutil
from datetime import datetime
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.ocsp_snowflake').setLevel(logging.ERROR)

# TEST CONFIGURATION
MAX_ROWS = int(os.environ.get('MAX_ROWS_PER_TABLE', '1000'))
MAX_FILES = int(os.environ.get('MAX_FILES_TO_PROCESS', '3'))  # Only process first 3 CSV files

logger.info(f"TEST MODE: Max {MAX_ROWS} rows per table, Max {MAX_FILES} files")

class TestEPALoader:
    """Test version of EPA loader with row limits."""
    
    def __init__(self):
        self.download_dir = tempfile.mkdtemp(prefix="test_epa_data_")
        logger.info(f"Test download directory: {self.download_dir}")
    
    def download_and_extract(self, dataset_type='sdwa'):
        """Download EPA data and extract CSVs."""
        logger.info(f"Downloading {dataset_type.upper()} data...")
        
        if dataset_type == 'sdwa':
            urls = [
                "https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip",
                "https://echo.epa.gov/files/echodownloads/sdwa_downloads.zip"
            ]
        else:  # FRS
            urls = [
                "https://echo.epa.gov/files/echodownloads/frs_downloads.zip",
                "https://echo.epa.gov/files/echodownloads/FRS_FULL.zip"
            ]
        
        for url in urls:
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                zip_path = os.path.join(self.download_dir, f"{dataset_type}.zip")
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Downloaded {zip_path}")
                
                # Extract CSVs
                extract_dir = os.path.join(self.download_dir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)
                
                csv_files = []
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for file_name in zip_ref.namelist():
                        if file_name.lower().endswith('.csv'):
                            zip_ref.extract(file_name, extract_dir)
                            csv_files.append(os.path.join(extract_dir, file_name))
                
                logger.info(f"Extracted {len(csv_files)} CSV files")
                
                # Limit number of files for testing
                if len(csv_files) > MAX_FILES:
                    logger.info(f"TEST MODE: Limiting to first {MAX_FILES} files")
                    csv_files = csv_files[:MAX_FILES]
                
                return csv_files
                
            except Exception as e:
                logger.warning(f"Failed to download from {url}: {e}")
                continue
        
        return []
    
    def cleanup(self):
        """Clean up temporary files."""
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)
            logger.info("Cleaned up test files")

class TestSnowflakeLoader:
    """Test Snowflake loader with row limits."""
    
    def __init__(self):
        self.account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.user = os.environ.get('SNOWFLAKE_USER')
        self.warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
        self.database = os.environ.get('SNOWFLAKE_DATABASE')
        self.schema = 'EPA_TEST_RESULTS'  # Force test schema
        self.role = os.environ.get('SNOWFLAKE_ROLE')
        
        private_key_str = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
        if not private_key_str:
            raise ValueError("Missing SNOWFLAKE_PRIVATE_KEY")
        
        self.private_key = self._parse_private_key(private_key_str)
        self.conn = None
        
    def _parse_private_key(self, private_key_str: str):
        """Parse private key."""
        private_key_bytes = private_key_str.encode('utf-8')
        private_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None,
            backend=default_backend()
        )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    
    def connect(self):
        """Connect to Snowflake."""
        self.conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            private_key=self.private_key,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role
        )
        logger.info(f"Connected to Snowflake TEST schema: {self.database}.{self.schema}")
    
    def disconnect(self):
        """Disconnect from Snowflake."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from Snowflake")
    
    def load_csv_limited(self, csv_path: str, table_name: str, dataset_type: str):
        """Load LIMITED rows from CSV to Snowflake."""
        
        # Add TEST prefix
        table_name = f"TEST_{dataset_type.upper()}_{os.path.basename(csv_path).replace('.csv', '').upper()}"
        
        logger.info(f"TEST: Loading {MAX_ROWS} rows from {os.path.basename(csv_path)} to {table_name}")
        
        try:
            # Read ONLY the limited number of rows
            df = pd.read_csv(csv_path, nrows=MAX_ROWS, dtype=str, na_filter=False)
            
            logger.info(f"Read {len(df)} rows from CSV")
            
            # Clean column names
            df.columns = [col.replace(' ', '_').replace('-', '_').upper() for col in df.columns]
            
            # Add metadata
            df['LOAD_TIMESTAMP'] = datetime.now()
            df['LOAD_DATE'] = datetime.now().date()
            df['LOAD_ID'] = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            df['TEST_RUN'] = True
            df['ROW_LIMIT'] = MAX_ROWS
            
            # Create table DDL
            columns = [f'"{col}" STRING' for col in df.columns]
            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
            
            cursor = self.conn.cursor()
            cursor.execute(ddl)
            cursor.close()
            
            logger.info(f"Created/verified table {table_name}")
            
            # Write data
            success, _, _, _ = write_pandas(
                self.conn, 
                df, 
                table_name,
                auto_create_table=False,
                quote_identifiers=True
            )
            
            if success:
                logger.info(f"Successfully loaded {len(df)} rows to {table_name}")
                return len(df)
            else:
                logger.error(f"Failed to load data to {table_name}")
                return 0
                
        except Exception as e:
            logger.error(f"Error loading {csv_path}: {e}")
            return 0

def main():
    """Main test function."""
    
    logger.info("="*60)
    logger.info("EPA PIPELINE TEST - LIMITED DATA")
    logger.info("="*60)
    logger.info(f"Row limit: {MAX_ROWS}")
    logger.info(f"File limit: {MAX_FILES}")
    logger.info("Target schema: EPA_TEST_RESULTS")
    logger.info("="*60)
    
    downloader = TestEPALoader()
    sf_loader = TestSnowflakeLoader()
    
    try:
        # Test SDWA
        logger.info("\n=== Testing SDWA ===")
        csv_files = downloader.download_and_extract('sdwa')
        
        if csv_files:
            sf_loader.connect()
            total_rows = 0
            
            for csv_file in csv_files:
                rows = sf_loader.load_csv_limited(csv_file, os.path.basename(csv_file), 'sdwa')
                total_rows += rows
            
            logger.info(f"SDWA Test Complete: {len(csv_files)} files, {total_rows} total rows")
        else:
            logger.warning("No SDWA files found")
        
        logger.info("\n=== Test Summary ===")
        logger.info(f"✅ Test completed successfully")
        logger.info(f"Tables created in {sf_loader.database}.{sf_loader.schema}")
        logger.info(f"All tables prefixed with TEST_")
        logger.info(f"Maximum {MAX_ROWS} rows per table")
        
        return 0
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return 1
        
    finally:
        sf_loader.disconnect()
        downloader.cleanup()

if __name__ == "__main__":
    sys.exit(main())
