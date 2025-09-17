#!/usr/bin/env python3
"""
Test version of EPA data loaders that only loads limited rows for quick validation.
This script is designed for CI/CD testing on commits.
"""

import os
import sys
import logging
import tempfile
from datetime import datetime
from typing import Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress Snowflake verbosity
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.ocsp_snowflake').setLevel(logging.ERROR)

def test_epa_loader(loader_type: str = 'sdwa', max_rows: int = 1000):
    """
    Test EPA data loader with limited rows.
    
    Args:
        loader_type: 'sdwa' or 'frs'
        max_rows: Maximum rows to load per table
    """
    
    logger.info(f"=== Testing {loader_type.upper()} Loader ===")
    logger.info(f"Max rows per table: {max_rows}")
    logger.info(f"Target schema: EPA_TEST_RESULTS")
    
    # Import the appropriate loader module
    if loader_type == 'sdwa':
        try:
            if os.path.exists('epa_sdwa_loader.py'):
                import epa_sdwa_loader as loader_module
            elif os.path.exists('scripts/epa_sdwa_loader.py'):
                sys.path.insert(0, 'scripts')
                import epa_sdwa_loader as loader_module
            else:
                logger.error("Cannot find epa_sdwa_loader.py")
                return 1
        except ImportError as e:
            logger.error(f"Failed to import SDWA loader: {e}")
            return 1
            
    elif loader_type == 'frs':
        try:
            if os.path.exists('epa_frs_loader.py'):
                import epa_frs_loader as loader_module
            elif os.path.exists('scripts/epa_frs_loader.py'):
                sys.path.insert(0, 'scripts')
                import epa_frs_loader as loader_module
            else:
                logger.error("Cannot find epa_frs_loader.py")
                return 1
        except ImportError as e:
            logger.error(f"Failed to import FRS loader: {e}")
            return 1
    else:
        logger.error(f"Unknown loader type: {loader_type}")
        return 1
    
    # Override the load function to limit rows
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas
    
    def limited_load_csv_to_table(self, csv_path: str, table_name: str, 
                                  chunk_size: int = 1000, load_id: str = None) -> int:
        """Modified load function that only loads first N rows for testing."""
        
        # Add TEST_ prefix to table name
        original_table_name = table_name
        table_name = f"TEST_{table_name}".upper()
        
        logger.info(f"TEST MODE: Loading max {max_rows} rows from {os.path.basename(csv_path)}")
        logger.info(f"TEST MODE: Target table: {table_name}")
        
        total_rows = 0
        load_id = load_id or datetime.now().strftime('%Y%m%d_%H%M%S_TEST')
        
        try:
            # Read only limited rows for testing
            df = pd.read_csv(csv_path, nrows=max_rows, dtype=str, na_filter=False)
            
            # Create or check table
            if hasattr(self, 'create_or_alter_table'):
                self.create_or_alter_table(df, table_name)
            
            # Clean column names
            df.columns = [col.replace(' ', '_').replace('-', '_').upper() for col in df.columns]
            
            # Add metadata columns
            df['LOAD_TIMESTAMP'] = datetime.now()
            df['LOAD_DATE'] = datetime.now().date()
            df['LOAD_ID'] = load_id
            df['TEST_RUN'] = 'TRUE'  # Mark as test data
            
            # Replace 'nan' strings with None
            df = df.replace('nan', None)
            
            # Write to Snowflake in smaller chunks for test
            chunk_size = min(500, len(df))
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                
                success, nchunks, nrows, _ = write_pandas(
                    self.conn, 
                    chunk, 
                    table_name,
                    auto_create_table=False,
                    quote_identifiers=True
                )
                
                if success:
                    total_rows += len(chunk)
                    logger.info(f"Loaded {len(chunk)} test rows (Total: {total_rows})")
                else:
                    logger.error(f"Failed to load test chunk")
                    break
                    
        except Exception as e:
            logger.error(f"Error in test load: {e}")
            raise
        
        logger.info(f"TEST MODE: Loaded {total_rows} rows to {table_name}")
        return total_rows
    
    # Monkey-patch the loader classes
    if loader_type == 'sdwa' and hasattr(loader_module, 'SnowflakeKeyPairLoader'):
        loader_module.SnowflakeKeyPairLoader.load_csv_to_table = limited_load_csv_to_table
    elif loader_type == 'frs' and hasattr(loader_module, 'SnowflakeFRSLoader'):
        loader_module.SnowflakeFRSLoader.load_csv_to_table = limited_load_csv_to_table
    
    # Override environment to use test schema
    original_schema = os.environ.get('SNOWFLAKE_SCHEMA', 'EPA_DATA_RAW')
    os.environ['SNOWFLAKE_SCHEMA'] = 'EPA_TEST_RESULTS'
    
    # Disable deduplication for tests
    os.environ['RUN_DEDUPLICATION'] = 'false'
    
    try:
        # Run the main function
        logger.info(f"Starting {loader_type.upper()} test run...")
        exit_code = loader_module.main()
        
        if exit_code == 0:
            logger.info(f"✅ {loader_type.upper()} test completed successfully")
        else:
            logger.error(f"❌ {loader_type.upper()} test failed with exit code {exit_code}")
        
        return exit_code
        
    finally:
        # Restore original schema
        os.environ['SNOWFLAKE_SCHEMA'] = original_schema

def main():
    """Run tests for both EPA loaders."""
    
    # Get test parameters from environment
    max_rows = int(os.environ.get('MAX_ROWS_PER_TABLE', '1000'))
    run_sdwa = os.environ.get('TEST_SDWA', 'true').lower() == 'true'
    run_frs = os.environ.get('TEST_FRS', 'true').lower() == 'true'
    
    logger.info("="*60)
    logger.info("EPA DATA PIPELINE TEST RUN")
    logger.info("="*60)
    logger.info(f"Max rows per table: {max_rows}")
    logger.info(f"Test SDWA: {run_sdwa}")
    logger.info(f"Test FRS: {run_frs}")
    logger.info(f"Target schema: EPA_TEST_RESULTS")
    logger.info("="*60)
    
    results = []
    
    # Test SDWA loader
    if run_sdwa:
        try:
            sdwa_result = test_epa_loader('sdwa', max_rows)
            results.append(('SDWA', sdwa_result))
        except Exception as e:
            logger.error(f"SDWA test exception: {e}")
            results.append(('SDWA', 1))
    
    # Test FRS loader
    if run_frs:
        try:
            frs_result = test_epa_loader('frs', max_rows)
            results.append(('FRS', frs_result))
        except Exception as e:
            logger.error(f"FRS test exception: {e}")
            results.append(('FRS', 1))
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    
    all_passed = True
    for loader_name, exit_code in results:
        status = "✅ PASSED" if exit_code == 0 else "❌ FAILED"
        logger.info(f"{loader_name}: {status}")
        if exit_code != 0:
            all_passed = False
    
    logger.info("="*60)
    
    if all_passed:
        logger.info("✅ All tests passed")
        return 0
    else:
        logger.error("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
