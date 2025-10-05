#!/usr/bin/env python3
"""
Test data loading to debug hanging issues
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_loading():
    """Test data loading functionality"""
    print("Starting data loading test...")
    
    try:
        # Set Python path for Spark workers on Windows
        python_path = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
        
        # Import required modules
        from pyspark.sql import SparkSession
        from data_loader import DataLoader
        
        print("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("TestDataLoading") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        print("Spark session created successfully")
        
        # Create data loader
        print("Creating DataLoader...")
        data_loader = DataLoader(spark)
        print("DataLoader created successfully")
        
        # Test data loading with small sample
        data_path = "./data/training.1600000.processed.noemoticon.csv"
        print(f"Testing data loading from: {data_path}")
        print("Loading with sample fraction 0.001...")
        
        df = data_loader.load_data(data_path, "csv", sample_fraction=0.001)
        
        print("Data loaded successfully")
        print(f"DataFrame schema: {df.schema}")
        
        # Get count
        print("Getting row count...")
        count = df.count()
        print(f"Row count: {count}")
        
        # Show first few rows
        print("Showing first 3 rows...")
        df.show(3, truncate=False)
        
        # Stop Spark session
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"Error during data loading test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_data_loading()
    if success:
        print("✅ Data loading test PASSED")
    else:
        print("❌ Data loading test FAILED")
