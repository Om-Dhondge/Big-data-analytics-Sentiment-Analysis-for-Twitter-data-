#!/usr/bin/env python3
"""
Test Spark initialization to debug hanging issues
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_spark_init():
    """Test Spark session initialization"""
    print("Starting Spark initialization test...")
    
    try:
        # Set Python path for Spark workers on Windows
        python_path = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
        
        print(f"Python path set to: {python_path}")
        
        # Import PySpark
        print("Importing PySpark...")
        from pyspark.sql import SparkSession
        print("PySpark imported successfully")
        
        # Create Spark session with minimal config
        print("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("TestSparkInit") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        print(f"Spark session created: {spark.sparkContext.appName}")
        print(f"Spark version: {spark.version}")
        print(f"Spark context: {spark.sparkContext}")
        
        # Test basic operation
        print("Testing basic Spark operation...")
        test_df = spark.range(10)
        count = test_df.count()
        print(f"Test DataFrame count: {count}")
        
        # Stop Spark session
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"Error during Spark initialization: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_spark_init()
    if success:
        print("✅ Spark initialization test PASSED")
    else:
        print("❌ Spark initialization test FAILED")
