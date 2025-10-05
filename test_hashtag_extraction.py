#!/usr/bin/env python3
"""
Test hashtag extraction to verify it works correctly
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_hashtag_extraction():
    """Test hashtag extraction functionality"""
    print("Starting hashtag extraction test...")
    
    try:
        # Set Python path for Spark workers on Windows
        python_path = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
        
        # Import required modules
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col
        from data_loader import DataLoader
        
        print("Creating Spark session...")
        spark = SparkSession.builder \
            .appName("TestHashtagExtraction") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        print("Spark session created successfully")
        
        # Create test data with hashtags
        test_data = [
            ("1", "I love #python and #spark for #bigdata processing!"),
            ("2", "No hashtags in this tweet"),
            ("3", "Check out #machinelearning and #AI trends #2023"),
            ("4", "Just a regular tweet without any tags"),
            ("5", "#NLP is amazing for #textprocessing and #analytics")
        ]
        
        test_df = spark.createDataFrame(test_data, ["id", "text"])
        
        print("Created test DataFrame:")
        test_df.show(truncate=False)
        
        # Create data loader and test hashtag extraction
        data_loader = DataLoader(spark)
        
        # Apply hashtag extraction
        df_with_hashtags = test_df.withColumn(
            "hashtags", 
            data_loader.extract_hashtags_builtin(col("text"))
        )
        
        print("\nDataFrame with extracted hashtags:")
        df_with_hashtags.show(truncate=False)
        
        # Show only tweets with hashtags
        tweets_with_hashtags = df_with_hashtags.filter(col("hashtags").isNotNull() & (col("hashtags") != []))
        print(f"\nTweets with hashtags: {tweets_with_hashtags.count()}")
        tweets_with_hashtags.show(truncate=False)
        
        # Stop Spark session
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"Error during hashtag extraction test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_hashtag_extraction()
    if success:
        print("✅ Hashtag extraction test PASSED")
    else:
        print("❌ Hashtag extraction test FAILED")
