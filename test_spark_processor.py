#!/usr/bin/env python3
"""
Test SparkProcessor initialization to debug hanging issues
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_spark_processor():
    """Test SparkProcessor initialization and basic operations"""
    print("Starting SparkProcessor test...")
    
    try:
        # Import the SparkProcessor class
        print("Importing SparkProcessor...")
        from spark_processor import SparkProcessor
        print("SparkProcessor imported successfully")
        
        # Create SparkProcessor instance
        print("Creating SparkProcessor instance...")
        processor = SparkProcessor()
        print("SparkProcessor created successfully")
        
        # Test basic batch processing with minimal sample
        print("Testing batch processing with sample fraction 0.001...")
        data_path = "./data/training.1600000.processed.noemoticon.csv"
        data_format = "csv"
        out_dir = "./out"
        sample_fraction = 0.001
        
        print(f"Processing: {data_path}")
        print(f"Sample fraction: {sample_fraction}")
        
        # Call process_batch
        vertices_df, edges_df = processor.process_batch(data_path, data_format, out_dir, sample_fraction)
        
        print("Batch processing completed successfully")
        print(f"Vertices count: {vertices_df.count()}")
        print(f"Edges count: {edges_df.count()}")
        
        # Stop processor
        print("Stopping processor...")
        processor.stop()
        print("Processor stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"Error during SparkProcessor test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_spark_processor()
    if success:
        print("✅ SparkProcessor test PASSED")
    else:
        print("❌ SparkProcessor test FAILED")
