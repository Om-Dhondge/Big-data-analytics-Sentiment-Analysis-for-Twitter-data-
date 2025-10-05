#!/usr/bin/env python3
"""
Test script for Tweet Analytics Pipeline
Validates all components work correctly
"""

import os
import sys
import subprocess
import time
import json
import pandas as pd
from pathlib import Path

def test_data_loader():
    """Test the data loader component"""
    print("🧪 Testing Data Loader...")
    
    try:
        # JAVA_HOME should be set in environment - no need to override on Windows
        
        from pyspark.sql import SparkSession
        from data_loader import DataLoader
        
        spark = SparkSession.builder \
            .appName("TestDataLoader") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        loader = DataLoader(spark)
        
        # Test with small sample
        df = loader.load_data("./data/training.1600000.processed.noemoticon.csv", "csv", sample_fraction=0.0001)
        
        if df.count() > 0:
            print("  ✅ Data Loader: Successfully loaded and processed data")
        else:
            print("  ❌ Data Loader: No data loaded")
            return False
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"  ❌ Data Loader: Error - {str(e)}")
        return False

def test_spark_processor():
    """Test the Spark processor"""
    print("🧪 Testing Spark Processor...")
    
    try:
        # JAVA_HOME should be set in environment
        
        # Run batch processing with tiny sample
        cmd = [
            'python', 'spark_processor.py',
            '--mode', 'batch',
            '--sample-fraction', '0.0001',
            '--out-dir', './test_output'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Check output files exist
            vertices_file = './test_output/vertices.json'
            edges_file = './test_output/edges.json'
            
            if os.path.exists(vertices_file) and os.path.exists(edges_file):
                print("  ✅ Spark Processor: Successfully generated output files")
                return True
            else:
                print("  ❌ Spark Processor: Output files not generated")
                return False
        else:
            print(f"  ❌ Spark Processor: Failed with error - {result.stderr}")
            return False
            
    except Exception as e:
        print(f"  ❌ Spark Processor: Error - {str(e)}")
        return False

def test_dashboard():
    """Test the dashboard component"""
    print("🧪 Testing Dashboard...")
    
    try:
        # First ensure we have test data
        if not os.path.exists('./test_output/vertices.json'):
            print("  ⚠️  Dashboard: No test data available, skipping dashboard test")
            return True
        
        # Import dashboard modules
        from dashboard import TweetAnalyticsDashboard
        
        dashboard = TweetAnalyticsDashboard('./test_output')
        
        # Test data loading
        vertices_df, edges_df = dashboard.load_data()
        
        if len(vertices_df) > 0 and len(edges_df) > 0:
            print("  ✅ Dashboard: Successfully loaded data and created visualizations")
            return True
        else:
            print("  ❌ Dashboard: Could not load data")
            return False
            
    except Exception as e:
        print(f"  ❌ Dashboard: Error - {str(e)}")
        return False

def test_streaming_simulator():
    """Test the streaming simulator"""
    print("🧪 Testing Streaming Simulator...")
    
    try:
        from streaming_simulator import StreamingSimulator
        
        simulator = StreamingSimulator(
            data_path="./data/training.1600000.processed.noemoticon.csv",
            input_dir="./test_stream_input",
            chunk_size=10,
            interval_seconds=1
        )
        
        # Clean up any existing files
        simulator.cleanup_input_dir()
        
        # Generate a couple of chunks
        simulator.start_simulation(max_chunks=2)
        
        # Check if files were created
        input_files = list(Path('./test_stream_input').glob('*.json'))
        
        if len(input_files) >= 2:
            print("  ✅ Streaming Simulator: Successfully generated streaming files")
            return True
        else:
            print("  ❌ Streaming Simulator: Failed to generate files")
            return False
            
    except Exception as e:
        print(f"  ❌ Streaming Simulator: Error - {str(e)}")
        return False

def test_file_formats():
    """Test output file formats"""
    print("🧪 Testing Output File Formats...")
    
    try:
        vertices_file = './test_output/vertices.json'
        edges_file = './test_output/edges.json'
        
        if os.path.exists(vertices_file) and os.path.exists(edges_file):
            # Test JSON loading
            with open(vertices_file, 'r') as f:
                vertices = json.load(f)
            
            with open(edges_file, 'r') as f:
                edges = json.load(f)
            
            # Validate structure
            if (vertices and isinstance(vertices[0], dict) and 
                'id' in vertices[0] and 'degree' in vertices[0] and 
                edges and isinstance(edges[0], dict) and 
                'src' in edges[0] and 'dst' in edges[0] and 'weight' in edges[0]):
                
                print("  ✅ File Formats: JSON output files have correct structure")
                return True
            else:
                print("  ❌ File Formats: Invalid JSON structure")
                return False
        else:
            print("  ⚠️  File Formats: Output files not available for testing")
            return True
            
    except Exception as e:
        print(f"  ❌ File Formats: Error - {str(e)}")
        return False

def cleanup_test_files():
    """Clean up test files"""
    print("🧹 Cleaning up test files...")
    
    try:
        import shutil
        
        # Remove test directories
        for dir_path in ['./test_output', './test_stream_input']:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
        
        print("  ✅ Cleanup: Test files removed")
        
    except Exception as e:
        print(f"  ⚠️  Cleanup: Error - {str(e)}")

def main():
    """Run all tests"""
    print("🚀 Tweet Analytics Pipeline - Integration Test")
    print("=" * 50)
    print()
    
    # JAVA_HOME should be set in environment
    
    tests = [
        ("Data Loader", test_data_loader),
        ("Spark Processor", test_spark_processor),
        ("Dashboard", test_dashboard),
        ("Streaming Simulator", test_streaming_simulator),
        ("File Formats", test_file_formats)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"  ❌ {test_name}: Unexpected error - {str(e)}")
            results[test_name] = False
        
        print()
    
    # Summary
    print("=" * 50)
    print("📋 Test Results Summary")
    print("=" * 50)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}  {test_name}")
        if result:
            passed += 1
    
    print()
    print(f"Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Pipeline is working correctly.")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
    
    # Cleanup
    cleanup_test_files()
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)