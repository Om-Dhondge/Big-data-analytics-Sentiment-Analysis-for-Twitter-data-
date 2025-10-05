#!/usr/bin/env python3
"""
Test the main function with command line arguments to debug hanging issues
"""

import sys
import os

def test_main_function():
    """Test the main function with command line arguments"""
    print("Starting main function test...")
    
    try:
        # Set up command line arguments
        original_argv = sys.argv.copy()
        sys.argv = [
            'spark_processor.py',
            '--mode', 'batch',
            '--sample-fraction', '0.01'
        ]
        
        print(f"Command line args: {sys.argv}")
        
        # Import and call main
        print("Importing spark_processor...")
        from spark_processor import main
        print("spark_processor imported successfully")
        
        print("Calling main function...")
        main()
        print("Main function completed successfully")
        
        # Restore original argv
        sys.argv = original_argv
        
        return True
        
    except Exception as e:
        print(f"Error during main function test: {str(e)}")
        import traceback
        traceback.print_exc()
        # Restore original argv
        sys.argv = original_argv
        return False

if __name__ == "__main__":
    success = test_main_function()
    if success:
        print("✅ Main function test PASSED")
    else:
        print("❌ Main function test FAILED")
