#!/bin/bash

# Tweet Analytics Pipeline Setup Script

echo "=== Tweet Analytics Pipeline Setup ==="
echo ""

# Set JAVA_HOME (Windows - adjust path as needed)
# export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
echo "✓ JAVA_HOME should be set in your environment"

# Set environment variables
export DATA_PATH="./data/training.1600000.processed.noemoticon.csv"
export DATA_FORMAT="csv"
export OUT_DIR="./out"
export INPUT_DIR="./stream_input"

echo "✓ Environment variables configured"
echo "  - DATA_PATH: $DATA_PATH"
echo "  - DATA_FORMAT: $DATA_FORMAT"
echo "  - OUT_DIR: $OUT_DIR"
echo "  - INPUT_DIR: $INPUT_DIR"
echo ""

# Check if data file exists
if [ -f "$DATA_PATH" ]; then
    echo "✓ Dataset found: $DATA_PATH"
    echo "  Size: $(du -sh $DATA_PATH | cut -f1)"
    echo "  Records: $(wc -l < $DATA_PATH | tr -d ' ')"
else
    echo "⚠️  Dataset not found at: $DATA_PATH"
    echo "   Please ensure the tweet dataset is available at this location"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Quick Start Commands:"
echo ""
echo "1. Test with small sample:"
echo "   python spark_processor.py --mode batch --sample-fraction 0.001"
echo ""
echo "2. Process full dataset:"
echo "   python spark_processor.py --mode batch"
echo ""
echo "3. Start dashboard:"
echo "   python dashboard.py"
echo ""
echo "4. Streaming simulation (optional):"
echo "   python streaming_simulator.py --max-chunks 5 &"
echo "   python spark_processor.py --mode streaming_files"
echo ""
echo "Dashboard will be available at: http://localhost:8050"
echo ""