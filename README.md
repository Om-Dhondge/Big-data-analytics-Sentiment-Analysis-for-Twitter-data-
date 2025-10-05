# Tweet Analytics Pipeline with PySpark

A lightweight, modular analytics pipeline for processing ~1.6M tweets to perform sentiment analysis and hashtag co-occurrence graph analysis using Apache Spark.

## Overview

This pipeline processes tweet data to:
- Extract and analyze sentiment using Spark NLP (or simplified rule-based analysis)
- Build hashtag co-occurrence networks
- Compute degree centrality for hashtags
- Determine dominant sentiment per hashtag
- Provide real-time dashboard visualization

## Features

- **Batch Processing**: Process the full dataset in one go
- **File-based Streaming**: Optional micro-batch processing simulation
- **Interactive Dashboard**: Real-time network visualization with Plotly Dash
- **Modular Design**: Easy to extend and customize
- **Local Deployment**: No Docker or cloud dependencies required

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Loader   │───▶│  Spark Processor │───▶│   Dashboard     │
│                 │    │                  │    │                 │
│ • CSV/JSON/     │    │ • Sentiment      │    │ • Network Graph │
│   Parquet       │    │   Analysis       │    │ • Charts        │
│ • Schema        │    │ • Co-occurrence  │    │ • Auto-refresh  │
│   Normalization │    │   Graph Building │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Streaming        │
                    │ Simulator        │
                    │ (Optional)       │
                    └──────────────────┘
```

## Prerequisites

- Python 3.9+
- Java 8 or 11 (for Spark)
- Apache Spark 3.3+ (will be installed with PySpark)
- Minimum 4GB RAM (8GB recommended for full dataset)

## Installation

1. **Clone and navigate to the project directory:**
   ```bash
   cd /app
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables (optional):**
   ```bash
   export DATA_PATH="/app/training_tweets.csv"
   export DATA_FORMAT="csv"
   export OUT_DIR="./out"
   export INPUT_DIR="./stream_input"
   ```

## Spark Configuration

### Required Packages for Production

For production use with Spark NLP and GraphFrames, use this spark-submit command:

```bash
spark-submit \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.2,graphframes:graphframes:0.8.2-spark3.2-s_2.12 \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  spark_processor.py --mode batch
```

### Local Development Setup

For local development and testing (using simplified components):

```bash
# Set Spark configuration for local machine
export SPARK_LOCAL_DIRS="/tmp/spark"
export SPARK_HOME=$(python -c "import pyspark; print(pyspark.__path__[0])")
```

## Usage

### 1. Batch Processing (Primary Mode)

Process the full dataset in batch mode:

```bash
# Using command line arguments
python spark_processor.py \
  --mode batch \
  --data-path "/app/training_tweets.csv" \
  --data-format csv \
  --out-dir "./out"

# Using environment variables
export DATA_PATH="/app/training_tweets.csv"
export DATA_FORMAT="csv"
export OUT_DIR="./out"
python spark_processor.py --mode batch
```

**Expected Output:**
- `./out/vertices.json`: Hashtag nodes with degree and dominant sentiment
- `./out/edges.json`: Co-occurrence edges with weights
- `./out/vertices/` and `./out/edges/`: Spark DataFrame outputs

### 2. File-based Streaming Mode (Optional)

#### Step 1: Start the streaming processor
```bash
python spark_processor.py \
  --mode streaming_files \
  --input-dir "./stream_input" \
  --out-dir "./stream_out" \
  --checkpoint-dir "./checkpoints"
```

#### Step 2: In another terminal, start the streaming simulator
```bash
python streaming_simulator.py \
  --data-path "/app/training_tweets.csv" \
  --input-dir "./stream_input" \
  --chunk-size 1000 \
  --interval 30 \
  --max-chunks 10
```

### 3. Dashboard Visualization

Start the interactive dashboard:

```bash
# Default settings
python dashboard.py

# Custom configuration
python dashboard.py \
  --out-dir "./out" \
  --host 127.0.0.1 \
  --port 8050
```

**Access the dashboard at:** http://127.0.0.1:8050

### Dashboard Features

- **Network Graph**: Interactive hashtag co-occurrence network
  - Node size = degree centrality
  - Node color = dominant sentiment (green=positive, red=negative, gray=neutral)
  - Edge width = co-occurrence strength

- **Sentiment Distribution**: Pie chart showing hashtag sentiment breakdown
- **Top Hashtags**: Bar chart of most connected hashtags
- **Auto-refresh**: Configurable data refresh intervals
- **Interactive Controls**: Adjust top-N hashtags display

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_PATH` | `/app/training_tweets.csv` | Path to input dataset |
| `DATA_FORMAT` | `csv` | Input format (csv/json/parquet) |
| `OUT_DIR` | `./out` | Output directory for results |
| `INPUT_DIR` | `./stream_input` | Input directory for streaming mode |

### Command Line Arguments

#### spark_processor.py
```bash
--mode {batch,streaming_files}  # Processing mode (default: batch)
--data-path PATH               # Input data path
--data-format FORMAT           # Input format (csv/json/parquet)
--out-dir DIR                  # Output directory
--input-dir DIR                # Streaming input directory
--checkpoint-dir DIR           # Streaming checkpoint directory
```

#### dashboard.py
```bash
--out-dir DIR                  # Results directory (default: ./out)
--host HOST                    # Server host (default: 127.0.0.1)
--port PORT                    # Server port (default: 8050)
--debug                        # Enable debug mode
```

#### streaming_simulator.py
```bash
--data-path PATH               # Full dataset path
--input-dir DIR                # Streaming chunks output directory
--chunk-size N                 # Records per chunk (default: 1000)
--interval N                   # Seconds between chunks (default: 30)
--max-chunks N                 # Maximum chunks to generate
--cleanup                      # Clean input directory before starting
```

## Data Schema

### Input Data Format (CSV)
The pipeline expects CSV data with columns:
- **sentiment_label**: 0 (negative) or 4 (positive)
- **id**: Unique tweet identifier
- **date**: Timestamp (format: "Mon Apr 06 22:19:45 PDT 2009")
- **query**: Query field (usually "NO_QUERY")
- **user**: Username
- **text**: Tweet content (hashtags extracted from here)

### Output Data Format

#### vertices.json
```json
[
  {
    "id": "hashtag_name",
    "degree": 150,
    "dominant_sentiment": "positive"
  }
]
```

#### edges.json
```json
[
  {
    "src": "hashtag1",
    "dst": "hashtag2", 
    "weight": 25
  }
]
```

## Performance Tuning

### Memory Configuration for Large Datasets

For processing 1.6M tweets locally:

```bash
export SPARK_DRIVER_MEMORY="4g"
export SPARK_EXECUTOR_MEMORY="4g"
export SPARK_DRIVER_MAX_RESULT_SIZE="2g"
```

### Spark SQL Configuration

```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```

### Partition Recommendations

- **Small datasets** (< 100K records): Use 2-4 partitions
- **Medium datasets** (100K - 1M records): Use 8-16 partitions  
- **Large datasets** (> 1M records): Use 32-64 partitions

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**
   - Increase driver/executor memory
   - Reduce partition size or increase partition count
   - Use sampling for development: `--sample-fraction 0.1`

2. **Java/Spark Version Conflicts**
   - Ensure Java 8 or 11 is installed
   - Check Spark version compatibility with packages

3. **GraphFrames Installation Issues**
   - GraphFrames requires specific Spark/Scala versions
   - Fallback: The pipeline includes DataFrame-based degree computation

4. **Dashboard Not Loading Data**
   - Ensure batch processing completed successfully
   - Check that `vertices.json` and `edges.json` exist in output directory
   - Verify file permissions

### Performance Optimization

1. **For Development/Testing:**
   ```python
   # Use sampling to work with smaller datasets
   python spark_processor.py --mode batch --sample-fraction 0.01
   ```

2. **For Production:**
   - Use cluster mode with multiple executors
   - Tune partition size based on data skew
   - Consider using Parquet format for better performance

## Example Workflows

### Quick Start (Development)
```bash
# 1. Process small sample
python spark_processor.py --mode batch --sample-fraction 0.01

# 2. Start dashboard
python dashboard.py

# 3. Open http://127.0.0.1:8050
```

### Full Pipeline (Production)
```bash
# 1. Process full dataset
python spark_processor.py --mode batch

# 2. Start dashboard with custom settings
python dashboard.py --host 0.0.0.0 --port 8080

# 3. Optional: Test streaming mode
python streaming_simulator.py --max-chunks 5 &
python spark_processor.py --mode streaming_files
```

### Streaming Simulation Test
```bash
# Terminal 1: Clean and start streaming processor
rm -rf ./stream_input ./stream_out ./checkpoints
python spark_processor.py --mode streaming_files

# Terminal 2: Start simulator
python streaming_simulator.py --chunk-size 500 --interval 15 --max-chunks 10

# Terminal 3: Monitor dashboard
python dashboard.py --out-dir ./stream_out
```

## Extending the Pipeline

### Adding Custom Sentiment Models

Replace the simplified sentiment analysis in `spark_processor.py`:

```python
def setup_spark_nlp(self):
    import sparknlp
    from sparknlp.base import *
    from sparknlp.annotator import *
    
    # Create Spark NLP pipeline
    document = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")
    
    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")
    
    sentiment = SentimentDLModel.pretrained("sentimentdl_use_twitter", "en") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("sentiment")
    
    pipeline = Pipeline(stages=[document, tokenizer, sentiment])
    return pipeline
```

### Adding Graph Algorithms

For advanced graph analysis with GraphFrames:

```python
from graphframes import GraphFrame

# Create GraphFrame
vertices_gf = vertices_df.withColumnRenamed("id", "id")
edges_gf = edges_df
g = GraphFrame(vertices_gf, edges_gf)

# Compute PageRank
pagerank = g.pageRank(resetProbability=0.15, tol=0.01)
```

## License

This project is provided as-is for educational and research purposes.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review Spark/PySpark documentation
3. Ensure all dependencies are correctly installed
