"""
Spark Processing Engine for Tweet Analytics Pipeline
Handles sentiment analysis and hashtag co-occurrence graph computation.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, collect_list, size, when, desc, asc, 
    count, sum as spark_sum, max as spark_max, struct,
    window, to_json, from_json, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

from data_loader import DataLoader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkProcessor:
    def __init__(self, app_name="TweetAnalytics"):
        """
        Initialize Spark Processor with optimized Spark session
        """
        # Set Python path for Spark workers on Windows
        python_path = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        self.data_loader = DataLoader(self.spark)
        logger.info(f"Initialized Spark session: {self.spark.sparkContext.appName}")
    
    def setup_spark_nlp(self):
        """
        Setup Spark NLP pipeline for sentiment analysis
        Note: This is a simplified version. In production, you would:
        1. Import sparknlp
        2. Set up DocumentAssembler, Tokenizer, SentimentDLModel pipeline
        
        For this implementation, we'll use a mock sentiment analysis
        """
        logger.info("Setting up Spark NLP pipeline...")
        
        # Simplified sentiment analysis using built-in functions (no UDF)
        return None
    
    def process_sentiment(self, df):
        """
        Process sentiment analysis on tweets using built-in functions

        Args:
            df: Input DataFrame with tweets

        Returns:
            DataFrame with sentiment analysis results
        """
        logger.info("Processing sentiment analysis...")

        self.setup_spark_nlp()  # Just for logging

        # Simple sentiment analysis using built-in functions
        # Based on original_sentiment column from the data
        df_with_sentiment = df.withColumn(
            "sentiment",
            when(col("original_sentiment") == "positive", "positive")
            .when(col("original_sentiment") == "negative", "negative")
            .otherwise("neutral")
        )

        return df_with_sentiment
    
    def build_hashtag_cooccurrence(self, df):
        """
        Build hashtag co-occurrence graph from tweets (Windows-compatible approach)

        Args:
            df: DataFrame with tweets

        Returns:
            Tuple of (vertices_df, edges_df)
        """
        logger.info("Building hashtag co-occurrence graph...")

        # Filter tweets that contain hashtags
        tweets_with_hashtags = df.filter(col("has_hashtags") == True)

        # Convert to Pandas for hashtag processing (Windows-compatible)
        logger.info("Converting to Pandas for hashtag extraction...")
        pandas_df = tweets_with_hashtags.toPandas()

        # Extract hashtags and build co-occurrence using pure Python
        import re
        from itertools import combinations

        hashtag_pairs = []
        hashtag_counts = {}
        hashtag_sentiments = {}

        for _, row in pandas_df.iterrows():
            text = str(row['text']) if row['text'] else ""
            sentiment = row['sentiment']

            # Extract hashtags using regex
            hashtags = re.findall(r'#(\w+)', text.lower())

            if len(hashtags) >= 1:
                # Count individual hashtags
                for hashtag in hashtags:
                    hashtag_counts[hashtag] = hashtag_counts.get(hashtag, 0) + 1
                    if hashtag not in hashtag_sentiments:
                        hashtag_sentiments[hashtag] = {}
                    hashtag_sentiments[hashtag][sentiment] = hashtag_sentiments[hashtag].get(sentiment, 0) + 1

                # Create pairs for co-occurrence (if multiple hashtags)
                if len(hashtags) >= 2:
                    for hashtag1, hashtag2 in combinations(sorted(hashtags), 2):
                        hashtag_pairs.append((hashtag1, hashtag2, sentiment))

        logger.info(f"Found {len(hashtag_counts)} unique hashtags and {len(hashtag_pairs)} co-occurrence pairs")

        # Build edges DataFrame
        if hashtag_pairs:
            edges_data = {}
            for src, dst, sentiment in hashtag_pairs:
                key = (src, dst)
                if key not in edges_data:
                    edges_data[key] = 0
                edges_data[key] += 1

            edges_list = [{"src": src, "dst": dst, "weight": weight}
                         for (src, dst), weight in edges_data.items()]
        else:
            edges_list = []

        # Build vertices DataFrame
        vertices_list = []
        for hashtag, count in hashtag_counts.items():
            # Find dominant sentiment
            sentiments = hashtag_sentiments[hashtag]
            dominant_sentiment = max(sentiments.items(), key=lambda x: x[1])[0]

            vertices_list.append({
                "id": hashtag,
                "degree": count,
                "dominant_sentiment": dominant_sentiment
            })

        # Return the data as Python lists instead of Spark DataFrames
        # to avoid Windows UDF compatibility issues
        logger.info(f"Generated {len(edges_list)} edges and {len(vertices_list)} vertices")

        return vertices_list, edges_list
    
    def process_batch(self, data_path, data_format, out_dir, sample_fraction=None):
        """
        Process tweets in batch mode
        
        Args:
            data_path: Path to input data
            data_format: Format of input data
            out_dir: Output directory for results
            sample_fraction: Optional fraction to sample for testing
        """
        logger.info("Starting batch processing...")
        
        # Load data
        df = self.data_loader.load_data(data_path, data_format, sample_fraction=sample_fraction)
        
        # Process sentiment
        df_with_sentiment = self.process_sentiment(df)
        
        # Build hashtag co-occurrence graph
        vertices_list, edges_list = self.build_hashtag_cooccurrence(df_with_sentiment)

        # Ensure output directory exists
        os.makedirs(out_dir, exist_ok=True)

        # Write results directly as JSON (Windows-compatible)
        vertices_json_path = os.path.join(out_dir, "vertices.json")
        edges_json_path = os.path.join(out_dir, "edges.json")

        logger.info(f"Writing vertices to: {vertices_json_path}")
        import json
        with open(vertices_json_path, 'w') as f:
            json.dump(vertices_list, f, indent=2)

        logger.info(f"Writing edges to: {edges_json_path}")
        with open(edges_json_path, 'w') as f:
            json.dump(edges_list, f, indent=2)

        logger.info("Batch processing completed successfully!")

        return vertices_list, edges_list
    
    def process_streaming(self, input_dir, out_dir, checkpoint_dir):
        """
        Process tweets in streaming mode (file-based)
        
        Args:
            input_dir: Directory to monitor for new files
            out_dir: Output directory for streaming results
            checkpoint_dir: Checkpoint directory for streaming
        """
        logger.info("Starting streaming processing...")
        
        # Schema for streaming input
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("hashtags", StringType(), True),  # JSON string array
            StructField("created_at", StringType(), True),
            StructField("sentiment", StringType(), True)
        ])
        
        # Read streaming data
        streaming_df = self.spark.readStream \
            .format("json") \
            .schema(schema) \
            .option("path", input_dir) \
            .load()
        
        # Process streaming data (simplified version)
        processed_df = streaming_df.select(
            col("id"),
            col("text"),
            from_json(col("hashtags"), "array<string>").alias("hashtags"),
            col("sentiment"),
            current_timestamp().alias("processing_time")
        )
        
        # Write streaming results
        query = processed_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", out_dir) \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        logger.info("Streaming query started. Waiting for termination...")
        query.awaitTermination()
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function to run the processor"""
    parser = argparse.ArgumentParser(description="Tweet Analytics Spark Processor")
    parser.add_argument("--mode", choices=["batch", "streaming_files"], 
                       default="batch", help="Processing mode")
    parser.add_argument("--data-path", default=os.environ.get("DATA_PATH", "./data/training.1600000.processed.noemoticon.csv"),
                       help="Path to input data")
    parser.add_argument("--data-format", default=os.environ.get("DATA_FORMAT", "csv"),
                       help="Format of input data")
    parser.add_argument("--out-dir", default=os.environ.get("OUT_DIR", "./out"),
                       help="Output directory")
    parser.add_argument("--input-dir", default=os.environ.get("INPUT_DIR", "./stream_input"),
                       help="Input directory for streaming mode")
    parser.add_argument("--checkpoint-dir", default="./checkpoints",
                       help="Checkpoint directory for streaming")
    parser.add_argument("--sample-fraction", type=float, default=None,
                       help="Fraction of data to sample for testing (0.0 to 1.0)")
    
    args = parser.parse_args()
    
    processor = SparkProcessor()
    
    try:
        if args.mode == "batch":
            processor.process_batch(args.data_path, args.data_format, args.out_dir, args.sample_fraction)
        elif args.mode == "streaming_files":
            processor.process_streaming(args.input_dir, args.out_dir, args.checkpoint_dir)
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        raise
    finally:
        processor.stop()

if __name__ == "__main__":
    main()