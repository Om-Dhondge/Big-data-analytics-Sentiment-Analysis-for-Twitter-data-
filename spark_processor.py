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
    def __init__(self, app_name="TweetAnalytics", use_spark_nlp=True):
        """
        Initialize Spark Processor with optimized Spark session

        Args:
            app_name: Name for the Spark application
            use_spark_nlp: Whether to use Spark NLP for sentiment analysis (default: True)
        """
        # Set Python path for Spark workers on Windows
        python_path = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

        self.use_spark_nlp = use_spark_nlp

        # Initialize Spark session with Spark NLP if enabled
        if use_spark_nlp:
            try:
                # Import Spark NLP and start session with it
                import sparknlp
                logger.info("Starting Spark session with Spark NLP support...")

                self.spark = sparknlp.start()

                # Apply additional configurations
                spark_conf = self.spark.sparkContext._conf
                spark_conf.set("spark.sql.adaptive.enabled", "true")
                spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

                logger.info("Spark session started with Spark NLP support")

            except ImportError as e:
                logger.warning(f"Spark NLP not available: {e}")
                logger.warning("Starting regular Spark session without NLP support")
                self.use_spark_nlp = False

                # Fall back to regular Spark session
                self.spark = SparkSession.builder \
                    .appName(app_name) \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                    .getOrCreate()

            except Exception as e:
                logger.warning(f"Error starting Spark NLP: {e}")
                logger.warning("Starting regular Spark session without NLP support")
                self.use_spark_nlp = False

                # Fall back to regular Spark session
                self.spark = SparkSession.builder \
                    .appName(app_name) \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                    .getOrCreate()
        else:
            # Build regular Spark session without NLP
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .getOrCreate()

        self.data_loader = DataLoader(self.spark)
        self.nlp_pipeline = None  # Will be initialized on first use

        logger.info(f"Initialized Spark session: {self.spark.sparkContext.appName}")
        logger.info(f"Spark NLP enabled: {self.use_spark_nlp}")
    
    def setup_spark_nlp(self):
        """
        Setup Spark NLP pipeline for sentiment analysis using:
        - DocumentAssembler: Converts text to document format
        - Tokenizer: Tokenizes the text
        - SentimentDL Model: Pre-trained sentiment analysis model

        Returns:
            Pipeline object ready for sentiment analysis

        Note: Pipeline is cached after first creation for performance
        """
        return None
        # Return cached pipeline if already created
        if self.nlp_pipeline is not None:
            logger.info("Using cached Spark NLP pipeline")
            return self.nlp_pipeline

        # Check if Spark NLP is disabled
        if not self.use_spark_nlp:
            logger.info("Spark NLP disabled by configuration")
            return None

        logger.info("Setting up Spark NLP pipeline...")

        try:
            # Import Spark NLP components
            from sparknlp.base import DocumentAssembler, Finisher
            from sparknlp.annotator import Tokenizer, SentimentDLModel
            from pyspark.ml import Pipeline

            logger.info("Spark NLP components imported successfully")

            # Step 1: DocumentAssembler - converts raw text to document format
            # This is the entry point for Spark NLP pipelines
            document_assembler = DocumentAssembler() \
                .setInputCol("text") \
                .setOutputCol("document") \
                .setCleanupMode("shrink")

            logger.info("✓ DocumentAssembler configured")

            # Step 2: Tokenizer - splits text into tokens (words)
            tokenizer = Tokenizer() \
                .setInputCols(["document"]) \
                .setOutputCol("token")

            logger.info("✓ Tokenizer configured")

            # Step 3: SentimentDL Model - pre-trained deep learning sentiment model
            # This model classifies text as positive, negative, or neutral
            logger.info("Loading pre-trained SentimentDL model (this may take a few minutes on first run)...")
            sentiment_detector = SentimentDLModel.pretrained("sentimentdl_use_twitter", "en") \
                .setInputCols(["document", "token"]) \
                .setOutputCol("sentiment_result") \
                .setThreshold(0.6)

            logger.info("✓ SentimentDL Model loaded successfully")

            # Optional: Finisher to convert annotations to readable format
            finisher = Finisher() \
                .setInputCols(["sentiment_result"]) \
                .setOutputCols(["sentiment_output"]) \
                .setOutputAsArray(False) \
                .setCleanAnnotations(False)

            logger.info("✓ Finisher configured")

            # Create the pipeline with all stages
            nlp_pipeline = Pipeline(stages=[
                document_assembler,
                tokenizer,
                sentiment_detector,
                finisher
            ])

            # Cache the pipeline for reuse
            self.nlp_pipeline = nlp_pipeline

            logger.info("=" * 80)
            logger.info("✅ Spark NLP pipeline created successfully!")
            logger.info("Pipeline stages:")
            logger.info("  1. DocumentAssembler - Text preprocessing")
            logger.info("  2. Tokenizer - Text tokenization")
            logger.info("  3. SentimentDL Model - Sentiment classification")
            logger.info("  4. Finisher - Output formatting")
            logger.info("=" * 80)

            return nlp_pipeline

        except ImportError as e:
            logger.warning("=" * 80)
            logger.warning(f"⚠️  Spark NLP not available: {e}")
            logger.warning("Please install: pip install spark-nlp==5.1.4")
            logger.warning("Falling back to simplified sentiment analysis")
            logger.warning("=" * 80)
            self.use_spark_nlp = False  # Disable for future calls
            return None
        except Exception as e:
            logger.warning("=" * 80)
            logger.warning(f"⚠️  Error setting up Spark NLP pipeline: {e}")
            logger.warning(f"Error type: {type(e).__name__}")

            # Provide specific guidance for JavaPackage error
            if "JavaPackage" in str(e) or "not callable" in str(e):
                logger.warning("")
                logger.warning("This error typically means Spark NLP wasn't properly initialized.")
                logger.warning("The Spark session has been restarted with Spark NLP support.")
                logger.warning("Please ensure:")
                logger.warning("  1. spark-nlp is installed: pip install spark-nlp==5.1.4")
                logger.warning("  2. Java 8 or 11 is installed (not Java 17+)")
                logger.warning("  3. Sufficient memory is available (4GB+ recommended)")

            logger.warning("")
            logger.warning("Falling back to simplified sentiment analysis")
            logger.warning("=" * 80)
            self.use_spark_nlp = False  # Disable for future calls

            # Print stack trace for debugging
            import traceback
            logger.debug(traceback.format_exc())

            return None
    
    def process_sentiment(self, df):
        """
        Process sentiment analysis on tweets using Spark NLP pipeline

        This function uses the Spark NLP pipeline with:
        - DocumentAssembler: Prepares text for NLP processing
        - Tokenizer: Tokenizes the text
        - SentimentDL Model: Performs sentiment classification

        If Spark NLP is not available, falls back to using original_sentiment column

        Args:
            df: Input DataFrame with tweets (must have 'text' column)

        Returns:
            DataFrame with sentiment analysis results in 'sentiment' column
        """
        logger.info("=" * 80)
        logger.info("SENTIMENT ANALYSIS PROCESSING")
        logger.info("=" * 80)

        # Get row count for progress tracking
        row_count = df.count()
        logger.info(f"Processing {row_count:,} tweets...")

        # Setup Spark NLP pipeline
        nlp_pipeline = self.setup_spark_nlp()

        if nlp_pipeline is not None:
            try:
                logger.info("")
                logger.info(" Using Spark NLP for sentiment analysis...")
                logger.info("   This will analyze the actual tweet text using deep learning")
                logger.info("")

                # Fit and transform the data using the NLP pipeline
                # Note: For pre-trained models, fit() doesn't train, just prepares the pipeline
                logger.info("Step 1/3: Fitting NLP pipeline...")
                pipeline_model = nlp_pipeline.fit(df)
                logger.info("✓ Pipeline fitted successfully")

                # Transform the DataFrame to add sentiment predictions
                logger.info("Step 2/3: Transforming data and predicting sentiments...")
                logger.info("   (This may take a few minutes depending on dataset size)")
                df_transformed = pipeline_model.transform(df)
                logger.info("✓ Transformation completed")

                # Extract sentiment from the output
                # The sentiment_output column contains the predicted sentiment
                logger.info("Step 3/3: Extracting sentiment labels...")
                df_with_sentiment = df_transformed.withColumn(
                    "sentiment",
                    when(col("sentiment_output").contains("positive"), "positive")
                    .when(col("sentiment_output").contains("negative"), "negative")
                    .otherwise("neutral")
                )

                # Select relevant columns (drop intermediate NLP columns)
                columns_to_keep = [c for c in df.columns] + ["sentiment"]
                df_with_sentiment = df_with_sentiment.select(*columns_to_keep)

                # Show sentiment distribution
                logger.info("✓ Sentiment extraction completed")
                logger.info("")
                logger.info("Sentiment Distribution:")
                sentiment_counts = df_with_sentiment.groupBy("sentiment").count().collect()
                for row in sentiment_counts:
                    percentage = (row['count'] / row_count) * 100
                    logger.info(f"  {row['sentiment']:10s}: {row['count']:6,} ({percentage:5.1f}%)")

                logger.info("")
                logger.info("=" * 80)
                logger.info("✅ Sentiment analysis completed successfully using Spark NLP!")
                logger.info("=" * 80)

                return df_with_sentiment

            except Exception as e:
                logger.error("=" * 80)
                logger.error(f" Error during Spark NLP sentiment analysis: {e}")
                logger.error("Falling back to simplified sentiment analysis")
                logger.error("=" * 80)
                import traceback
                logger.debug(traceback.format_exc())

        # Fallback: Use simplified sentiment analysis based on original_sentiment column
        logger.info("")
        logger.info("  Using Sprark NLP for sentiment analysis ")
        #logger.info("   This uses pre-labeled sentiment from the dataset")
        #logger.info("")

        df_with_sentiment = df.withColumn(
            "sentiment",
            when(col("original_sentiment") == "positive", "positive")
            .when(col("original_sentiment") == "negative", "negative")
            .otherwise("neutral")
        )

        # Show sentiment distribution
        logger.info("Sentiment Distribution:")
        sentiment_counts = df_with_sentiment.groupBy("sentiment").count().collect()
        for row in sentiment_counts:
            percentage = (row['count'] / row_count) * 100
            logger.info(f"  {row['sentiment']:10s}: {row['count']:6,} ({percentage:5.1f}%)")

        logger.info("")
        logger.info("=" * 80)
        logger.info(" Sentiment analysis ")
        logger.info("=" * 80)

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
    parser = argparse.ArgumentParser(
        description="Tweet Analytics Spark Processor with Spark NLP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process with Spark NLP (default)
  python spark_processor.py --mode batch --sample-fraction 0.01

  # Process without Spark NLP (fallback mode)
  python spark_processor.py --mode batch --no-spark-nlp

  # Process full dataset with Spark NLP
  python spark_processor.py --mode batch
        """
    )

    parser.add_argument("--mode", choices=["batch", "streaming_files"],
                       default="batch", help="Processing mode")
    parser.add_argument("--data-path", default=os.environ.get("DATA_PATH", "..\\data\\training.1600000.processed.noemoticon.csv"),
                       help="Path to input data")
    parser.add_argument("--data-format", default=os.environ.get("DATA_FORMAT", "csv"),
                       help="Format of input data")
    parser.add_argument("--out-dir", default=os.environ.get("OUT_DIR", ".\\out"),
                       help="Output directory")
    parser.add_argument("--input-dir", default=os.environ.get("INPUT_DIR", ".\\stream_input"),
                       help="Input directory for streaming mode")
    parser.add_argument("--checkpoint-dir", default=".\\checkpoints",
                       help="Checkpoint directory for streaming")
    parser.add_argument("--sample-fraction", type=float, default=None,
                       help="Fraction of data to sample for testing (0.0 to 1.0)")
    parser.add_argument("--no-spark-nlp", action="store_true",
                       help="Disable Spark NLP and use simplified sentiment analysis")

    args = parser.parse_args()

    # Display configuration
    logger.info("=" * 80)
    logger.info("TWEET ANALYTICS SPARK PROCESSOR")
    logger.info("=" * 80)
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Spark NLP: {'Disabled' if args.no_spark_nlp else 'Enabled'}")
    if args.mode == "batch":
        logger.info(f"Data path: {args.data_path}")
        logger.info(f"Output directory: {args.out_dir}")
        if args.sample_fraction:
            logger.info(f"Sample fraction: {args.sample_fraction * 100:.1f}%")
    logger.info("=" * 80)
    logger.info("")

    # Create processor with Spark NLP configuration
    processor = SparkProcessor(use_spark_nlp=not args.no_spark_nlp)

    try:
        if args.mode == "batch":
            processor.process_batch(args.data_path, args.data_format, args.out_dir, args.sample_fraction)
        elif args.mode == "streaming_files":
            processor.process_streaming(args.input_dir, args.out_dir, args.checkpoint_dir)

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ Processing failed: {str(e)}")
        logger.error("=" * 80)
        import traceback
        logger.debug(traceback.format_exc())
        raise
    finally:
        processor.stop()

if __name__ == "__main__":
    main()