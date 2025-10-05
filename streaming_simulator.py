"""
Streaming Simulator for Tweet Analytics Pipeline
Simulates file-based streaming by writing data chunks to input directory.
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamingSimulator:
    def __init__(self, data_path, input_dir, chunk_size=1000, interval_seconds=30):
        """
        Initialize streaming simulator
        
        Args:
            data_path: Path to the full dataset
            input_dir: Directory to write streaming chunks
            chunk_size: Number of records per chunk
            interval_seconds: Interval between chunk writes
        """
        self.data_path = data_path
        self.input_dir = input_dir
        self.chunk_size = chunk_size
        self.interval_seconds = interval_seconds
        
        # Ensure input directory exists
        os.makedirs(self.input_dir, exist_ok=True)
        
    def extract_hashtags(self, text):
        """Extract hashtags from tweet text"""
        import re
        if text is None:
            return []
        hashtags = re.findall(r'#(\w+)', text.lower())
        return list(set(hashtags)) if hashtags else []
    
    def parse_timestamp(self, date_str):
        """Parse tweet timestamp"""
        try:
            # Parse format: "Mon Apr 06 22:19:45 PDT 2009"
            clean_date = ' '.join(date_str.split()[:-2])
            year = date_str.split()[-1]
            return f"{clean_date} {year}"
        except:
            return None
    
    def load_and_prepare_data(self):
        """
        Load and prepare the full dataset for streaming simulation
        
        Returns:
            Prepared DataFrame ready for streaming
        """
        logger.info(f"Loading data from: {self.data_path}")
        
        try:
            # Load CSV data
            df = pd.read_csv(
                self.data_path,
                header=None,
                names=["sentiment_label", "id", "date", "query", "user", "text"],
                encoding='latin1',  # Handle encoding issues
                on_bad_lines='skip'
            )
            
            logger.info(f"Loaded {len(df)} records")
            
            # Prepare data for streaming
            df['hashtags'] = df['text'].apply(self.extract_hashtags)
            df['created_at'] = df['date'].apply(self.parse_timestamp)
            
            # Simple sentiment mapping
            df['sentiment'] = df['sentiment_label'].map({
                0: 'negative',
                4: 'positive'
            }).fillna('neutral')
            
            # Select only required columns and convert to streaming format
            streaming_df = df[['id', 'text', 'hashtags', 'created_at', 'sentiment']].copy()
            
            # Convert hashtags to JSON string for streaming
            streaming_df['hashtags'] = streaming_df['hashtags'].apply(json.dumps)
            
            # Filter out records with no hashtags to focus on co-occurrence
            streaming_df = streaming_df[streaming_df['hashtags'] != '[]'].reset_index(drop=True)
            
            logger.info(f"Prepared {len(streaming_df)} records with hashtags for streaming")
            return streaming_df
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def start_simulation(self, max_chunks=None):
        """
        Start the streaming simulation
        
        Args:
            max_chunks: Maximum number of chunks to generate (None for unlimited)
        """
        logger.info("Starting streaming simulation...")
        
        # Load and prepare data
        df = self.load_and_prepare_data()
        
        if len(df) == 0:
            logger.error("No data available for streaming")
            return
        
        chunk_count = 0
        start_idx = 0
        
        try:
            while start_idx < len(df):
                if max_chunks and chunk_count >= max_chunks:
                    logger.info(f"Reached maximum chunks limit: {max_chunks}")
                    break
                
                # Get current chunk
                end_idx = min(start_idx + self.chunk_size, len(df))
                chunk = df.iloc[start_idx:end_idx]
                
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"tweets_chunk_{chunk_count:04d}_{timestamp}.json"
                filepath = os.path.join(self.input_dir, filename)
                
                # Write chunk as JSON
                chunk.to_json(filepath, orient='records', lines=True)
                
                logger.info(f"Written chunk {chunk_count + 1}: {len(chunk)} records to {filename}")
                
                chunk_count += 1
                start_idx = end_idx
                
                # Wait for next interval (except for the last chunk)
                if start_idx < len(df):
                    logger.info(f"Waiting {self.interval_seconds} seconds for next chunk...")
                    time.sleep(self.interval_seconds)
            
            logger.info(f"Streaming simulation completed. Generated {chunk_count} chunks.")
            
        except KeyboardInterrupt:
            logger.info("Streaming simulation interrupted by user")
        except Exception as e:
            logger.error(f"Error during streaming simulation: {str(e)}")
            raise
    
    def cleanup_input_dir(self):
        """Clean up the input directory"""
        try:
            for filename in os.listdir(self.input_dir):
                filepath = os.path.join(self.input_dir, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
            logger.info(f"Cleaned up input directory: {self.input_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up input directory: {str(e)}")

def main():
    """Main function to run the streaming simulator"""
    parser = argparse.ArgumentParser(description="Tweet Analytics Streaming Simulator")
    parser.add_argument("--data-path", 
                       default=os.environ.get("DATA_PATH", "/app/training_tweets.csv"),
                       help="Path to the full dataset")
    parser.add_argument("--input-dir", 
                       default=os.environ.get("INPUT_DIR", "./stream_input"),
                       help="Directory to write streaming chunks")
    parser.add_argument("--chunk-size", type=int, default=1000,
                       help="Number of records per chunk")
    parser.add_argument("--interval", type=int, default=30,
                       help="Interval between chunks in seconds")
    parser.add_argument("--max-chunks", type=int, default=None,
                       help="Maximum number of chunks to generate")
    parser.add_argument("--cleanup", action="store_true",
                       help="Clean up input directory before starting")
    
    args = parser.parse_args()
    
    simulator = StreamingSimulator(
        data_path=args.data_path,
        input_dir=args.input_dir,
        chunk_size=args.chunk_size,
        interval_seconds=args.interval
    )
    
    if args.cleanup:
        simulator.cleanup_input_dir()
    
    simulator.start_simulation(max_chunks=args.max_chunks)

if __name__ == "__main__":
    main()