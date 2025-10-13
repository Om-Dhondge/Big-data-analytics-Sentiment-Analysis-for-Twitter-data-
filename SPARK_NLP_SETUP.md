# Spark NLP Implementation Guide

## Overview

This project now includes full **Spark NLP** implementation for sentiment analysis using:
- **DocumentAssembler**: Converts raw text to document format for NLP processing
- **Tokenizer**: Splits text into tokens (words)
- **SentimentDL Model**: Pre-trained deep learning model for sentiment classification

## Implementation Details

### 1. `setup_spark_nlp()` Function

Located in `spark_processor.py`, this function creates a complete Spark NLP pipeline:

```python
def setup_spark_nlp(self):
    """
    Setup Spark NLP pipeline for sentiment analysis using:
    - DocumentAssembler: Converts text to document format
    - Tokenizer: Tokenizes the text
    - SentimentDL Model: Pre-trained sentiment analysis model
    """
```

**Pipeline Stages:**

1. **DocumentAssembler**
   - Input: Raw text column (`text`)
   - Output: Document annotation (`document`)
   - Purpose: Prepares text for NLP processing
   - Configuration: Uses "shrink" cleanup mode to remove extra whitespace

2. **Tokenizer**
   - Input: Document annotation (`document`)
   - Output: Token annotations (`token`)
   - Purpose: Splits text into individual words/tokens

3. **SentimentDL Model**
   - Model: `sentimentdl_use_twitter` (pre-trained on Twitter data)
   - Input: Document and token annotations
   - Output: Sentiment predictions (`sentiment_result`)
   - Threshold: 0.6 (confidence threshold for predictions)
   - Classes: positive, negative, neutral

4. **Finisher**
   - Input: Sentiment results
   - Output: Clean sentiment output (`sentiment_output`)
   - Purpose: Converts annotations to readable string format

### 2. `process_sentiment()` Function

This function applies the Spark NLP pipeline to the tweet DataFrame:

```python
def process_sentiment(self, df):
    """
    Process sentiment analysis on tweets using Spark NLP pipeline
    
    Falls back to simplified analysis if Spark NLP is not available
    """
```

**Processing Flow:**

1. Calls `setup_spark_nlp()` to create the pipeline
2. Fits the pipeline to the DataFrame (prepares pre-trained models)
3. Transforms the DataFrame to add sentiment predictions
4. Extracts sentiment labels from the output
5. Returns DataFrame with new `sentiment` column

**Fallback Mechanism:**

If Spark NLP is not available or encounters errors, the function automatically falls back to using the `original_sentiment` column from the dataset.

## Installation

### Step 1: Install Spark NLP

```bash
pip install spark-nlp==5.1.4
```

**Note:** Spark NLP requires:
- Java 8 or 11
- PySpark 3.x
- Sufficient memory (4GB+ recommended)

### Step 2: Verify Installation

```python
import sparknlp
print(sparknlp.version())
```

### Step 3: Update Requirements

The `requirements.txt` file has been updated to include:
```
spark-nlp==5.1.4
```

Install all dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage (Automatic)

The Spark NLP implementation is **automatically used** when you run the processor:

```bash
python spark_processor.py --mode batch --sample-fraction 0.01
```

The pipeline will:
1. Attempt to load Spark NLP
2. If successful, use the full NLP pipeline for sentiment analysis
3. If not available, fall back to simplified analysis
4. Log which method is being used

### Advanced Usage with spark-submit

For production use with proper Spark NLP packages:

```bash
spark-submit \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=2000M \
  spark_processor.py --mode batch
```

### Testing the Implementation

Test with a small sample first:

```bash
# Test with 1% of data
python spark_processor.py --mode batch --sample-fraction 0.01

# Check logs for:
# "Setting up Spark NLP pipeline..."
# "Using Spark NLP for sentiment analysis..."
# "Sentiment analysis completed using Spark NLP"
```

## Pre-trained Model Details

### SentimentDL Model: `sentimentdl_use_twitter`

- **Type**: Deep Learning sentiment classifier
- **Training Data**: Twitter dataset
- **Architecture**: Universal Sentence Encoder (USE) + Deep Learning
- **Output Classes**: positive, negative, neutral
- **Language**: English
- **Performance**: Optimized for short social media text

### Model Download

The model is automatically downloaded on first use:
- Size: ~400MB
- Location: `~/.cache/spark-nlp/` (Linux/Mac) or `%USERPROFILE%\.cache\spark-nlp\` (Windows)
- Download time: 2-5 minutes (depending on internet speed)

## Configuration Options

### Adjusting Sentiment Threshold

In `setup_spark_nlp()`, you can adjust the confidence threshold:

```python
sentiment_detector = SentimentDLModel.pretrained("sentimentdl_use_twitter", "en") \
    .setInputCols(["document", "token"]) \
    .setOutputCol("sentiment_result") \
    .setThreshold(0.7)  # Increase for higher confidence (default: 0.6)
```

### Using Different Models

Spark NLP provides multiple sentiment models:

```python
# For general text (not Twitter-specific)
sentiment_detector = SentimentDLModel.pretrained("sentimentdl_use_imdb", "en")

# For GLOVE embeddings
sentiment_detector = SentimentDLModel.pretrained("sentimentdl_glove_imdb", "en")
```

## Troubleshooting

### Issue 1: Spark NLP Not Found

**Error:** `ImportError: No module named 'sparknlp'`

**Solution:**
```bash
pip install spark-nlp==5.1.4
```

### Issue 2: Model Download Fails

**Error:** `Cannot download model`

**Solution:**
- Check internet connection
- Ensure sufficient disk space (~500MB)
- Try manual download from [Spark NLP Models Hub](https://nlp.johnsnowlabs.com/models)

### Issue 3: Java Version Issues

**Error:** `Java version not compatible`

**Solution:**
- Install Java 8 or 11 (not Java 17+)
- Set `JAVA_HOME` environment variable
- Verify: `java -version`

### Issue 4: Memory Issues

**Error:** `OutOfMemoryError`

**Solution:**
```bash
# Increase driver memory
export PYSPARK_DRIVER_MEMORY=4g
python spark_processor.py --mode batch
```

### Issue 5: Fallback Mode Activated

**Log:** `"Falling back to simplified sentiment analysis"`

**Reasons:**
- Spark NLP not installed
- Model download failed
- Insufficient memory
- Java version incompatible

**Solution:** Check logs for specific error message and address the root cause

## Performance Considerations

### Spark NLP vs Simplified Analysis

| Aspect | Spark NLP | Simplified |
|--------|-----------|------------|
| Accuracy | High (85-90%) | Medium (uses dataset labels) |
| Speed | Slower (model inference) | Faster (simple mapping) |
| Memory | Higher (4GB+) | Lower (2GB) |
| Setup | Complex (requires Java, models) | Simple (no dependencies) |
| Use Case | Production, real tweets | Development, testing |

### Optimization Tips

1. **Use sampling for development:**
   ```bash
   python spark_processor.py --mode batch --sample-fraction 0.1
   ```

2. **Increase parallelism:**
   ```python
   df = df.repartition(8)  # Adjust based on CPU cores
   ```

3. **Cache intermediate results:**
   ```python
   df_transformed = pipeline_model.transform(df).cache()
   ```

## Integration with Existing Code

The implementation is **backward compatible**:

- ✅ Existing code continues to work without changes
- ✅ Automatic fallback if Spark NLP unavailable
- ✅ Same output schema (`sentiment` column)
- ✅ No changes required in `build_hashtag_cooccurrence()` or dashboard

## Next Steps

1. **Install Spark NLP**: `pip install spark-nlp==5.1.4`
2. **Test with sample**: `python spark_processor.py --mode batch --sample-fraction 0.01`
3. **Monitor logs**: Check for "Using Spark NLP for sentiment analysis"
4. **Compare results**: Run with and without Spark NLP to see accuracy differences
5. **Optimize**: Adjust threshold and memory settings for your use case

## References

- [Spark NLP Documentation](https://nlp.johnsnowlabs.com/)
- [Spark NLP Models Hub](https://nlp.johnsnowlabs.com/models)
- [SentimentDL Model Details](https://nlp.johnsnowlabs.com/2021/01/18/sentimentdl_use_twitter_en.html)
- [Spark NLP GitHub](https://github.com/JohnSnowLabs/spark-nlp)

