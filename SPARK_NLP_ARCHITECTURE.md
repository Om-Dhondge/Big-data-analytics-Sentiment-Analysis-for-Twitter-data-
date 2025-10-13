# Spark NLP Pipeline Architecture

## 🏗️ Pipeline Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SPARK NLP PIPELINE                              │
└─────────────────────────────────────────────────────────────────────────┘

Input DataFrame
     │
     │ Column: "text" (raw tweet text)
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STAGE 1: DocumentAssembler                                             │
│  ─────────────────────────────────────────────────────────────────────  │
│  • Input:  "text" column                                                │
│  • Output: "document" column (Spark NLP document format)                │
│  • Action: Converts raw text to structured document                     │
│  • Cleanup: Removes extra whitespace (shrink mode)                      │
└─────────────────────────────────────────────────────────────────────────┘
     │
     │ Column: "document" (structured format)
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STAGE 2: Tokenizer                                                     │
│  ─────────────────────────────────────────────────────────────────────  │
│  • Input:  "document" column                                            │
│  • Output: "token" column (array of tokens)                             │
│  • Action: Splits text into individual words/tokens                     │
│  • Example: "I love this!" → ["I", "love", "this", "!"]                │
└─────────────────────────────────────────────────────────────────────────┘
     │
     │ Columns: "document" + "token"
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STAGE 3: SentimentDL Model                                             │
│  ─────────────────────────────────────────────────────────────────────  │
│  • Model:  sentimentdl_use_twitter (pre-trained)                        │
│  • Input:  "document" + "token" columns                                 │
│  • Output: "sentiment_result" column (annotations)                      │
│  • Action: Deep learning sentiment classification                       │
│  • Classes: positive, negative, neutral                                 │
│  • Threshold: 0.6 (confidence level)                                    │
└─────────────────────────────────────────────────────────────────────────┘
     │
     │ Column: "sentiment_result" (annotations)
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STAGE 4: Finisher                                                      │
│  ─────────────────────────────────────────────────────────────────────  │
│  • Input:  "sentiment_result" column                                    │
│  • Output: "sentiment_output" column (readable string)                  │
│  • Action: Converts annotations to simple string format                 │
│  • Example: [Annotation(...)] → "positive"                              │
└─────────────────────────────────────────────────────────────────────────┘
     │
     │ Column: "sentiment_output" (string)
     │
     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  POST-PROCESSING: Extract Sentiment                                     │
│  ─────────────────────────────────────────────────────────────────────  │
│  • Input:  "sentiment_output" column                                    │
│  • Output: "sentiment" column (standardized)                            │
│  • Action: Maps output to standard labels                               │
│  • Mapping: "positive" → "positive"                                     │
│            "negative" → "negative"                                      │
│            other → "neutral"                                            │
└─────────────────────────────────────────────────────────────────────────┘
     │
     │ Final Column: "sentiment"
     │
     ▼
Output DataFrame
(with sentiment analysis results)
```

## 🔄 Function Call Flow

```
main()
  │
  ├─> SparkProcessor.__init__()
  │     └─> Creates Spark session
  │
  └─> process_batch()
        │
        ├─> data_loader.load_data()
        │     └─> Returns DataFrame with "text" column
        │
        ├─> process_sentiment(df)
        │     │
        │     ├─> setup_spark_nlp()
        │     │     │
        │     │     ├─> Import Spark NLP components
        │     │     │     ├─> DocumentAssembler
        │     │     │     ├─> Tokenizer
        │     │     │     ├─> SentimentDLModel
        │     │     │     └─> Finisher
        │     │     │
        │     │     ├─> Configure each component
        │     │     │
        │     │     ├─> Create Pipeline with all stages
        │     │     │
        │     │     └─> Return pipeline (or None if error)
        │     │
        │     ├─> If pipeline exists:
        │     │     ├─> pipeline.fit(df)
        │     │     ├─> pipeline_model.transform(df)
        │     │     ├─> Extract sentiment from output
        │     │     └─> Return df_with_sentiment
        │     │
        │     └─> Else (fallback):
        │           ├─> Use original_sentiment column
        │           └─> Return df_with_sentiment
        │
        └─> build_hashtag_cooccurrence()
              └─> Uses sentiment column for analysis
```

## 📊 Data Flow Example

### Input Tweet:
```
"I love this product! It's amazing! #happy #excited"
```

### Stage-by-Stage Transformation:

**1. DocumentAssembler Output:**
```
document: [
  {
    annotatorType: "document",
    begin: 0,
    end: 52,
    result: "I love this product! It's amazing! #happy #excited",
    metadata: {...}
  }
]
```

**2. Tokenizer Output:**
```
token: [
  {annotatorType: "token", begin: 0, end: 0, result: "I"},
  {annotatorType: "token", begin: 2, end: 5, result: "love"},
  {annotatorType: "token", begin: 7, end: 10, result: "this"},
  {annotatorType: "token", begin: 12, end: 18, result: "product"},
  ...
]
```

**3. SentimentDL Model Output:**
```
sentiment_result: [
  {
    annotatorType: "sentiment",
    begin: 0,
    end: 52,
    result: "positive",
    metadata: {
      "confidence": "0.9876"
    }
  }
]
```

**4. Finisher Output:**
```
sentiment_output: "positive"
```

**5. Final Output:**
```
sentiment: "positive"
```

## 🔀 Error Handling Flow

```
setup_spark_nlp()
  │
  ├─> Try:
  │     ├─> Import Spark NLP
  │     ├─> Create pipeline
  │     └─> Return pipeline ✅
  │
  └─> Catch:
        ├─> ImportError (Spark NLP not installed)
        │     └─> Log warning → Return None
        │
        └─> Exception (Other errors)
              └─> Log error → Return None

process_sentiment(df)
  │
  ├─> pipeline = setup_spark_nlp()
  │
  ├─> If pipeline is not None:
  │     │
  │     ├─> Try:
  │     │     ├─> Fit pipeline
  │     │     ├─> Transform data
  │     │     └─> Return results ✅
  │     │
  │     └─> Catch Exception:
  │           └─> Log error → Use fallback
  │
  └─> Fallback Mode:
        ├─> Use original_sentiment column
        └─> Return results ✅
```

## 🎯 Component Responsibilities

### DocumentAssembler
- **Purpose:** Entry point for Spark NLP
- **Input:** Raw text string
- **Output:** Structured document annotation
- **Key Feature:** Handles text cleanup and normalization

### Tokenizer
- **Purpose:** Text segmentation
- **Input:** Document annotation
- **Output:** Array of token annotations
- **Key Feature:** Preserves position information for each token

### SentimentDL Model
- **Purpose:** Sentiment classification
- **Input:** Document + tokens
- **Output:** Sentiment prediction with confidence
- **Key Feature:** Deep learning model trained on Twitter data

### Finisher
- **Purpose:** Format conversion
- **Input:** Spark NLP annotations
- **Output:** Simple string values
- **Key Feature:** Makes results easy to use in downstream processing

## 🔧 Configuration Points

### 1. DocumentAssembler
```python
.setInputCol("text")           # Change input column name
.setOutputCol("document")      # Change output column name
.setCleanupMode("shrink")      # Options: shrink, disabled, each
```

### 2. Tokenizer
```python
.setInputCols(["document"])    # Input columns
.setOutputCol("token")         # Output column name
```

### 3. SentimentDL Model
```python
.pretrained("sentimentdl_use_twitter", "en")  # Model name and language
.setInputCols(["document", "token"])          # Required inputs
.setOutputCol("sentiment_result")             # Output column
.setThreshold(0.6)                            # Confidence threshold (0.0-1.0)
```

### 4. Finisher
```python
.setInputCols(["sentiment_result"])  # Input columns
.setOutputCols(["sentiment_output"]) # Output column names
.setOutputAsArray(False)             # Single value vs array
.setCleanAnnotations(False)          # Include metadata
```

## 📈 Performance Characteristics

### Pipeline Creation (First Time)
```
setup_spark_nlp()
  ├─> Import libraries: ~1 second
  ├─> Download model: ~2-5 minutes (400MB)
  ├─> Load model: ~10-30 seconds
  └─> Create pipeline: ~1 second
  
Total: ~3-6 minutes (first run only)
```

### Pipeline Creation (Subsequent)
```
setup_spark_nlp()
  ├─> Import libraries: ~1 second
  ├─> Load cached model: ~5-10 seconds
  └─> Create pipeline: ~1 second
  
Total: ~7-12 seconds
```

### Sentiment Processing
```
process_sentiment(df)
  ├─> Setup pipeline: ~7-12 seconds (cached)
  ├─> Fit pipeline: ~1-2 seconds
  ├─> Transform data: ~0.5-1 second per 1000 tweets
  └─> Extract results: ~0.1 second
  
Total: ~10-15 seconds + (0.5-1 sec per 1000 tweets)
```

## 🎨 Visual Summary

```
┌──────────────┐
│  Raw Text    │  "I love this product!"
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Document    │  [Annotation(document, 0, 21, ...)]
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Tokens      │  [Annotation(token, "I"), ...]
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Sentiment   │  [Annotation(sentiment, "positive", confidence=0.98)]
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Output      │  "positive"
└──────────────┘
```

## 🔍 Debugging Tips

### Check Pipeline Stages
```python
pipeline = setup_spark_nlp()
if pipeline:
    stages = pipeline.getStages()
    print(f"Pipeline has {len(stages)} stages:")
    for stage in stages:
        print(f"  - {stage.__class__.__name__}")
```

### Inspect Intermediate Results
```python
# After transform
df_transformed.select("text", "document", "token", "sentiment_result").show(1, truncate=False)
```

### Monitor Performance
```python
import time
start = time.time()
df_result = process_sentiment(df)
print(f"Processing took {time.time() - start:.2f} seconds")
```

---

This architecture ensures robust, scalable, and maintainable sentiment analysis for your tweet analytics pipeline!

