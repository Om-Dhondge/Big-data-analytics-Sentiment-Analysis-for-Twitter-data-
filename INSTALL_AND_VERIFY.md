# Installation and Verification Guide

## 🚀 Complete Installation Steps

### Prerequisites Check

Before installing Spark NLP, verify you have:

```bash
# Check Python version (3.7+)
python --version

# Check Java version (8 or 11)
java -version

# Check pip
pip --version
```

**Required:**
- Python 3.7 or higher
- Java 8 or 11 (NOT Java 17+)
- pip (latest version)
- 4GB+ RAM available

### Step 1: Install Spark NLP

```bash
# Install Spark NLP
pip install spark-nlp==5.1.4

# Verify installation
python -c "import sparknlp; print('✅ Spark NLP version:', sparknlp.version())"
```

**Expected Output:**
```
✅ Spark NLP version: 5.1.4
```

### Step 2: Verify PySpark Compatibility

```bash
# Check PySpark version
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
```

**Expected Output:**
```
PySpark version: 3.4.0
```

### Step 3: Test Spark NLP Import

```bash
# Test all required imports
python -c "
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, SentimentDLModel
from pyspark.ml import Pipeline
print('✅ All Spark NLP components imported successfully!')
"
```

**Expected Output:**
```
✅ All Spark NLP components imported successfully!
```

## 🧪 Verification Tests

### Test 1: Quick Import Test

Create a file `test_import.py`:

```python
#!/usr/bin/env python
"""Quick test to verify Spark NLP is working"""

try:
    import sparknlp
    print(f"✅ Spark NLP version: {sparknlp.version()}")
    
    from sparknlp.base import DocumentAssembler, Finisher
    print("✅ DocumentAssembler imported")
    
    from sparknlp.annotator import Tokenizer, SentimentDLModel
    print("✅ Tokenizer and SentimentDLModel imported")
    
    from pyspark.ml import Pipeline
    print("✅ Pipeline imported")
    
    print("\n🎉 All imports successful! Spark NLP is ready to use.")
    
except ImportError as e:
    print(f"❌ Import failed: {e}")
    print("\nPlease install Spark NLP:")
    print("  pip install spark-nlp==5.1.4")
```

Run it:
```bash
python test_import.py
```

### Test 2: Pipeline Creation Test

Create a file `test_pipeline.py`:

```python
#!/usr/bin/env python
"""Test Spark NLP pipeline creation"""

import sys
import os
from pyspark.sql import SparkSession

# Set Python path for Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create Spark session
spark = SparkSession.builder \
    .appName("PipelineTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

try:
    from sparknlp.base import DocumentAssembler, Finisher
    from sparknlp.annotator import Tokenizer, SentimentDLModel
    from pyspark.ml import Pipeline
    
    print("Creating Spark NLP pipeline...")
    
    # Create pipeline components
    document_assembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")
    
    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")
    
    print("✅ Pipeline components created")
    
    # Note: Model download happens on first use
    print("\n⚠️  Note: First run will download the model (~400MB)")
    print("This may take 2-5 minutes depending on your internet speed.")
    
    print("\n🎉 Pipeline creation test passed!")
    
except Exception as e:
    print(f"❌ Pipeline creation failed: {e}")
    
finally:
    spark.stop()
```

Run it:
```bash
python test_pipeline.py
```

### Test 3: Full Implementation Test

Run the comprehensive test suite:

```bash
python test_spark_nlp.py
```

**Expected Output:**
```
================================================================================
SPARK NLP IMPLEMENTATION TEST SUITE
================================================================================

================================================================================
TEST 1: Testing Spark NLP Pipeline Setup
================================================================================
INFO - Setting up Spark NLP pipeline...
INFO - Spark NLP pipeline created successfully with DocumentAssembler, Tokenizer, and SentimentDL Model
✅ SUCCESS: Spark NLP pipeline created successfully!

================================================================================
TEST 2: Testing Sentiment Processing
================================================================================
INFO - Creating sample tweet data...
INFO - Processing sentiment analysis...
INFO - Using Spark NLP for sentiment analysis...
✅ Sentiment analysis completed!

================================================================================
TEST SUMMARY
================================================================================
Pipeline Setup: ✅ PASSED
Sentiment Processing: ✅ PASSED
Real Data Test: ✅ PASSED

Total: 3/3 tests passed

🎉 All tests passed successfully!
```

## 🔍 Troubleshooting Common Issues

### Issue 1: "No module named 'sparknlp'"

**Symptom:**
```
ImportError: No module named 'sparknlp'
```

**Solution:**
```bash
# Install Spark NLP
pip install spark-nlp==5.1.4

# If using conda
conda install -c johnsnowlabs spark-nlp
```

### Issue 2: Java Version Issues

**Symptom:**
```
Java version not compatible
```

**Check Java version:**
```bash
java -version
```

**Solution:**
- Install Java 8 or 11 (NOT Java 17+)
- Download from: https://adoptium.net/
- Set JAVA_HOME environment variable

**Windows:**
```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-11"
```

**Linux/Mac:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

### Issue 3: Model Download Fails

**Symptom:**
```
Cannot download model: sentimentdl_use_twitter
```

**Solutions:**

1. **Check internet connection**
2. **Wait for download** (2-5 minutes, ~400MB)
3. **Check disk space** (need ~500MB free)
4. **Manual download:**
   ```bash
   # Download model manually
   python -c "
   from sparknlp.annotator import SentimentDLModel
   model = SentimentDLModel.pretrained('sentimentdl_use_twitter', 'en')
   print('Model downloaded successfully!')
   "
   ```

### Issue 4: Memory Issues

**Symptom:**
```
OutOfMemoryError: Java heap space
```

**Solution:**
```bash
# Increase driver memory
export PYSPARK_DRIVER_MEMORY=4g

# Or set in code
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Issue 5: PySpark Version Conflict

**Symptom:**
```
Spark NLP requires PySpark 3.x
```

**Solution:**
```bash
# Check current version
pip show pyspark

# Upgrade if needed
pip install --upgrade pyspark==3.4.0

# Reinstall Spark NLP
pip install --force-reinstall spark-nlp==5.1.4
```

## ✅ Verification Checklist

Use this checklist to ensure everything is working:

- [ ] Python 3.7+ installed
- [ ] Java 8 or 11 installed (check: `java -version`)
- [ ] PySpark 3.4.0 installed (check: `pip show pyspark`)
- [ ] Spark NLP 5.1.4 installed (check: `pip show spark-nlp`)
- [ ] Can import sparknlp (test: `python -c "import sparknlp"`)
- [ ] Can import DocumentAssembler
- [ ] Can import Tokenizer
- [ ] Can import SentimentDLModel
- [ ] Can create Pipeline
- [ ] test_import.py runs successfully
- [ ] test_pipeline.py runs successfully
- [ ] test_spark_nlp.py runs successfully
- [ ] spark_processor.py runs with Spark NLP

## 🎯 Quick Verification Command

Run this single command to verify everything:

```bash
python -c "
import sys
import os
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Test').getOrCreate()

try:
    import sparknlp
    from sparknlp.base import DocumentAssembler, Finisher
    from sparknlp.annotator import Tokenizer, SentimentDLModel
    from pyspark.ml import Pipeline
    
    print('✅ Spark NLP version:', sparknlp.version())
    print('✅ All components imported successfully')
    print('✅ Ready to use Spark NLP!')
    
except Exception as e:
    print('❌ Error:', e)
finally:
    spark.stop()
"
```

## 📊 Performance Benchmarks

After installation, you can benchmark performance:

```bash
# Test with small sample
time python spark_processor.py --mode batch --sample-fraction 0.001

# Expected times:
# - First run: 3-6 minutes (includes model download)
# - Subsequent runs: 30-60 seconds
```

## 🔄 Update Instructions

To update Spark NLP to a newer version:

```bash
# Check current version
pip show spark-nlp

# Update to latest
pip install --upgrade spark-nlp

# Or specific version
pip install spark-nlp==5.2.0
```

## 🆘 Getting Help

If you encounter issues:

1. **Check logs:** Look for error messages in console output
2. **Run tests:** Use `test_spark_nlp.py` to identify specific issues
3. **Check documentation:** See `SPARK_NLP_SETUP.md` for detailed info
4. **Verify environment:** Ensure Java, Python, PySpark versions are correct
5. **Check resources:** Ensure sufficient memory (4GB+) available

## 📚 Additional Resources

- **Spark NLP Documentation:** https://nlp.johnsnowlabs.com/
- **Installation Guide:** https://nlp.johnsnowlabs.com/docs/en/install
- **Models Hub:** https://nlp.johnsnowlabs.com/models
- **GitHub Issues:** https://github.com/JohnSnowLabs/spark-nlp/issues

## 🎉 Success Indicators

You'll know everything is working when:

1. ✅ All import tests pass
2. ✅ Pipeline creation succeeds
3. ✅ Model downloads successfully (first run)
4. ✅ Test suite passes all tests
5. ✅ spark_processor.py logs show "Using Spark NLP for sentiment analysis"
6. ✅ Results include sentiment predictions

## 🚀 Next Steps After Verification

Once verified, you can:

1. **Run with sample data:**
   ```bash
   python spark_processor.py --mode batch --sample-fraction 0.01
   ```

2. **View results in dashboard:**
   ```bash
   python dashboard.py
   ```

3. **Process full dataset:**
   ```bash
   python spark_processor.py --mode batch
   ```

4. **Experiment with configurations:**
   - Adjust threshold in `spark_processor.py`
   - Try different models
   - Optimize memory settings

---

**Installation complete!** You're now ready to use Spark NLP for sentiment analysis. 🎉

