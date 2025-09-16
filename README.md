# PipelineHub

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


A flexible Python library for creating custom data processing workflows with ease.

## ✨ Features

- 🔧 **Flexible**: Add any callable function as a processing step
- 🔗 **Chainable**: Fluent method chaining for clean, readable code
- 🐛 **Debuggable**: Verbose mode shows data flow between steps
- 🧪 **Testable**: Clear error handling with step identification
- 📦 **Lightweight**: Zero external dependencies
- 🎯 **Type-friendly**: Full type hints for better IDE support
- 🚀 **Performance**: Minimal overhead for maximum speed
- 🔄 **Reusable**: Create pipelines once, use with different datasets

## Installation
```bash
pip install pipelinehub
```

## 📖 Quick Start
```python
from pipelinehub import DataPipeline, normalize_data, square_numbers

# Create a pipeline with multiple steps
pipeline = DataPipeline()
pipeline.add_step(lambda x: [i for i in x if i > 0], "filter_positive")
pipeline.add_step(square_numbers, "square")
pipeline.add_step(normalize_data, "normalize")

# Execute with sample data
data = [-2, -1, 0, 1, 2, 3, 4, 5]
result = pipeline.execute(data, verbose=True)

print(result)
```
## 🔗 Method Chaining
Create pipelines fluently with method chaining:

```python
from pipelinehub import DataPipeline, add_constant

# Chain operations together
result = (DataPipeline()
          .add_step(lambda x: [i for i in x if i % 2 == 0], "filter_even")
          .add_step(add_constant(10), "add_10")  
          .add_step(lambda x: sorted(x, reverse=True), "sort_desc")
          .execute([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))

print(result) 
```

## 📚 Comprehensive Examples

### Data Cleaning Pipeline
```python 
from datapipeline import DataPipeline, outlier_removal, normalize_data, calculate_stats

# Create a data cleaning pipeline
cleaning_pipeline = (DataPipeline()
    .add_step(lambda x: [float(i) for i in x if i is not None], "convert_and_filter")
    .add_step(lambda x: outlier_removal(x, threshold=2.5), "remove_outliers") 
    .add_step(normalize_data, "normalize")
    .add_step(calculate_stats, "final_stats"))

# Process messy data
messy_data = [1, 2, 3, None, 100, 4, 5, 6, 7, 8, 9]
stats = cleaning_pipeline.execute(messy_data, verbose=True)
print(stats)
```
### Text Processing Pipeline
```python
import re
from datapipeline import DataPipeline

def clean_text(text):
    """Remove special characters and extra whitespace."""
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return ' '.join(text.split())

def extract_keywords(words, min_length=4):
    """Extract words longer than min_length."""
    return [word for word in words if len(word) >= min_length]

# Build text processing pipeline
text_pipeline = (DataPipeline()
    .add_step(str.lower, "lowercase")
    .add_step(clean_text, "clean")
    .add_step(str.split, "tokenize") 
    .add_step(lambda words: extract_keywords(words, min_length=4), "extract_keywords")
    .add_step(lambda words: sorted(set(words)), "unique_and_sort"))

# Process text
text = "Hello World! This is a Sample Text for Processing... With special chars!!!"
keywords = text_pipeline.execute(text, verbose=True)
print(keywords)
```


