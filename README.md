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

📖 Quick Start

from datapipeline import DataPipeline, normalize_data, square_numbers

# Create a pipeline with multiple steps
pipeline = DataPipeline()
pipeline.add_step(lambda x: [i for i in x if i > 0], "filter_positive")
pipeline.add_step(square_numbers, "square")
pipeline.add_step(normalize_data, "normalize")

# Execute with sample data
data = [-2, -1, 0, 1, 2, 3, 4, 5]
result = pipeline.execute(data, verbose=True)

# Output with verbose mode:
# Starting pipeline with 3 steps
# Initial data: list with 8 elements
# 
# Step 1: filter_positive
#   Output: list with 5 elements
# 
# Step 2: square
#   Output: list with 5 elements
# 
# Step 3: normalize
#   Output: list with 5 elements

print(result)
# [0.0, 0.07142857142857142, 0.2857142857142857, 0.6428571428571429, 1.0]