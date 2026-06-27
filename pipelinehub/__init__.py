"""
PipelineHub - A library for creating custom data processing workflows.
"""

from .pipeline import DataPipeline
from .errors import PipelineStepError
from .agent_pipeline import AgentPipeline
from .utils import (
    filter_numbers,
    square_numbers,
    sum_data,
    normalize_data,
    add_constant,
    calculate_stats,
    outlier_removal,
)

__version__ = "0.1.19"
__author__ = "Rahul Paul"
__email__ = "paul.rahulxj100@gmail.com"

__all__ = [
    "DataPipeline",
    "PipelineStepError",
    "AgentPipeline",
    "filter_numbers",
    "square_numbers", 
    "sum_data",
    "normalize_data",
    "add_constant",
    "calculate_stats",
    "outlier_removal",
]