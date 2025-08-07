#!/usr/bin/env python3
"""
Pipeline Package Initialization

Makes the pipeline directory a proper Python package.
"""

from .controller import PipelineController, run_pipeline_async, run_pipeline_sync
from .config.settings import config, ProcessingMode, LogLevel
from .utils.state_manager import state_manager, TaskStatus
from .utils.monitor import monitor

__version__ = "1.0.0"
__author__ = "EO Change Detection Team"

__all__ = [
    "PipelineController",
    "run_pipeline_async",
    "run_pipeline_sync",
    "config",
    "ProcessingMode",
    "LogLevel",
    "state_manager",
    "TaskStatus",
    "monitor",
]
