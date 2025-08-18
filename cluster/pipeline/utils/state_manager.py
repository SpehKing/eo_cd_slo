#!/usr/bin/env python3
"""
Pipeline State Management

Handles checkpoint files for resumable pipeline execution.
Tracks progress of downloads, insertions, and BTC processing.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, asdict
from enum import Enum

from ..config.settings import config


class TaskStatus(Enum):
    """Task status enumeration"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskInfo:
    """Information about a specific task"""

    task_id: str
    status: TaskStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "error_message": self.error_message,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskInfo":
        """Create from dictionary"""
        return cls(
            task_id=data["task_id"],
            status=TaskStatus(data["status"]),
            started_at=(
                datetime.fromisoformat(data["started_at"])
                if data["started_at"]
                else None
            ),
            completed_at=(
                datetime.fromisoformat(data["completed_at"])
                if data["completed_at"]
                else None
            ),
            error_message=data.get("error_message"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class StageCheckpoint:
    """Checkpoint for a pipeline stage"""

    stage_name: str
    year: Optional[int]
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    skipped_tasks: int
    tasks: Dict[str, TaskInfo]
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "stage_name": self.stage_name,
            "year": self.year,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "skipped_tasks": self.skipped_tasks,
            "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StageCheckpoint":
        """Create from dictionary"""
        tasks = {k: TaskInfo.from_dict(v) for k, v in data["tasks"].items()}
        return cls(
            stage_name=data["stage_name"],
            year=data.get("year"),
            total_tasks=data["total_tasks"],
            completed_tasks=data["completed_tasks"],
            failed_tasks=data["failed_tasks"],
            skipped_tasks=data["skipped_tasks"],
            tasks=tasks,
            started_at=(
                datetime.fromisoformat(data["started_at"])
                if data["started_at"]
                else None
            ),
            completed_at=(
                datetime.fromisoformat(data["completed_at"])
                if data["completed_at"]
                else None
            ),
        )

    @property
    def is_completed(self) -> bool:
        """Check if stage is completed"""
        return self.completed_tasks + self.skipped_tasks == self.total_tasks

    @property
    def progress_percentage(self) -> float:
        """Get progress percentage"""
        if self.total_tasks == 0:
            return 100.0
        return ((self.completed_tasks + self.skipped_tasks) / self.total_tasks) * 100.0


class StateManager:
    """Manages pipeline state and checkpoints"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.StateManager")
        self.checkpoints: Dict[str, StageCheckpoint] = {}

    def load_checkpoint(
        self, stage: str, year: Optional[int] = None
    ) -> Optional[StageCheckpoint]:
        """Load checkpoint for a specific stage"""
        try:
            checkpoint_file = config.get_checkpoint_file(stage, year)

            if not checkpoint_file.exists():
                self.logger.info(
                    f"No checkpoint found for {stage}" + (f"_{year}" if year else "")
                )
                return None

            with open(checkpoint_file, "r") as f:
                data = json.load(f)

            checkpoint = StageCheckpoint.from_dict(data)
            key = f"{stage}_{year}" if year else stage
            self.checkpoints[key] = checkpoint

            self.logger.info(
                f"Loaded checkpoint for {stage}"
                + (f"_{year}" if year else "")
                + f": {checkpoint.completed_tasks}/{checkpoint.total_tasks} completed"
            )

            return checkpoint

        except Exception as e:
            self.logger.error(f"Failed to load checkpoint for {stage}: {e}")
            return None

    def save_checkpoint(self, checkpoint: StageCheckpoint):
        """Save checkpoint to file"""
        try:
            key = (
                f"{checkpoint.stage_name}_{checkpoint.year}"
                if checkpoint.year
                else checkpoint.stage_name
            )
            self.checkpoints[key] = checkpoint

            checkpoint_file = config.get_checkpoint_file(
                checkpoint.stage_name, checkpoint.year
            )

            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint.to_dict(), f, indent=2)

            self.logger.debug(
                f"Saved checkpoint for {checkpoint.stage_name}"
                + (f"_{checkpoint.year}" if checkpoint.year else "")
                + f": {checkpoint.completed_tasks}/{checkpoint.total_tasks} completed"
            )

        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    def create_stage_checkpoint(
        self, stage_name: str, year: Optional[int], task_ids: List[str]
    ) -> StageCheckpoint:
        """Create a new stage checkpoint"""
        tasks = {
            task_id: TaskInfo(task_id=task_id, status=TaskStatus.PENDING)
            for task_id in task_ids
        }

        checkpoint = StageCheckpoint(
            stage_name=stage_name,
            year=year,
            total_tasks=len(task_ids),
            completed_tasks=0,
            failed_tasks=0,
            skipped_tasks=0,
            tasks=tasks,
            started_at=datetime.now(),
        )

        self.save_checkpoint(checkpoint)
        return checkpoint

    def update_task_status(
        self,
        stage_name: str,
        year: Optional[int],
        task_id: str,
        status: TaskStatus,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Update status of a specific task"""
        key = f"{stage_name}_{year}" if year else stage_name

        if key not in self.checkpoints:
            self.logger.error(f"No checkpoint found for {key}")
            return

        checkpoint = self.checkpoints[key]

        if task_id not in checkpoint.tasks:
            self.logger.error(f"Task {task_id} not found in checkpoint {key}")
            return

        task = checkpoint.tasks[task_id]
        old_status = task.status

        # Update task info
        task.status = status
        if status == TaskStatus.RUNNING:
            task.started_at = datetime.now()
        elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED]:
            task.completed_at = datetime.now()
            if error_message:
                task.error_message = error_message
            if metadata:
                task.metadata = metadata

        # Update checkpoint counters
        if old_status == TaskStatus.COMPLETED:
            checkpoint.completed_tasks -= 1
        elif old_status == TaskStatus.FAILED:
            checkpoint.failed_tasks -= 1
        elif old_status == TaskStatus.SKIPPED:
            checkpoint.skipped_tasks -= 1

        if status == TaskStatus.COMPLETED:
            checkpoint.completed_tasks += 1
        elif status == TaskStatus.FAILED:
            checkpoint.failed_tasks += 1
        elif status == TaskStatus.SKIPPED:
            checkpoint.skipped_tasks += 1

        # Check if stage is completed
        if checkpoint.is_completed and not checkpoint.completed_at:
            checkpoint.completed_at = datetime.now()

        self.save_checkpoint(checkpoint)

        self.logger.debug(
            f"Updated task {task_id} status: {old_status.value} -> {status.value}"
        )

    def get_pending_tasks(self, stage_name: str, year: Optional[int]) -> List[str]:
        """Get list of pending task IDs for a stage"""
        key = f"{stage_name}_{year}" if year else stage_name

        if key not in self.checkpoints:
            return []

        checkpoint = self.checkpoints[key]
        return [
            task_id
            for task_id, task in checkpoint.tasks.items()
            if task.status == TaskStatus.PENDING
        ]

    def get_failed_tasks(self, stage_name: str, year: Optional[int]) -> List[str]:
        """Get list of failed task IDs for a stage"""
        key = f"{stage_name}_{year}" if year else stage_name

        if key not in self.checkpoints:
            return []

        checkpoint = self.checkpoints[key]
        return [
            task_id
            for task_id, task in checkpoint.tasks.items()
            if task.status == TaskStatus.FAILED
        ]

    def is_stage_completed(
        self, stage: str, year: int, grid_id: Optional[str] = None
    ) -> bool:
        """Check if a specific stage is completed"""
        # Support both old and new stage naming
        if stage == "download_and_insert":
            # For the new combined stage, check if either the combined stage is completed
            # or if both download and insert stages are completed
            combined_completed = self._check_stage_completion(
                "download_and_insert", year, grid_id
            )
            if combined_completed:
                return True

            # Fall back to checking both old stages
            download_completed = self._check_stage_completion("download", year, grid_id)
            insert_completed = self._check_stage_completion("insert", year, grid_id)
            return download_completed and insert_completed

        return self._check_stage_completion(stage, year, grid_id)

    def _check_stage_completion(
        self, stage: str, year: int, grid_id: Optional[str] = None
    ) -> bool:
        """Helper method to check if a specific stage is completed"""
        key = f"{stage}_{year}" if year else stage

        if key not in self.checkpoints:
            return False

        return self.checkpoints[key].is_completed

    def mark_stage_completed(self, stage_name: str, year: Optional[int]):
        """Mark a stage as completed"""
        key = f"{stage_name}_{year}" if year else stage_name

        if key not in self.checkpoints:
            self.checkpoints[key] = StageCheckpoint(
                stage_name=stage_name,
                year=year,
                status=TaskStatus.COMPLETED,
                last_updated=datetime.now(),
                progress=1.0,
                metadata={},
            )
        else:
            self.checkpoints[key].status = TaskStatus.COMPLETED
            self.checkpoints[key].progress = 1.0
            self.checkpoints[key].last_updated = datetime.now()

        self.save_checkpoints()
        self.logger.info(f"Marked stage {stage_name} for year {year} as completed")

    def get_stage_progress(
        self, stage_name: str, year: Optional[int]
    ) -> Dict[str, Any]:
        """Get progress information for a stage"""
        key = f"{stage_name}_{year}" if year else stage_name

        if key not in self.checkpoints:
            return {
                "stage": stage_name,
                "year": year,
                "progress": 0.0,
                "total": 0,
                "completed": 0,
                "failed": 0,
                "skipped": 0,
                "status": "not_started",
            }

        checkpoint = self.checkpoints[key]
        return {
            "stage": stage_name,
            "year": year,
            "progress": checkpoint.progress_percentage,
            "total": checkpoint.total_tasks,
            "completed": checkpoint.completed_tasks,
            "failed": checkpoint.failed_tasks,
            "skipped": checkpoint.skipped_tasks,
            "status": "completed" if checkpoint.is_completed else "in_progress",
        }

    def get_all_progress(self) -> Dict[str, Any]:
        """Get progress information for all stages"""
        progress = {}
        for key, checkpoint in self.checkpoints.items():
            progress[key] = {
                "stage": checkpoint.stage_name,
                "year": checkpoint.year,
                "progress": checkpoint.progress_percentage,
                "total": checkpoint.total_tasks,
                "completed": checkpoint.completed_tasks,
                "failed": checkpoint.failed_tasks,
                "skipped": checkpoint.skipped_tasks,
                "status": "completed" if checkpoint.is_completed else "in_progress",
            }
        return progress

    def reset_failed_tasks(self, stage_name: str, year: Optional[int]):
        """Reset all failed tasks to pending status"""
        key = f"{stage_name}_{year}" if year else stage_name

        if key not in self.checkpoints:
            return

        checkpoint = self.checkpoints[key]
        reset_count = 0

        for task_id, task in checkpoint.tasks.items():
            if task.status == TaskStatus.FAILED:
                task.status = TaskStatus.PENDING
                task.error_message = None
                task.started_at = None
                task.completed_at = None
                reset_count += 1

        checkpoint.failed_tasks = 0
        checkpoint.completed_at = None  # Stage is no longer completed

        self.save_checkpoint(checkpoint)
        self.logger.info(f"Reset {reset_count} failed tasks in {key}")

    def reset_all_failed_tasks(self):
        """Reset all failed tasks across all stages"""
        total_reset = 0

        # Reset failed tasks in loaded checkpoints
        for checkpoint in self.checkpoints.values():
            reset_count = 0
            for task_id, task in checkpoint.tasks.items():
                if task.status == TaskStatus.FAILED:
                    task.status = TaskStatus.PENDING
                    task.error_message = None
                    task.started_at = None
                    task.completed_at = None
                    reset_count += 1

            if reset_count > 0:
                checkpoint.failed_tasks = 0
                checkpoint.completed_at = None
                self.save_checkpoint(checkpoint)
                total_reset += reset_count

        # Also check checkpoint files on disk for stages not yet loaded
        checkpoint_dir = config.checkpoint_dir
        if checkpoint_dir.exists():
            for checkpoint_file in checkpoint_dir.glob("*.json"):
                stage_key = checkpoint_file.stem
                if stage_key not in self.checkpoints:
                    try:
                        with open(checkpoint_file, "r") as f:
                            data = json.load(f)

                        checkpoint = StageCheckpoint.from_dict(data)
                        reset_count = 0

                        for task_id, task in checkpoint.tasks.items():
                            if task.status == TaskStatus.FAILED:
                                task.status = TaskStatus.PENDING
                                task.error_message = None
                                task.started_at = None
                                task.completed_at = None
                                reset_count += 1

                        if reset_count > 0:
                            checkpoint.failed_tasks = 0
                            checkpoint.completed_at = None

                            with open(checkpoint_file, "w") as f:
                                json.dump(checkpoint.to_dict(), f, indent=2)

                            total_reset += reset_count

                    except Exception as e:
                        self.logger.warning(
                            f"Failed to process checkpoint file {checkpoint_file}: {e}"
                        )

        self.logger.info(f"Reset {total_reset} failed tasks across all stages")
        return total_reset


# Global state manager instance
state_manager = StateManager()
