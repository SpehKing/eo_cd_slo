#!/usr/bin/env python3
"""
Real-time Pipeline Monitoring

Provides real-time monitoring and alerting capabilities for the pipeline.
Includes web interface, progress tracking, and logging.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import aiohttp
from aiohttp import web
from aiohttp.web import Response, Request
import aiofiles

from ..config.settings import config, ProcessingMode
from ..utils.state_manager import state_manager


class PipelineMonitor:
    """Real-time pipeline monitoring system"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.PipelineMonitor")
        self.app = web.Application()
        self.setup_routes()
        self.websocket_connections: List[web.WebSocketResponse] = []
        self.pipeline_stats = {
            "started_at": None,
            "current_stage": None,
            "current_year": None,
            "total_years": len(config.years),
            "total_stages": 3,  # download, insert, btc_process
            "overall_progress": 0.0,
            "status": "idle",
        }

        # Pipeline control state
        self.pipeline_controller: Optional[Any] = None
        self.start_requested = asyncio.Event()
        self.stop_requested = asyncio.Event()
        self.pause_requested = asyncio.Event()
        self.resume_requested = asyncio.Event()

    def setup_routes(self):
        """Setup web routes for monitoring interface"""
        self.app.router.add_get("/", self.index_handler)
        self.app.router.add_get("/api/status", self.status_handler)
        self.app.router.add_get("/api/progress", self.progress_handler)
        self.app.router.add_get("/api/logs/{component}", self.logs_handler)
        self.app.router.add_get("/ws", self.websocket_handler)
        self.app.router.add_post("/api/control/{action}", self.control_handler)

        # Configuration management endpoints
        self.app.router.add_get("/api/config", self.config_get_handler)
        self.app.router.add_post("/api/config", self.config_set_handler)

        # Grid status management endpoints
        self.app.router.add_get("/api/grid-status", self.grid_status_handler)
        self.app.router.add_post("/api/grid-status/check", self.grid_check_handler)

        # Static files
        self.app.router.add_static(
            "/", path=Path(__file__).parent / "static", name="static"
        )

    async def index_handler(self, request: Request) -> Response:
        """Serve monitoring dashboard"""
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>EO Change Detection Pipeline Monitor</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .stat-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .progress-bar { width: 100%; height: 20px; background: #ecf0f1; border-radius: 10px; overflow: hidden; }
        .progress-fill { height: 100%; background: #3498db; transition: width 0.3s ease; }
        .stage-progress { margin-bottom: 15px; }
        .stage-title { font-weight: bold; margin-bottom: 5px; }
        .logs { background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 8px; height: 300px; overflow-y: auto; font-family: monospace; }
        .control-buttons { margin: 20px 0; }
        .btn { padding: 10px 20px; margin: 5px; border: none; border-radius: 4px; cursor: pointer; transition: all 0.3s ease; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .btn-primary { background: #3498db; color: white; }
        .btn-primary:hover:not(:disabled) { background: #2980b9; }
        .btn-danger { background: #e74c3c; color: white; }
        .btn-danger:hover:not(:disabled) { background: #c0392b; }
        .btn-warning { background: #f39c12; color: white; }
        .btn-warning:hover:not(:disabled) { background: #e67e22; }
        .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
        .status.idle { background: #95a5a6; color: white; }
        .status.waiting_for_start { background: #f39c12; color: white; }
        .status.starting { background: #3498db; color: white; }
        .status.running { background: #27ae60; color: white; }
        .status.pausing { background: #f39c12; color: white; }
        .status.paused { background: #e67e22; color: white; }
        .status.stopping { background: #e74c3c; color: white; }
        .status.stopped { background: #7f8c8d; color: white; }
        .status.error { background: #e74c3c; color: white; }
        .status.completed { background: #2ecc71; color: white; }
        .config-section { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .config-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .config-field { margin-bottom: 15px; }
        .config-field label { display: block; font-weight: bold; margin-bottom: 5px; }
        .config-field input, .config-field select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
        .config-field input:focus, .config-field select:focus { outline: none; border-color: #3498db; }
        .config-buttons { margin-top: 15px; }
        .array-input { display: flex; align-items: center; gap: 10px; }
        .array-input input { flex: 1; }
        .help-text { font-size: 12px; color: #666; margin-top: 3px; }
        .config-message { padding: 10px; border-radius: 4px; margin-top: 10px; display: none; }
        .config-message.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .config-message.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .grid-status-section { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .grid-table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .grid-table th, .grid-table td { padding: 8px 12px; border: 1px solid #ddd; text-align: left; }
        .grid-table th { background: #f8f9fa; font-weight: bold; }
        .grid-table tbody tr:nth-child(even) { background: #f8f9fa; }
        .status-badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
        .status-completed { background: #d4edda; color: #155724; }
        .status-ready_for_processing { background: #fff3cd; color: #856404; }
        .status-ready_for_insert { background: #cce7ff; color: #004085; }
        .status-partially_downloaded { background: #f4cccc; color: #721c24; }
        .status-not_started { background: #e2e3e5; color: #383d41; }
        .year-status { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin: 0 2px; }
        .year-completed { background: #28a745; }
        .year-partial { background: #ffc107; }
        .year-missing { background: #dc3545; }
        .add-grid-section { margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 4px; }
        .inline-form { display: flex; gap: 10px; align-items: end; }
        .inline-form input { flex: 1; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>EO Change Detection Pipeline Monitor</h1>
            <div class="status" id="pipeline-status">Status: Idle</div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Overall Progress</h3>
                <div class="progress-bar">
                    <div class="progress-fill" id="overall-progress" style="width: 0%"></div>
                </div>
                <p id="overall-text">0%</p>
            </div>
            
            <div class="stat-card">
                <h3>Current Stage</h3>
                <p id="current-stage">-</p>
                <p id="current-year">-</p>
            </div>
            
            <div class="stat-card">
                <h3>Pipeline Stats</h3>
                <p>Years: <span id="years-total">-</span></p>
                <p>Grid Cells: <span id="grid-cells">-</span></p>
                <p>Started: <span id="started-at">-</span></p>
            </div>
        </div>
        
        <div class="config-section">
            <h3>Pipeline Configuration</h3>
            <div class="config-grid">
                <div>
                    <div class="config-field">
                        <label for="years-input">Years to Process:</label>
                        <div class="array-input">
                            <input type="text" id="years-input" placeholder="e.g., 2020:2024,2027 or * for all available">
                        </div>
                        <div class="help-text">Enter years/ranges separated by commas (2016-2030), ranges like 2020:2024, or * for all available (2016-2030)</div>
                    </div>
                    
                    <div class="config-field">
                        <label for="grid-ids-input">Grid IDs to Process:</label>
                        <div class="array-input">
                            <input type="text" id="grid-ids-input" placeholder="e.g., 465:467,500:502,600 or * for all available">
                        </div>
                        <div class="help-text">Enter grid IDs/ranges separated by commas (1-1000), ranges like 465:467, or * for all available</div>
                    </div>
                    
                    <div class="config-field">
                        <label for="max-workers-input">Max Workers:</label>
                        <input type="number" id="max-workers-input" min="1" max="32" value="4">
                        <div class="help-text">Number of parallel processing workers (1-32)</div>
                    </div>
                </div>
                
                <div>
                    <div class="config-field">
                        <label for="memory-limit-input">Memory Limit (GB):</label>
                        <input type="number" id="memory-limit-input" min="1" max="64" value="4">
                        <div class="help-text">Memory limit for BTC model (1-64 GB)</div>
                    </div>
                    
                    <div class="config-field">
                        <label for="btc-threshold-input">BTC Threshold:</label>
                        <input type="number" id="btc-threshold-input" min="0" max="1" step="0.1" value="0.5">
                        <div class="help-text">Change detection sensitivity (0.0-1.0)</div>
                    </div>
                    
                    <div class="config-field">
                        <label for="cloud-coverage-input">Max Cloud Coverage (%):</label>
                        <input type="number" id="cloud-coverage-input" min="0" max="100" value="0">
                        <div class="help-text">Maximum allowed cloud coverage (0-100%)</div>
                    </div>
                </div>
            </div>
            
            <div class="config-buttons">
                <button class="btn btn-primary" onclick="loadConfiguration()">Load Current Config</button>
                <button class="btn btn-primary" onclick="saveConfiguration()">Apply Configuration</button>
                <button class="btn btn-warning" onclick="resetConfiguration()">Reset to Defaults</button>
            </div>
            
            <div id="config-message" class="config-message"></div>
        </div>
        
        <div class="grid-status-section">
            <h3>Grid Processing Status</h3>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                <div>
                    <span id="grid-summary-text">Loading grid status...</span>
                </div>
                <button class="btn btn-primary" onclick="refreshGridStatus()">Refresh Status</button>
            </div>
            
            <table class="grid-table" id="grid-status-table">
                <thead>
                    <tr>
                        <th>Grid ID</th>
                        <th>Status</th>
                        <th>Years Progress</th>
                        <th>Total Images</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody id="grid-status-tbody">
                    <tr><td colspan="5">Loading...</td></tr>
                </tbody>
            </table>
            
            <div class="add-grid-section">
                <h4>Add New Grid Cells</h4>
                <div class="inline-form">
                    <div class="config-field" style="margin-bottom: 0;">
                        <label for="new-grid-ids">Grid IDs to Add:</label>
                        <input type="text" id="new-grid-ids" placeholder="e.g., 468:470,500:502 or * for all">
                    </div>
                    <button class="btn btn-primary" onclick="addGridCells()">Add Grid Cells</button>
                    <button class="btn btn-warning" onclick="checkGridExists()">Check if Exists in DB</button>
                </div>
                <div class="help-text">Add grid IDs/ranges separated by commas, ranges like 468:470, or * for all available. System will check if data already exists before processing.</div>
            </div>
        </div>
        
        <div class="control-buttons">
            <button class="btn btn-primary" id="start-btn" onclick="controlPipeline('start')">Start Pipeline</button>
            <button class="btn btn-warning" id="pause-btn" onclick="controlPipeline('pause')" disabled>Pause</button>
            <button class="btn btn-primary" id="resume-btn" onclick="controlPipeline('resume')" disabled style="display:none;">Resume</button>
            <button class="btn btn-danger" id="stop-btn" onclick="controlPipeline('stop')" disabled>Stop</button>
            <button class="btn btn-warning" id="retry-btn" onclick="controlPipeline('retry_failed')">Retry Failed</button>
        </div>
        
        <div class="stat-card">
            <h3>Stage Progress</h3>
            <div id="stage-progress"></div>
        </div>
        
        <div class="stat-card">
            <h3>Recent Logs</h3>
            <div class="logs" id="logs"></div>
        </div>
    </div>

    <script>
        let ws = null;
        
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                updateInterface(data);
            };
            
            ws.onclose = function() {
                setTimeout(connectWebSocket, 1000);
            };
        }
        
        function updateInterface(data) {
            // Update overall progress
            document.getElementById('overall-progress').style.width = data.overall_progress + '%';
            document.getElementById('overall-text').textContent = data.overall_progress.toFixed(1) + '%';
            
            // Update status
            const statusEl = document.getElementById('pipeline-status');
            statusEl.textContent = `Status: ${data.status}`;
            statusEl.className = `status ${data.status}`;
            
            // Update button states based on status
            updateButtonStates(data.status);
            
            // Update configuration controls based on status
            updateConfigControls(data.status);
            
            // Update current stage
            document.getElementById('current-stage').textContent = data.current_stage || '-';
            document.getElementById('current-year').textContent = data.current_year ? `Year: ${data.current_year}` : '-';
            
            // Update stats
            document.getElementById('years-total').textContent = data.total_years || data.years?.length || '-';
            document.getElementById('grid-cells').textContent = data.grid_cells || '-';
            document.getElementById('started-at').textContent = data.started_at || '-';
            
            // Update stage progress
            if (data.stage_progress) {
                let html = '';
                for (const [key, stage] of Object.entries(data.stage_progress)) {
                    html += `
                        <div class="stage-progress">
                            <div class="stage-title">${stage.stage} ${stage.year ? '(' + stage.year + ')' : ''}</div>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: ${stage.progress}%"></div>
                            </div>
                            <small>${stage.completed}/${stage.total} completed, ${stage.failed} failed</small>
                        </div>
                    `;
                }
                document.getElementById('stage-progress').innerHTML = html;
            }
        }
        
        function updateButtonStates(status) {
            const startBtn = document.getElementById('start-btn');
            const pauseBtn = document.getElementById('pause-btn');
            const resumeBtn = document.getElementById('resume-btn');
            const stopBtn = document.getElementById('stop-btn');
            const retryBtn = document.getElementById('retry-btn');
            
            // Reset all buttons
            startBtn.disabled = false;
            pauseBtn.disabled = true;
            pauseBtn.style.display = 'inline-block';
            resumeBtn.disabled = true;
            resumeBtn.style.display = 'none';
            stopBtn.disabled = true;
            retryBtn.disabled = false;
            
            switch(status) {
                case 'idle':
                case 'waiting_for_start':
                case 'stopped':
                case 'error':
                case 'completed':
                    startBtn.disabled = false;
                    startBtn.textContent = 'Start Pipeline';
                    pauseBtn.disabled = true;
                    stopBtn.disabled = true;
                    break;
                    
                case 'starting':
                case 'running':
                    startBtn.disabled = true;
                    pauseBtn.disabled = false;
                    stopBtn.disabled = false;
                    resumeBtn.style.display = 'none';
                    break;
                    
                case 'pausing':
                case 'paused':
                    startBtn.disabled = true;
                    pauseBtn.disabled = true;
                    pauseBtn.style.display = 'none';
                    resumeBtn.disabled = false;
                    resumeBtn.style.display = 'inline-block';
                    stopBtn.disabled = false;
                    break;
                    
                case 'stopping':
                    startBtn.disabled = true;
                    pauseBtn.disabled = true;
                    resumeBtn.disabled = true;
                    stopBtn.disabled = true;
                    break;
            }
        }
        
        async function loadConfiguration() {
            try {
                const response = await fetch('/api/config');
                const config = await response.json();
                
                // Populate form fields
                document.getElementById('years-input').value = config.years.join(',');
                document.getElementById('grid-ids-input').value = config.grid_ids.join(',');
                document.getElementById('max-workers-input').value = config.max_workers;
                document.getElementById('memory-limit-input').value = config.memory_limit_gb;
                document.getElementById('btc-threshold-input').value = config.btc_threshold;
                document.getElementById('cloud-coverage-input').value = config.max_cloud_coverage;
                
                showConfigMessage('Configuration loaded successfully', 'success');
                
            } catch (error) {
                console.error('Failed to load configuration:', error);
                showConfigMessage('Failed to load configuration: ' + error.message, 'error');
            }
        }
        
        function parseYearsInput(input) {
            if (!input) return [];
            
            // Handle wildcard for all available years (2016-2030)
            if (input.trim() === '*') {
                const years = [];
                for (let year = 2016; year <= 2030; year++) {
                    years.push(year);
                }
                return years;
            }
            
            const years = new Set(); // Use Set to avoid duplicates
            const parts = input.split(',');
            
            for (const part of parts) {
                const trimmed = part.trim();
                if (!trimmed) continue;
                
                if (trimmed.includes(':')) {
                    // Handle range like "2020:2024"
                    const [start, end] = trimmed.split(':');
                    const startYear = parseInt(start.trim());
                    const endYear = parseInt(end.trim());
                    
                    if (isNaN(startYear) || isNaN(endYear)) {
                        throw new Error(`Invalid year range: ${trimmed}`);
                    }
                    
                    if (startYear > endYear) {
                        throw new Error(`Invalid range: start year ${startYear} is greater than end year ${endYear}`);
                    }
                    
                    for (let year = startYear; year <= endYear; year++) {
                        if (year < 2016 || year > 2030) {
                            throw new Error(`Year ${year} is out of valid range (2016-2030)`);
                        }
                        years.add(year);
                    }
                } else {
                    // Handle single year
                    const year = parseInt(trimmed);
                    if (isNaN(year)) {
                        throw new Error(`Invalid year: ${trimmed}`);
                    }
                    if (year < 2016 || year > 2030) {
                        throw new Error(`Year ${year} is out of valid range (2016-2030)`);
                    }
                    years.add(year);
                }
            }
            
            return Array.from(years).sort((a, b) => a - b);
        }
        
        function parseGridIdsInput(input) {
            if (!input) return [];
            
            // Handle wildcard for all available grid IDs (1-1000)
            if (input.trim() === '*') {
                const gridIds = [];
                for (let id = 1; id <= 1000; id++) {
                    gridIds.push(id);
                }
                return gridIds;
            }
            
            const gridIds = new Set(); // Use Set to avoid duplicates
            const parts = input.split(',');
            
            for (const part of parts) {
                const trimmed = part.trim();
                if (!trimmed) continue;
                
                if (trimmed.includes(':')) {
                    // Handle range like "465:467"
                    const [start, end] = trimmed.split(':');
                    const startId = parseInt(start.trim());
                    const endId = parseInt(end.trim());
                    
                    if (isNaN(startId) || isNaN(endId)) {
                        throw new Error(`Invalid grid ID range: ${trimmed}`);
                    }
                    
                    if (startId > endId) {
                        throw new Error(`Invalid range: start ID ${startId} is greater than end ID ${endId}`);
                    }
                    
                    for (let id = startId; id <= endId; id++) {
                        if (id < 1 || id > 1000) {
                            throw new Error(`Grid ID ${id} is out of valid range (1-1000)`);
                        }
                        gridIds.add(id);
                    }
                } else {
                    // Handle single grid ID
                    const gridId = parseInt(trimmed);
                    if (isNaN(gridId)) {
                        throw new Error(`Invalid grid ID: ${trimmed}`);
                    }
                    if (gridId < 1 || gridId > 1000) {
                        throw new Error(`Grid ID ${gridId} is out of valid range (1-1000)`);
                    }
                    gridIds.add(gridId);
                }
            }
            
            return Array.from(gridIds).sort((a, b) => a - b);
        }
        
        async function saveConfiguration() {
            try {
                // Validate and parse input fields
                const yearsText = document.getElementById('years-input').value.trim();
                const gridIdsText = document.getElementById('grid-ids-input').value.trim();
                
                let years = [];
                let gridIds = [];
                
                // Use new parsing functions that support ranges and wildcards
                try {
                    years = parseYearsInput(yearsText);
                } catch (error) {
                    throw new Error(`Years parsing error: ${error.message}`);
                }
                
                try {
                    gridIds = parseGridIdsInput(gridIdsText);
                } catch (error) {
                    throw new Error(`Grid IDs parsing error: ${error.message}`);
                }
                
                const configData = {
                    years: years,
                    grid_ids: gridIds,
                    max_workers: parseInt(document.getElementById('max-workers-input').value),
                    memory_limit_gb: parseInt(document.getElementById('memory-limit-input').value),
                    btc_threshold: parseFloat(document.getElementById('btc-threshold-input').value),
                    max_cloud_coverage: parseInt(document.getElementById('cloud-coverage-input').value),
                };
                
                const response = await fetch('/api/config', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(configData)
                });
                
                const result = await response.json();
                
                if (result.status === 'success') {
                    showConfigMessage(`Configuration updated: ${result.updated_fields.join(', ')}`, 'success');
                } else {
                    showConfigMessage('Configuration update failed: ' + result.message, 'error');
                }
                
            } catch (error) {
                console.error('Failed to save configuration:', error);
                showConfigMessage('Failed to save configuration: ' + error.message, 'error');
            }
        }
        
        async function resetConfiguration() {
            if (confirm('Reset configuration to defaults?')) {
                try {
                    const defaultConfig = {
                        years: [2020, 2021, 2022, 2023, 2024],
                        grid_ids: [465, 466, 467],
                        max_workers: 4,
                        memory_limit_gb: 4,
                        btc_threshold: 0.5,
                        max_cloud_coverage: 0,
                    };
                    
                    const response = await fetch('/api/config', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(defaultConfig)
                    });
                    
                    const result = await response.json();
                    
                    if (result.status === 'success') {
                        await loadConfiguration(); // Reload to show the defaults
                        showConfigMessage('Configuration reset to defaults', 'success');
                    } else {
                        showConfigMessage('Failed to reset configuration: ' + result.message, 'error');
                    }
                    
                } catch (error) {
                    console.error('Failed to reset configuration:', error);
                    showConfigMessage('Failed to reset configuration: ' + error.message, 'error');
                }
            }
        }
        
        function showConfigMessage(message, type) {
            const messageEl = document.getElementById('config-message');
            messageEl.textContent = message;
            messageEl.className = `config-message ${type}`;
            messageEl.style.display = 'block';
            
            // Hide message after 5 seconds
            setTimeout(() => {
                messageEl.style.display = 'none';
            }, 5000);
        }
        
        function updateConfigControls(status) {
            const configInputs = document.querySelectorAll('.config-field input, .config-field select');
            const configButtons = document.querySelectorAll('.config-buttons .btn');
            
            // Disable configuration controls when pipeline is running
            const isRunning = ['starting', 'running', 'pausing', 'paused', 'stopping'].includes(status);
            
            configInputs.forEach(input => {
                input.disabled = isRunning;
            });
            
            configButtons.forEach(button => {
                button.disabled = isRunning;
            });
            
            // Show warning message if trying to change config while running
            if (isRunning) {
                const messageEl = document.getElementById('config-message');
                if (messageEl.style.display === 'none' || messageEl.textContent === '') {
                    showConfigMessage('Configuration is locked while pipeline is running', 'error');
                }
            }
        }
        
        async function refreshGridStatus() {
            try {
                const response = await fetch('/api/grid-status');
                const data = await response.json();
                
                if (data.grid_status) {
                    updateGridStatusTable(data.grid_status, data.summary);
                } else {
                    showConfigMessage('Failed to load grid status: ' + (data.message || 'Unknown error'), 'error');
                }
                
            } catch (error) {
                console.error('Failed to refresh grid status:', error);
                showConfigMessage('Failed to refresh grid status: ' + error.message, 'error');
            }
        }
        
        function updateGridStatusTable(gridStatus, summary) {
            const tbody = document.getElementById('grid-status-tbody');
            const summaryText = document.getElementById('grid-summary-text');
            
            // Update summary
            summaryText.textContent = `${summary.total_grids} grids total | ${summary.completed} completed | ${summary.in_progress} in progress | ${summary.not_started} not started | ${summary.total_images} total images`;
            
            // Clear table
            tbody.innerHTML = '';
            
            // Populate table
            for (const [gridId, gridInfo] of Object.entries(gridStatus)) {
                const row = document.createElement('tr');
                
                // Grid ID
                const gridIdCell = document.createElement('td');
                gridIdCell.textContent = gridId;
                row.appendChild(gridIdCell);
                
                // Status
                const statusCell = document.createElement('td');
                const statusBadge = document.createElement('span');
                statusBadge.className = `status-badge status-${gridInfo.status}`;
                statusBadge.textContent = gridInfo.status.replace(/_/g, ' ').toUpperCase();
                statusCell.appendChild(statusBadge);
                row.appendChild(statusCell);
                
                // Years Progress
                const yearsCell = document.createElement('td');
                for (const [year, yearInfo] of Object.entries(gridInfo.years)) {
                    const yearDot = document.createElement('span');
                    yearDot.className = 'year-status';
                    yearDot.title = `${year}: ${yearInfo.image_count} images`;
                    
                    if (yearInfo.processed) {
                        yearDot.className += ' year-completed';
                        yearDot.title += ' (processed)';
                    } else if (yearInfo.inserted) {
                        yearDot.className += ' year-partial';
                        yearDot.title += ' (inserted)';
                    } else if (yearInfo.downloaded) {
                        yearDot.className += ' year-partial';
                        yearDot.title += ' (downloaded)';
                    } else {
                        yearDot.className += ' year-missing';
                        yearDot.title += ' (missing)';
                    }
                    
                    yearsCell.appendChild(yearDot);
                }
                row.appendChild(yearsCell);
                
                // Total Images
                const imagesCell = document.createElement('td');
                imagesCell.textContent = gridInfo.total_images;
                row.appendChild(imagesCell);
                
                // Actions
                const actionsCell = document.createElement('td');
                if (gridInfo.status === 'not_started') {
                    const processBtn = document.createElement('button');
                    processBtn.className = 'btn btn-primary btn-sm';
                    processBtn.textContent = 'Process';
                    processBtn.onclick = () => processGrid(gridId);
                    actionsCell.appendChild(processBtn);
                } else if (gridInfo.status === 'partially_downloaded') {
                    const resumeBtn = document.createElement('button');
                    resumeBtn.className = 'btn btn-warning btn-sm';
                    resumeBtn.textContent = 'Resume';
                    resumeBtn.onclick = () => processGrid(gridId);
                    actionsCell.appendChild(resumeBtn);
                }
                row.appendChild(actionsCell);
                
                tbody.appendChild(row);
            }
        }
        
        async function addGridCells() {
            try {
                const newGridsText = document.getElementById('new-grid-ids').value.trim();
                if (!newGridsText) {
                    showConfigMessage('Please enter grid IDs to add', 'error');
                    return;
                }
                
                // Parse new grid IDs using the enhanced parser
                let newGridIds;
                try {
                    newGridIds = parseGridIdsInput(newGridsText);
                } catch (error) {
                    throw new Error(`Grid IDs parsing error: ${error.message}`);
                }
                
                // Get current configuration
                const configResponse = await fetch('/api/config');
                const currentConfig = await configResponse.json();
                
                // Merge with existing grid IDs (avoid duplicates)
                const allGridIds = [...new Set([...currentConfig.grid_ids, ...newGridIds])];
                
                // Update configuration
                const updateResponse = await fetch('/api/config', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        grid_ids: allGridIds
                    })
                });
                
                const result = await updateResponse.json();
                
                if (result.status === 'success') {
                    showConfigMessage(`Added ${newGridIds.length} new grid cells`, 'success');
                    document.getElementById('new-grid-ids').value = '';
                    await loadConfiguration();
                    await refreshGridStatus();
                } else {
                    showConfigMessage('Failed to add grid cells: ' + result.message, 'error');
                }
                
            } catch (error) {
                console.error('Failed to add grid cells:', error);
                showConfigMessage('Failed to add grid cells: ' + error.message, 'error');
            }
        }
        
        async function checkGridExists() {
            try {
                const newGridsText = document.getElementById('new-grid-ids').value.trim();
                if (!newGridsText) {
                    showConfigMessage('Please enter grid IDs to check', 'error');
                    return;
                }
                
                // Parse grid IDs using the enhanced parser
                let gridIds;
                try {
                    gridIds = parseGridIdsInput(newGridsText);
                } catch (error) {
                    throw new Error(`Grid IDs parsing error: ${error.message}`);
                }
                
                // Get current years from config
                const configResponse = await fetch('/api/config');
                const currentConfig = await configResponse.json();
                
                // Check if grids exist
                const checkResponse = await fetch('/api/grid-status/check', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        grid_ids: gridIds,
                        years: currentConfig.years
                    })
                });
                
                const result = await checkResponse.json();
                
                if (result.status === 'success') {
                    let message = 'Grid existence check:\\n';
                    for (const [gridId, gridData] of Object.entries(result.results)) {
                        const existingYears = Object.entries(gridData)
                            .filter(([year, data]) => data.exists)
                            .map(([year, data]) => `${year} (${data.image_count} images)`);
                        
                        if (existingYears.length > 0) {
                            message += `Grid ${gridId}: EXISTS for years ${existingYears.join(', ')}\\n`;
                        } else {
                            message += `Grid ${gridId}: NOT FOUND in database\\n`;
                        }
                    }
                    alert(message);
                } else {
                    showConfigMessage('Failed to check grid existence: ' + result.message, 'error');
                }
                
            } catch (error) {
                console.error('Failed to check grid existence:', error);
                showConfigMessage('Failed to check grid existence: ' + error.message, 'error');
            }
        }
        
        function processGrid(gridId) {
            if (confirm(`Start processing grid ${gridId}?`)) {
                showConfigMessage(`Processing for grid ${gridId} will be included in next pipeline run`, 'success');
            }
        }
        
        async function controlPipeline(action) {
            try {
                // Disable button to prevent double-clicking
                const buttons = document.querySelectorAll('.btn');
                buttons.forEach(btn => btn.disabled = true);
                
                const response = await fetch(`/api/control/${action}`, { method: 'POST' });
                const result = await response.json();
                
                console.log('Control action result:', result);
                
                // Show feedback message
                if (result.message) {
                    const statusEl = document.getElementById('pipeline-status');
                    const originalText = statusEl.textContent;
                    statusEl.textContent = result.message;
                    setTimeout(() => {
                        // Status will be updated by WebSocket, but this provides immediate feedback
                    }, 2000);
                }
                
                // Re-enable buttons after a short delay
                setTimeout(() => {
                    buttons.forEach(btn => btn.disabled = false);
                }, 1000);
                
            } catch (error) {
                console.error('Control action failed:', error);
                
                // Re-enable buttons on error
                const buttons = document.querySelectorAll('.btn');
                buttons.forEach(btn => btn.disabled = false);
                
                alert('Control action failed: ' + error.message);
            }
        }
        
        async function loadLogs() {
            try {
                const response = await fetch('/api/logs/pipeline');
                const logs = await response.text();
                document.getElementById('logs').textContent = logs;
            } catch (error) {
                console.error('Failed to load logs:', error);
            }
        }
        
        // Initialize
        connectWebSocket();
        loadLogs();
        loadConfiguration(); // Load current configuration on page load
        refreshGridStatus(); // Load grid status on page load
        setInterval(loadLogs, 5000); // Refresh logs every 5 seconds
    </script>
</body>
</html>
        """
        return Response(text=html_content, content_type="text/html")

    async def status_handler(self, request: Request) -> Response:
        """Get current pipeline status"""
        return web.json_response(self.pipeline_stats)

    async def progress_handler(self, request: Request) -> Response:
        """Get detailed progress information"""
        progress_data = state_manager.get_all_progress()

        # Calculate overall progress
        total_stages = len(config.years) * 3  # 3 stages per year
        completed_stages = sum(
            1 for p in progress_data.values() if p["status"] == "completed"
        )
        overall_progress = (
            (completed_stages / total_stages) * 100 if total_stages > 0 else 0
        )

        response_data = {
            "overall_progress": overall_progress,
            "stage_progress": progress_data,
            "grid_cells": len(config.grid_ids),
            "total_years": len(config.years),
            "years": config.years,
            **self.pipeline_stats,
        }

        return web.json_response(response_data)

    async def logs_handler(self, request: Request) -> Response:
        """Get logs for a specific component"""
        component = request.match_info["component"]
        log_file = config.get_log_file(component)

        try:
            if log_file.exists():
                async with aiofiles.open(log_file, "r") as f:
                    content = await f.read()
                    # Return last 100 lines
                    lines = content.split("\n")
                    last_lines = lines[-100:] if len(lines) > 100 else lines
                    return Response(text="\n".join(last_lines))
            else:
                return Response(text="Log file not found")
        except Exception as e:
            return Response(text=f"Error reading log file: {e}")

    async def websocket_handler(self, request: Request) -> web.WebSocketResponse:
        """Handle WebSocket connections for real-time updates"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        self.websocket_connections.append(ws)
        self.logger.info(
            f"WebSocket client connected. Total connections: {len(self.websocket_connections)}"
        )

        try:
            # Send initial status
            await self.send_update_to_client(ws)

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"WebSocket error: {ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    break
        finally:
            if ws in self.websocket_connections:
                self.websocket_connections.remove(ws)
            self.logger.info(
                f"WebSocket client disconnected. Total connections: {len(self.websocket_connections)}"
            )

        return ws

    async def control_handler(self, request: Request) -> Response:
        """Handle pipeline control actions"""
        action = request.match_info["action"]

        result = {
            "action": action,
            "status": "acknowledged",
            "timestamp": datetime.now().isoformat(),
        }

        self.logger.info(f"Control action received: {action}")

        try:
            if action == "start":
                self.logger.info("Start pipeline requested via web interface")
                self.start_requested.set()
                self.update_pipeline_status("starting")
                result["message"] = "Pipeline start initiated"

            elif action == "stop":
                self.logger.info("Stop pipeline requested via web interface")
                self.stop_requested.set()
                if self.pipeline_controller:
                    self.pipeline_controller.should_stop = True
                self.update_pipeline_status("stopping")
                result["message"] = "Pipeline stop initiated"

            elif action == "pause":
                self.logger.info("Pause pipeline requested via web interface")
                self.pause_requested.set()
                if self.pipeline_controller:
                    self.pipeline_controller.is_paused = True
                self.update_pipeline_status("pausing")
                result["message"] = "Pipeline pause initiated"

            elif action == "resume":
                self.logger.info("Resume pipeline requested via web interface")
                self.resume_requested.set()
                if self.pipeline_controller:
                    self.pipeline_controller.is_paused = False
                self.update_pipeline_status("running")
                result["message"] = "Pipeline resume initiated"

            elif action == "retry_failed":
                self.logger.info("Retry failed tasks requested via web interface")
                # Reset failed tasks in state manager
                reset_count = state_manager.reset_all_failed_tasks()
                result["message"] = (
                    f"Reset {reset_count} failed tasks. Pipeline can be restarted."
                )

            else:
                result["status"] = "error"
                result["message"] = f"Unknown action: {action}"

        except Exception as e:
            self.logger.error(f"Error handling control action {action}: {e}")
            result["status"] = "error"
            result["message"] = str(e)

        # Broadcast status update to all WebSocket clients
        await self.broadcast_update()

        return web.json_response(result)

    async def config_get_handler(self, request: Request) -> Response:
        """Get current pipeline configuration"""
        config_data = {
            "years": config.years,
            "grid_ids": config.grid_ids,
            "mode": config.mode.value,
            "max_workers": config.max_workers,
            "memory_limit_gb": config.memory_limit_gb,
            "btc_threshold": config.btc_threshold,
            "btc_model_checkpoint": config.btc_model_checkpoint,
            "max_cloud_coverage": config.max_cloud_coverage,
            "start_month": config.start_month,
            "end_month": config.end_month,
        }
        return web.json_response(config_data)

    async def config_set_handler(self, request: Request) -> Response:
        """Update pipeline configuration"""
        try:
            data = await request.json()

            # Validate and update configuration
            result = {
                "status": "success",
                "message": "Configuration updated successfully",
                "updated_fields": [],
                "timestamp": datetime.now().isoformat(),
            }

            # Update years if provided
            if "years" in data:
                years = data["years"]
                if isinstance(years, list) and all(isinstance(y, int) for y in years):
                    if len(years) > 0 and all(2016 <= y <= 2030 for y in years):
                        config.years = sorted(years)
                        result["updated_fields"].append("years")
                        self.logger.info(f"Updated years to: {config.years}")
                    else:
                        result["status"] = "error"
                        result["message"] = "Years must be between 2016 and 2030"
                        return web.json_response(result)
                else:
                    result["status"] = "error"
                    result["message"] = "Years must be a list of integers"
                    return web.json_response(result)

            # Update grid_ids if provided
            if "grid_ids" in data:
                grid_ids = data["grid_ids"]
                if isinstance(grid_ids, list) and all(
                    isinstance(g, int) for g in grid_ids
                ):
                    if len(grid_ids) > 0 and all(1 <= g <= 1000 for g in grid_ids):
                        config.grid_ids = sorted(grid_ids)
                        result["updated_fields"].append("grid_ids")
                        self.logger.info(f"Updated grid_ids to: {config.grid_ids}")
                    else:
                        result["status"] = "error"
                        result["message"] = "Grid IDs must be between 1 and 1000"
                        return web.json_response(result)
                else:
                    result["status"] = "error"
                    result["message"] = "Grid IDs must be a list of integers"
                    return web.json_response(result)

            # Update other simple fields
            simple_fields = {
                "max_workers": (int, 1, 32, "Max workers must be between 1 and 32"),
                "memory_limit_gb": (
                    int,
                    1,
                    64,
                    "Memory limit must be between 1 and 64 GB",
                ),
                "btc_threshold": (
                    float,
                    0.0,
                    1.0,
                    "BTC threshold must be between 0.0 and 1.0",
                ),
                "max_cloud_coverage": (
                    int,
                    0,
                    100,
                    "Cloud coverage must be between 0 and 100",
                ),
                "start_month": (int, 1, 12, "Start month must be between 1 and 12"),
                "end_month": (int, 1, 12, "End month must be between 1 and 12"),
            }

            for field, (
                field_type,
                min_val,
                max_val,
                error_msg,
            ) in simple_fields.items():
                if field in data:
                    try:
                        value = field_type(data[field])
                        if min_val <= value <= max_val:
                            setattr(config, field, value)
                            result["updated_fields"].append(field)
                            self.logger.info(f"Updated {field} to: {value}")
                        else:
                            result["status"] = "error"
                            result["message"] = error_msg
                            return web.json_response(result)
                    except (ValueError, TypeError):
                        result["status"] = "error"
                        result["message"] = f"Invalid value for {field}"
                        return web.json_response(result)

            # Update model checkpoint if provided
            if "btc_model_checkpoint" in data:
                model_checkpoint = data["btc_model_checkpoint"]
                if (
                    isinstance(model_checkpoint, str)
                    and len(model_checkpoint.strip()) > 0
                ):
                    config.btc_model_checkpoint = model_checkpoint.strip()
                    result["updated_fields"].append("btc_model_checkpoint")
                    self.logger.info(
                        f"Updated BTC model checkpoint to: {config.btc_model_checkpoint}"
                    )
                else:
                    result["status"] = "error"
                    result["message"] = "Model checkpoint must be a non-empty string"
                    return web.json_response(result)

            # Update pipeline stats with new totals
            self.pipeline_stats["total_years"] = len(config.years)

            # Broadcast configuration update to all clients
            await self.broadcast_update()

            if not result["updated_fields"]:
                result["message"] = "No valid fields to update"

            return web.json_response(result)

        except Exception as e:
            self.logger.error(f"Error updating configuration: {e}")
            return web.json_response(
                {
                    "status": "error",
                    "message": f"Configuration update failed: {str(e)}",
                    "timestamp": datetime.now().isoformat(),
                }
            )

    async def grid_status_handler(self, request: Request) -> Response:
        """Get status of all configured grid cells"""
        try:
            grid_status = {}

            # Check each configured grid ID
            for grid_id in config.grid_ids:
                grid_info = {
                    "grid_id": grid_id,
                    "years": {},
                    "total_images": 0,
                    "status": "not_started",
                }

                # Check each year
                for year in config.years:
                    year_info = {
                        "year": year,
                        "downloaded": False,
                        "inserted": False,
                        "processed": False,
                        "image_count": 0,
                        "last_updated": None,
                    }

                    # Check if data exists in database (if database mode)
                    if config.mode in ["database_only", "hybrid"]:
                        try:
                            # This would need to be implemented based on your database schema
                            # For now, we'll check the checkpoint files
                            download_checkpoint = state_manager.get_task_status(
                                "download", year, str(grid_id)
                            )
                            insert_checkpoint = state_manager.get_task_status(
                                "insert", year, str(grid_id)
                            )
                            btc_checkpoint = state_manager.get_task_status(
                                "btc_process", year, str(grid_id)
                            )

                            if download_checkpoint:
                                year_info["downloaded"] = (
                                    download_checkpoint.status.value == "completed"
                                )
                                year_info["last_updated"] = (
                                    download_checkpoint.completed_at.isoformat()
                                    if download_checkpoint.completed_at
                                    else None
                                )

                            if insert_checkpoint:
                                year_info["inserted"] = (
                                    insert_checkpoint.status.value == "completed"
                                )
                                if insert_checkpoint.metadata:
                                    year_info["image_count"] = (
                                        insert_checkpoint.metadata.get("image_count", 0)
                                    )

                            if btc_checkpoint:
                                year_info["processed"] = (
                                    btc_checkpoint.status.value == "completed"
                                )

                        except Exception as e:
                            self.logger.warning(
                                f"Error checking grid {grid_id} year {year}: {e}"
                            )

                    # Check local files if local mode
                    elif config.mode == "local_only":
                        year_dir = config.get_year_images_dir(year)
                        grid_files = list(year_dir.glob(f"*grid_{grid_id}_*.tiff"))
                        year_info["image_count"] = len(grid_files)
                        year_info["downloaded"] = len(grid_files) > 0
                        year_info["inserted"] = year_info[
                            "downloaded"
                        ]  # Same as downloaded in local mode

                        # Check for masks
                        mask_dir = config.get_year_masks_dir(year)
                        mask_files = list(mask_dir.glob(f"*grid_{grid_id}_*.tiff"))
                        year_info["processed"] = len(mask_files) > 0

                    grid_info["years"][year] = year_info
                    grid_info["total_images"] += year_info["image_count"]

                # Determine overall status
                all_years_downloaded = all(
                    info["downloaded"] for info in grid_info["years"].values()
                )
                all_years_inserted = all(
                    info["inserted"] for info in grid_info["years"].values()
                )
                all_years_processed = all(
                    info["processed"] for info in grid_info["years"].values()
                )

                if all_years_processed:
                    grid_info["status"] = "completed"
                elif all_years_inserted:
                    grid_info["status"] = "ready_for_processing"
                elif all_years_downloaded:
                    grid_info["status"] = "ready_for_insert"
                elif any(info["downloaded"] for info in grid_info["years"].values()):
                    grid_info["status"] = "partially_downloaded"
                else:
                    grid_info["status"] = "not_started"

                grid_status[grid_id] = grid_info

            return web.json_response(
                {
                    "grid_status": grid_status,
                    "summary": {
                        "total_grids": len(config.grid_ids),
                        "completed": sum(
                            1
                            for g in grid_status.values()
                            if g["status"] == "completed"
                        ),
                        "in_progress": sum(
                            1
                            for g in grid_status.values()
                            if g["status"] not in ["completed", "not_started"]
                        ),
                        "not_started": sum(
                            1
                            for g in grid_status.values()
                            if g["status"] == "not_started"
                        ),
                        "total_images": sum(
                            g["total_images"] for g in grid_status.values()
                        ),
                    },
                }
            )

        except Exception as e:
            self.logger.error(f"Error getting grid status: {e}")
            return web.json_response(
                {"status": "error", "message": f"Failed to get grid status: {str(e)}"}
            )

    async def grid_check_handler(self, request: Request) -> Response:
        """Check if specific grid cells and years exist in database"""
        try:
            data = await request.json()
            grid_ids = data.get("grid_ids", [])
            years = data.get("years", [])

            if not grid_ids or not years:
                return web.json_response(
                    {"status": "error", "message": "grid_ids and years are required"}
                )

            results = {}

            for grid_id in grid_ids:
                results[grid_id] = {}
                for year in years:
                    # Check if data exists using comprehensive checking
                    result = await self._check_grid_year_comprehensive(grid_id, year)
                    results[grid_id][year] = result

            return web.json_response({"status": "success", "results": results})

        except Exception as e:
            self.logger.error(f"Error checking grid data: {e}")
            return web.json_response(
                {"status": "error", "message": f"Failed to check grid data: {str(e)}"}
            )

    async def _check_grid_year_comprehensive(self, grid_id: int, year: int):
        """Check if grid/year combination exists using database and filesystem"""
        try:
            # First check checkpoint status for quick response
            checkpoint_exists = False
            checkpoint_image_count = 0

            try:
                insert_checkpoint = state_manager.get_task_status(
                    "insert", year, str(grid_id)
                )
                if insert_checkpoint and insert_checkpoint.status.value == "completed":
                    checkpoint_exists = True
                    if insert_checkpoint.metadata:
                        checkpoint_image_count = insert_checkpoint.metadata.get(
                            "image_count", 0
                        )
            except:
                pass

            # For local mode, check filesystem
            if config.mode == ProcessingMode.LOCAL_ONLY:
                year_dir = config.get_year_images_dir(year)
                filename = f"sentinel2_grid_{grid_id}_{year}_08.tiff"
                image_path = year_dir / filename

                file_exists = image_path.exists()

                return {
                    "exists": checkpoint_exists or file_exists,
                    "image_count": (
                        checkpoint_image_count
                        if checkpoint_exists
                        else (1 if file_exists else 0)
                    ),
                    "source": (
                        "checkpoint"
                        if checkpoint_exists
                        else ("file" if file_exists else "none")
                    ),
                }

            # For database mode, check actual database
            else:
                db_exists = False
                db_image_count = 0

                try:
                    # Import here to avoid circular imports
                    import psycopg2
                    from ..config.settings import get_database_config

                    db_config = get_database_config()
                    conn = psycopg2.connect(**db_config)

                    try:
                        with conn.cursor() as cur:
                            # Check for records in the specific year
                            cur.execute(
                                """
                                SELECT COUNT(*) 
                                FROM eo 
                                WHERE grid_id = %s 
                                AND EXTRACT(YEAR FROM time) = %s
                            """,
                                (grid_id, year),
                            )

                            db_image_count = cur.fetchone()[0]
                            db_exists = db_image_count > 0

                    finally:
                        conn.close()

                except Exception as db_error:
                    self.logger.warning(
                        f"Database check failed for grid {grid_id}, year {year}: {db_error}"
                    )
                    # Fall back to checkpoint data
                    db_exists = checkpoint_exists
                    db_image_count = checkpoint_image_count

                return {
                    "exists": db_exists,
                    "image_count": db_image_count,
                    "source": "database" if db_exists else "none",
                }

        except Exception as e:
            self.logger.error(f"Failed to check grid {grid_id}, year {year}: {e}")
            return {
                "exists": False,
                "image_count": 0,
                "source": "error",
                "error": str(e),
            }

    async def send_update_to_client(self, ws: web.WebSocketResponse):
        """Send current status to a specific WebSocket client"""
        try:
            progress_data = state_manager.get_all_progress()

            # Calculate overall progress
            total_stages = len(config.years) * 3  # 3 stages per year
            completed_stages = sum(
                1 for p in progress_data.values() if p["status"] == "completed"
            )
            overall_progress = (
                (completed_stages / total_stages) * 100 if total_stages > 0 else 0
            )

            update_data = {
                "overall_progress": overall_progress,
                "stage_progress": progress_data,
                "grid_cells": len(config.grid_ids),
                "total_years": len(config.years),
                "years": config.years,
                **self.pipeline_stats,
            }

            await ws.send_str(json.dumps(update_data))
        except Exception as e:
            self.logger.error(f"Error sending WebSocket update: {e}")

    async def broadcast_update(self):
        """Broadcast update to all connected WebSocket clients"""
        if not self.websocket_connections:
            return

        progress_data = state_manager.get_all_progress()

        # Calculate overall progress
        total_stages = len(config.years) * 3  # 3 stages per year
        completed_stages = sum(
            1 for p in progress_data.values() if p["status"] == "completed"
        )
        overall_progress = (
            (completed_stages / total_stages) * 100 if total_stages > 0 else 0
        )

        update_data = {
            "overall_progress": overall_progress,
            "stage_progress": progress_data,
            "grid_cells": len(config.grid_ids),
            "total_years": len(config.years),
            "years": config.years,
            **self.pipeline_stats,
        }

        message = json.dumps(update_data)

        # Send to all connected clients
        disconnected = []
        for ws in self.websocket_connections:
            try:
                await ws.send_str(message)
            except Exception as e:
                self.logger.warning(f"Failed to send update to WebSocket client: {e}")
                disconnected.append(ws)

        # Remove disconnected clients
        for ws in disconnected:
            if ws in self.websocket_connections:
                self.websocket_connections.remove(ws)

    def update_pipeline_status(self, status: str, stage: str = None, year: int = None):
        """Update pipeline status"""
        self.pipeline_stats.update(
            {
                "status": status,
                "current_stage": stage,
                "current_year": year,
                "last_updated": datetime.now().isoformat(),
            }
        )

        if status == "running" and not self.pipeline_stats["started_at"]:
            self.pipeline_stats["started_at"] = datetime.now().isoformat()

    def register_pipeline_controller(self, controller):
        """Register the pipeline controller for control operations"""
        self.pipeline_controller = controller

    async def wait_for_start_command(self):
        """Wait for the start command from the web interface"""
        self.logger.info("Waiting for start command from web interface...")
        self.update_pipeline_status("waiting_for_start")
        await self.broadcast_update()

        # Wait for the start command
        await self.start_requested.wait()
        self.start_requested.clear()

        self.logger.info("Start command received, beginning pipeline execution")
        return True

    async def check_control_commands(self):
        """Check for control commands during pipeline execution"""
        # Use a very short timeout to make this non-blocking
        try:
            if self.stop_requested.is_set():
                self.stop_requested.clear()
                return "stop"

            if self.pause_requested.is_set():
                self.pause_requested.clear()
                return "pause"

            if self.resume_requested.is_set():
                self.resume_requested.clear()
                return "resume"

            return None
        except Exception as e:
            self.logger.error(f"Error checking control commands: {e}")
            return None

    async def start_server(self):
        """Start the monitoring web server"""
        runner = web.AppRunner(self.app)
        await runner.setup()

        site = web.TCPSite(runner, "0.0.0.0", config.monitoring_port)
        await site.start()

        self.logger.info(f"Monitoring server started on port {config.monitoring_port}")
        self.logger.info(
            f"Access dashboard at: http://localhost:{config.monitoring_port}"
        )

    async def start_background_updates(self):
        """Start background task for periodic updates"""
        while True:
            try:
                await self.broadcast_update()
                await asyncio.sleep(2)  # Update every 2 seconds
            except Exception as e:
                self.logger.error(f"Error in background update task: {e}")
                await asyncio.sleep(5)


# Global monitor instance
monitor = PipelineMonitor()
