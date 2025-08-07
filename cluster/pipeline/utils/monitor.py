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
from typing import Dict, Any, List
from pathlib import Path
import aiohttp
from aiohttp import web
from aiohttp.web import Response, Request
import aiofiles

from ..config.settings import config
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

    def setup_routes(self):
        """Setup web routes for monitoring interface"""
        self.app.router.add_get("/", self.index_handler)
        self.app.router.add_get("/api/status", self.status_handler)
        self.app.router.add_get("/api/progress", self.progress_handler)
        self.app.router.add_get("/api/logs/{component}", self.logs_handler)
        self.app.router.add_get("/ws", self.websocket_handler)
        self.app.router.add_post("/api/control/{action}", self.control_handler)

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
        .btn { padding: 10px 20px; margin: 5px; border: none; border-radius: 4px; cursor: pointer; }
        .btn-primary { background: #3498db; color: white; }
        .btn-danger { background: #e74c3c; color: white; }
        .btn-warning { background: #f39c12; color: white; }
        .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
        .status.idle { background: #95a5a6; color: white; }
        .status.running { background: #27ae60; color: white; }
        .status.error { background: #e74c3c; color: white; }
        .status.completed { background: #2ecc71; color: white; }
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
        
        <div class="control-buttons">
            <button class="btn btn-primary" onclick="controlPipeline('start')">Start Pipeline</button>
            <button class="btn btn-warning" onclick="controlPipeline('pause')">Pause</button>
            <button class="btn btn-danger" onclick="controlPipeline('stop')">Stop</button>
            <button class="btn btn-warning" onclick="controlPipeline('retry_failed')">Retry Failed</button>
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
            
            // Update current stage
            document.getElementById('current-stage').textContent = data.current_stage || '-';
            document.getElementById('current-year').textContent = data.current_year ? `Year: ${data.current_year}` : '-';
            
            // Update stats
            document.getElementById('years-total').textContent = data.total_years;
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
        
        async function controlPipeline(action) {
            try {
                const response = await fetch(`/api/control/${action}`, { method: 'POST' });
                const result = await response.json();
                console.log('Control action result:', result);
            } catch (error) {
                console.error('Control action failed:', error);
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

        # This would interface with the main pipeline controller
        # For now, just return acknowledgment
        result = {
            "action": action,
            "status": "acknowledged",
            "timestamp": datetime.now().isoformat(),
        }

        self.logger.info(f"Control action received: {action}")
        return web.json_response(result)

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
