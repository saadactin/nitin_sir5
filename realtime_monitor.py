"""
Real-time Monitoring Module - Phase 2
WebSocket-based real-time updates for sync operations
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
from flask import Flask
from flask_socketio import SocketIO, emit, join_room, leave_room
import threading
import time

logger = logging.getLogger(__name__)

# Global SocketIO instance (will be initialized in app.py)
socketio = None


def init_socketio(app: Flask):
    """Initialize SocketIO for the Flask app"""
    global socketio
    try:
        socketio = SocketIO(
            app,
            cors_allowed_origins="*",
            async_mode='eventlet',
            logger=False,
            engineio_logger=False
        )
        logger.info("SocketIO initialized for real-time monitoring")
        return socketio
    except Exception as e:
        logger.warning(f"SocketIO initialization failed (non-critical): {e}")
        # Fallback to threading mode
        try:
            socketio = SocketIO(
                app,
                cors_allowed_origins="*",
                async_mode='threading',
                logger=False,
                engineio_logger=False
            )
            logger.info("SocketIO initialized with threading mode")
            return socketio
        except Exception as e2:
            logger.error(f"SocketIO initialization completely failed: {e2}")
            return None


def broadcast_sync_update(source_name: str, table_name: str, status: str, details: Dict):
    """Broadcast sync update to all connected clients"""
    if socketio:
        try:
            socketio.emit('sync_update', {
                'source_name': source_name,
                'table_name': table_name,
                'status': status,
                'details': details,
                'timestamp': datetime.now().isoformat()
            }, namespace='/monitor', broadcast=True)
        except Exception as e:
            logger.warning(f"Error broadcasting sync update: {e}")


def broadcast_validation_result(source_name: str, table_name: str, result: Dict):
    """Broadcast validation result"""
    if socketio:
        try:
            socketio.emit('validation_result', {
                'source_name': source_name,
                'table_name': table_name,
                'result': result,
                'timestamp': datetime.now().isoformat()
            }, namespace='/monitor', broadcast=True)
        except Exception as e:
            logger.warning(f"Error broadcasting validation result: {e}")


def broadcast_performance_metrics(source_name: str, table_name: str, metrics: Dict):
    """Broadcast performance metrics"""
    if socketio:
        try:
            socketio.emit('performance_metrics', {
                'source_name': source_name,
                'table_name': table_name,
                'metrics': metrics,
                'timestamp': datetime.now().isoformat()
            }, namespace='/monitor', broadcast=True)
        except Exception as e:
            logger.warning(f"Error broadcasting performance metrics: {e}")


def broadcast_quality_score(source_name: str, table_name: str, score: Dict):
    """Broadcast data quality score"""
    if socketio:
        try:
            socketio.emit('quality_score', {
                'source_name': source_name,
                'table_name': table_name,
                'score': score,
                'timestamp': datetime.now().isoformat()
            }, namespace='/monitor', broadcast=True)
        except Exception as e:
            logger.warning(f"Error broadcasting quality score: {e}")


def broadcast_alert(alert_type: str, severity: str, message: str, details: Dict = None):
    """Broadcast alert to all connected clients"""
    if socketio:
        try:
            socketio.emit('alert', {
                'type': alert_type,
                'severity': severity,
                'message': message,
                'details': details or {},
                'timestamp': datetime.now().isoformat()
            }, namespace='/monitor', broadcast=True)
        except Exception as e:
            logger.warning(f"Error broadcasting alert: {e}")


def register_socketio_handlers(socketio_instance):
    """Register SocketIO event handlers"""
    if not socketio_instance:
        return
    
    @socketio_instance.on('connect', namespace='/monitor')
    def handle_connect():
        """Handle client connection"""
        logger.info("Client connected to real-time monitor")
        emit('connected', {'status': 'connected', 'timestamp': datetime.now().isoformat()})
    
    @socketio_instance.on('disconnect', namespace='/monitor')
    def handle_disconnect():
        """Handle client disconnection"""
        logger.info("Client disconnected from real-time monitor")
    
    @socketio_instance.on('join_room', namespace='/monitor')
    def handle_join_room(data):
        """Handle client joining a room"""
        room = data.get('room', 'default')
        join_room(room)
        emit('joined_room', {'room': room})
    
    @socketio_instance.on('leave_room', namespace='/monitor')
    def handle_leave_room(data):
        """Handle client leaving a room"""
        room = data.get('room', 'default')
        leave_room(room)
        emit('left_room', {'room': room})
    
    @socketio_instance.on('subscribe', namespace='/monitor')
    def handle_subscribe(data):
        """Handle subscription to specific source/table"""
        source = data.get('source')
        table = data.get('table')
        room = f"{source}_{table}" if source and table else 'all'
        join_room(room)
        emit('subscribed', {'room': room, 'source': source, 'table': table})


class RealtimeMonitor:
    """Manages real-time monitoring and broadcasting"""
    
    def __init__(self):
        self.active_monitors = {}
    
    def start_monitoring(self, source_name: str, table_name: str):
        """Start monitoring a specific source/table"""
        monitor_id = f"{source_name}_{table_name}"
        if monitor_id not in self.active_monitors:
            self.active_monitors[monitor_id] = {
                'source_name': source_name,
                'table_name': table_name,
                'start_time': datetime.now(),
                'status': 'monitoring'
            }
    
    def stop_monitoring(self, source_name: str, table_name: str):
        """Stop monitoring a specific source/table"""
        monitor_id = f"{source_name}_{table_name}"
        if monitor_id in self.active_monitors:
            del self.active_monitors[monitor_id]
    
    def get_active_monitors(self) -> List[Dict]:
        """Get list of active monitors"""
        return list(self.active_monitors.values())


# Global realtime monitor instance
realtime_monitor = RealtimeMonitor()

