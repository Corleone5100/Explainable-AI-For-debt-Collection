"""
Logging utility for monitoring and debugging the XAI pipeline.
Provides file and console logging with automatic log rotation.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


class LogCapture:
    """Captures terminal output and logs to file for monitoring."""
    
    def __init__(self, log_dir="logs", log_file=None, module_name=None):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        if log_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            module = module_name or "xai"
            log_file = f"{module}_{timestamp}.log"
        
        self.log_path = self.log_dir / log_file
        self.logger = logging.getLogger(f"xai.{module_name or 'main'}")
        self.logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler - captures everything
        file_handler = logging.FileHandler(self.log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler - shows INFO and above
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(levelname)-8s | %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        return self.logger
    
    def info(self, message):
        self.logger.info(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def critical(self, message):
        self.logger.critical(message)
    
    def log_path_str(self):
        return str(self.log_path)


class TerminalMonitor:
    """Monitors script execution and captures all output."""
    
    def __init__(self, script_name, log_dir="logs"):
        self.script_name = script_name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{script_name}_{self.timestamp}.log"
        self.errors = []
        self.warnings = []
        
        # Setup logger
        self.logger = logging.getLogger(f"monitor.{script_name}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()
        
        # File handler
        fh = logging.FileHandler(self.log_file, encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        self.logger.addHandler(fh)
        
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter('%(levelname)-8s | %(message)s'))
        self.logger.addHandler(ch)
    
    def log_start(self):
        self.logger.info("=" * 70)
        self.logger.info(f"SCRIPT STARTED: {self.script_name}")
        self.logger.info(f"Timestamp: {self.timestamp}")
        self.logger.info(f"Log file: {self.log_file}")
        self.logger.info("=" * 70)
    
    def log_end(self, status="SUCCESS"):
        self.logger.info("=" * 70)
        self.logger.info(f"SCRIPT COMPLETED: {self.script_name}")
        self.logger.info(f"Status: {status}")
        self.logger.info(f"Total errors: {len(self.errors)}")
        self.logger.info(f"Total warnings: {len(self.warnings)}")
        self.logger.info(f"Full log: {self.log_file}")
        self.logger.info("=" * 70)
    
    def log_error(self, message, exception=None):
        self.errors.append(message)
        self.logger.error(message)
        if exception:
            self.logger.exception(exception)
    
    def log_warning(self, message):
        self.warnings.append(message)
        self.logger.warning(message)
    
    def log_info(self, message):
        self.logger.info(message)
    
    def get_report(self):
        return {
            'script': self.script_name,
            'timestamp': self.timestamp,
            'log_file': str(self.log_file),
            'status': 'FAILED' if self.errors else 'SUCCESS',
            'errors': self.errors,
            'warnings': self.warnings,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
        }


def create_logger(module_name, log_dir="logs"):
    """Quick helper to create a logger for a module."""
    capture = LogCapture(log_dir=log_dir, module_name=module_name)
    return capture
