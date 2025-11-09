#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Comprehensive logging utility for Naksha FE - selective logging of important messages to data/log.txt
"""

import os
import sys
import inspect
from datetime import datetime


class Colors:
    """ANSI color codes for terminal output"""
    def __init__(self):
        self.supported = self._supports_color()

        if self.supported:
            self.HEADER = '\033[95m'
            self.BLUE = '\033[94m'
            self.CYAN = '\033[96m'
            self.GREEN = '\033[92m'
            self.YELLOW = '\033[93m'
            self.RED = '\033[91m'
            self.BOLD = '\033[1m'
            self.END = '\033[0m'
        else:
            self.HEADER = self.BLUE = self.CYAN = self.GREEN = ''
            self.YELLOW = self.RED = self.BOLD = self.END = ''

    def _supports_color(self):
        """Check if the terminal supports ANSI colors"""
        # Disable color support to avoid showing escape sequences as symbols
        return False


class NakLogger:
    """Comprehensive logging utility with multiple log levels"""

    def __init__(self, log_file='data/log.txt'):
        self.log_file = log_file
        self.original_stdout = sys.stdout
        self.colors = Colors()
        self._ensure_log_directory()
        self.start_time = datetime.now()

    def _ensure_log_directory(self):
        """Ensure log directory exists"""
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

    def _get_timestamp(self):
        """Get current timestamp in standard format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _get_caller_context(self):
        """Get the calling function/module for context"""
        frame = inspect.currentframe()
        try:
            # Go up the call stack to find the actual caller
            caller_frame = frame.f_back.f_back
            if caller_frame:
                module_name = os.path.basename(caller_frame.f_code.co_filename)
                function_name = caller_frame.f_code.co_name
                return "{}:{}".format(module_name.replace('.py', ''), function_name)
        finally:
            del frame
        return "unknown"

    def _should_log(self, level, message):
        """Determine if message should be logged based on importance criteria"""
        # Always log errors, steps, and headers
        if level in ["ERROR", "STEP", "HEADER"]:
            return True

        # Log success messages that contain command completion or key milestones
        if level == "SUCCESS":
            success_keywords = ["completed", "finished", "uploaded", "downloaded", "created", "generated"]
            message_lower = message.lower()
            return any(keyword in message_lower for keyword in success_keywords)

        # Log progress messages for major milestones (5%, 10%, 25%, 50%, 75%, 90%, 100%)
        if level == "PROGRESS" and "%" in message:
            milestone_percentages = ["5%", "10%", "25%", "50%", "75%", "90%", "100%"]
            return any(pct in message for pct in milestone_percentages)

        # Log info messages with important indicators
        if level == "INFO":
            info_keywords = ["command", "starting", "processing", "batch", "api", "authentication"]
            message_lower = message.lower()
            return any(keyword in message_lower for keyword in info_keywords)

        # Default: don't log (display only)
        return False

    def _write_to_logs(self, message, level="INFO", color=None, force_log=False):
        """Write message to log file with context (prepended to top)"""
        # Check if this message should be logged (unless forced)
        if not force_log and not self._should_log(level, message):
            return

        try:
            timestamp = self._get_timestamp()
            context = self._get_caller_context()

            # Format message with level and context
            log_message = "[{}] [{}] [{}] {}\n".format(timestamp, level, context, message)

            if os.path.exists(self.log_file):
                # Read existing content efficiently
                try:
                    with open(self.log_file, 'r') as f:
                        existing_content = f.read()
                except:
                    existing_content = ""

                # Write new content with existing content appended
                with open(self.log_file, 'w') as f:
                    f.write(log_message)
                    if existing_content:
                        f.write(existing_content)
            else:
                # File doesn't exist, create it with just the new message
                with open(self.log_file, 'w') as f:
                    f.write(log_message)

        except Exception as e:
            # If logging fails, don't crash the application
            self.original_stdout.write("Logging error: {}\n".format(e))

    def _write_to_console(self, message, color=None):
        """Write colored message to console"""
        if color and self.colors.supported:
            message = "{}{}{}".format(color, message, self.colors.END)
        self.original_stdout.write(message)

    def info(self, message, force_log=False):
        """Log informational message"""
        self._write_to_console(message, self.colors.CYAN)
        self.original_stdout.write("\n")  # Add newline for proper spacing
        self._write_to_logs(message, "INFO", self.colors.CYAN, force_log)

    def success(self, message, force_log=False):
        """Log success message"""
        self._write_to_console(message, self.colors.GREEN)
        self.original_stdout.write("\n")  # Add newline for proper spacing
        self._write_to_logs(message, "SUCCESS", self.colors.GREEN, force_log)

    def warning(self, message, force_log=False):
        """Log warning message"""
        self._write_to_console(message, self.colors.YELLOW)
        self.original_stdout.write("\n")  # Add newline for proper spacing
        self._write_to_logs(message, "WARNING", self.colors.YELLOW, force_log)

    def error(self, message, exception=None):
        """Log error message with optional exception details"""
        console_msg = message
        log_msg = message

        if exception:
            console_msg += ": {}".format(str(exception))
            log_msg += ": {}".format(str(exception))

        self._write_to_console(console_msg, self.colors.RED)
        self._write_to_logs(log_msg, "ERROR", self.colors.RED, True)  # Always log errors

    def progress(self, current, total, item_name=""):
        """Log progress message"""
        percentage = (current * 100) // total if total > 0 else 0
        message = "Progress: {}/{} ({}%) - {}".format(current, total, percentage, item_name)
        self._write_to_console(message + "\r", self.colors.BLUE)  # Use \r for progress bars
        self._write_to_logs(message, "PROGRESS", self.colors.BLUE)  # Uses selective logic

    def step(self, step_name, force_log=False):
        """Log step indicator"""
        message = "=== {} ===".format(step_name.upper())
        self._write_to_console(message + "\n", self.colors.BOLD)
        self._write_to_logs(message, "STEP", self.colors.BOLD, True or force_log)  # Always log steps

    def header(self, title, force_log=False):
        """Log section header"""
        message = "\n=== {} ===".format(title)
        self._write_to_console(message + "\n", self.colors.HEADER)
        self._write_to_logs(message, "HEADER", self.colors.HEADER, True or force_log)  # Always log headers

    def plain(self, message, force_log=False):
        """Log plain message without colors or level"""
        self._write_to_console(message)
        self._write_to_logs(message, "PLAIN", None, force_log)

    def write(self, message):
        """Override write method to capture all output (backward compatibility)"""
        # Write to original stdout (console)
        self.original_stdout.write(message)

        # Write to log file if message contains content (not just newlines)
        # Use selective logic for PRINT messages - display only by default
        if message.strip():
            self._write_to_logs(message.rstrip('\n'), "PRINT")

    def flush(self):
        """Override flush method"""
        self.original_stdout.flush()

    def __getattr__(self, name):
        """Delegate any other attributes to original stdout"""
        return getattr(self.original_stdout, name)


# Enhanced Logger class for backward compatibility
class Logger(NakLogger):
    """Backward compatible Logger class"""
    pass


# Global logger instance
_logger = None


def setup_logging(log_file='data/log.txt'):
    """Setup comprehensive logging to redirect all print statements and provide centralized logging"""
    global _logger
    if _logger is None:
        _logger = NakLogger(log_file)
        sys.stdout = _logger


def get_logger():
    """Get the current logger instance"""
    return _logger


# Centralized logging utility functions
def log_info(message, context=None, force_log=False):
    """Log an informational message"""
    if _logger:
        _logger.info(message, force_log)


def log_success(message, context=None, force_log=False):
    """Log a success message"""
    if _logger:
        _logger.success(message, force_log)


def log_warning(message, context=None, force_log=False):
    """Log a warning message"""
    if _logger:
        _logger.warning(message, force_log)


def log_error(message, context=None, exception=None, force_log=False):
    """Log an error message with optional exception details"""
    if _logger:
        _logger.error(message, exception)


def log_progress(current, total, item_name="", context=None):
    """Log progress message"""
    if _logger:
        _logger.progress(current, total, item_name)


def log_step(step_name, context=None, force_log=False):
    """Log step indicator"""
    if _logger:
        _logger.step(step_name, force_log)


def log_header(title, context=None, force_log=False):
    """Log section header"""
    if _logger:
        _logger.header(title, force_log)


def log_plain(message, context=None):
    """Log plain message without formatting"""
    if _logger:
        _logger.plain(message)


# Backward compatibility functions
def log_info_old(message):
    """Backward compatibility - log an informational message"""
    if _logger:
        _logger._write_to_logs("INFO: " + message, "INFO")


def log_error_old(message):
    """Backward compatibility - log an error message"""
    if _logger:
        _logger._write_to_logs("ERROR: " + message, "ERROR")


def log_warning_old(message):
    """Backward compatibility - log a warning message"""
    if _logger:
        _logger._write_to_logs("WARNING: " + message, "WARNING")