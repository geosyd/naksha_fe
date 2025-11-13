#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utility functions for file operations, error handling, and survey unit matching
"""

import os
import shutil
import tempfile
import traceback
import json
from datetime import datetime


class FileOps:
    """File and directory operations"""

    @staticmethod
    def ensure_dir_exists(path):
        """Ensure directory exists, create if necessary"""
        try:
            if not os.path.exists(path):
                os.makedirs(path)
                print_verbose_info("Created directory: {}".format(path))
            return True
        except Exception as e:
            print_error("Error creating directory {}: {}".format(path, e))
            return False

    @staticmethod
    def validate_file_exists(file_path, file_desc="File"):
        """Validate that a file exists"""
        if not os.path.exists(file_path):
            print_error("{} not found: {}".format(file_desc, file_path))
            return False
        return True

    @staticmethod
    def validate_gdb_file(gdb_path):
        """Validate geodatabase file"""
        try:
            if not os.path.exists(gdb_path):
                return False
            if not gdb_path.endswith('.gdb'):
                return False
            if not os.path.isdir(gdb_path):
                return False
            return True
        except Exception:
            return False

    @staticmethod
    def get_output_path(base_path, sryunit_code, ext=""):
        """Generate standardized output path"""
        if ext and not ext.startswith('.'):
            ext = '.' + ext
        return os.path.join(base_path, "{}{}".format(sryunit_code, ext))

    @staticmethod
    def get_timestamped_name(base_name, ext=""):
        """Generate timestamped filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if ext and not ext.startswith('.'):
            ext = '.' + ext
        return "{}_{}{}".format(base_name, timestamp, ext)

    @staticmethod
    def safe_remove_file(file_path):
        """Safely remove file with error handling"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print_verbose_info("Removed file: {}".format(file_path))
            return True
        except Exception as e:
            print_error("File removal error: {}".format(e))
            return False

    @staticmethod
    def safe_remove_dir(dir_path, rm_contents=False):
        """Safely remove directory with error handling"""
        try:
            if not os.path.exists(dir_path):
                return True
            if rm_contents and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
                print_verbose_info("Removed directory: {}".format(dir_path))
            return True
        except Exception as e:
            print_error("Directory removal error: {}".format(e))
            return False

    @staticmethod
    def get_file_size(file_path):
        """Get file size in bytes"""
        try:
            if os.path.exists(file_path):
                return os.path.getsize(file_path)
            return 0
        except Exception:
            return 0

    @staticmethod
    def create_temp_dir(prefix="naksha_"):
        """Create temporary directory"""
        try:
            temp_dir = tempfile.mkdtemp(prefix=prefix)
            return temp_dir
        except Exception as e:
            print_error("Temp directory creation error: {}".format(e))
            return None

    @staticmethod
    def cleanup_temp_dir(temp_dir):
        """Clean up temporary directory"""
        return FileOps.safe_remove_dir(temp_dir, rm_contents=True)

    @staticmethod
    def get_abs_path(path):
        """Get absolute path"""
        return os.path.abspath(path)

    @staticmethod
    def norm_path(path):
        """Normalize path"""
        return os.path.normpath(path)

    @staticmethod
    def join_paths(*paths):
        """Join multiple path components"""
        return os.path.join(*paths)

    @staticmethod
    def get_file_ext(file_path):
        """Get file extension without dot"""
        _, ext = os.path.splitext(file_path)
        return ext[1:] if ext else ""

    @staticmethod
    def get_file_basename(file_path):
        """Get filename without extension"""
        return os.path.splitext(os.path.basename(file_path))[0]

    @staticmethod
    def is_file_readable(file_path):
        """Check if file is readable"""
        try:
            return os.path.exists(file_path) and os.access(file_path, os.R_OK)
        except Exception:
            return False

    @staticmethod
    def is_file_writable(file_path):
        """Check if file is writable"""
        try:
            if os.path.exists(file_path):
                return os.access(file_path, os.W_OK)
            else:
                parent_dir = os.path.dirname(file_path)
                return os.path.exists(parent_dir) and os.access(parent_dir, os.W_OK)
        except Exception:
            return False


class ErrHnd:
    """Error handling and exception management"""

    @staticmethod
    def handle_file_operation(operation, file_path, exception, verbose=False):
        """Handle file operation errors"""
        error_info = {
            'operation': operation,
            'file_path': file_path,
            'error_type': type(exception).__name__,
            'error_message': str(exception),
            'timestamp': datetime.now().isoformat()
        }

        error_msg = "File not found: {}".format(file_path) \
            if isinstance(exception, FileNotFoundError) \
            else "Error {}: {}".format(operation, exception)

        print_error(error_msg)
        return error_info

    @staticmethod
    def handle_api_error(operation, response=None, exception=None, verbose=False):
        """Handle API-related errors"""
        error_info = {
            'operation': operation,
            'timestamp': datetime.now().isoformat()
        }

        if exception:
            error_info.update({
                'error_type': type(exception).__name__,
                'error_message': str(exception)
            })
            error_msg = "API error: {}".format(exception)
        elif response:
            error_info.update({
                'status_code': response.status_code,
                'response_text': response.text
            })
            error_msg = "API request failed: status {}".format(response.status_code)
        else:
            error_msg = "Unknown API error during {}".format(operation)

        print_error(error_msg)
        return error_info

    @staticmethod
    def handle_arcpy_error(operation, exception, verbose=False):
        """Handle ArcPy operation errors"""
        error_info = {
            'operation': operation,
            'error_type': type(exception).__name__,
            'error_message': str(exception),
            'timestamp': datetime.now().isoformat()
        }

        error_msg = "ArcPy error in {}: {}".format(operation, exception)
        print_error(error_msg)
        return error_info

    @staticmethod
    def safe_execute(operation, func, *args, **kwargs):
        """Safely execute a function with error handling"""
        try:
            result = func(*args, **kwargs)
            return True, result, None
        except Exception as e:
            error_info = ErrHnd.handle_generic_error(operation, e)
            return False, None, error_info

    @staticmethod
    def handle_generic_error(operation, exception, verbose=False):
        """Handle generic errors"""
        error_info = {
            'operation': operation,
            'error_type': type(exception).__name__,
            'error_message': str(exception),
            'timestamp': datetime.now().isoformat()
        }

        print_error("Error in {}: {}".format(operation, exception))
        return error_info


class SurveyMatch:
    """Survey unit matching utilities"""

    @staticmethod
    def find_by_sryunit_code(hierarchical_data, sryunit_code, verbose=False):
        """Find hierarchical data by exact survey unit code match"""
        for data in hierarchical_data:
            survey_unit_code = data.get('SurveyUnitCode', '')
            block_sryunit = data.get('block_sryunit', '')

            if survey_unit_code == sryunit_code or block_sryunit == sryunit_code:
                if verbose:
                    print_verbose_info("Direct match: {}".format(sryunit_code))
                return data

        if verbose:
            print_verbose_info("No direct match found: {}".format(sryunit_code))
        return None

    @staticmethod
    def find_best_match(hierarchical_data, search_term, verbose=False):
        """Find best matching survey unit using multiple strategies"""
        search_term = str(search_term).strip()

        # Strategy 1: Exact match by survey unit code
        match = SurveyMatch.find_by_sryunit_code(hierarchical_data, search_term, verbose)
        if match:
            return match

        # Strategy 2: Exact match by block name (new format: "1", "2", etc.)
        for data in hierarchical_data:
            survey_unit = data.get('SurveyUnit', '')
            block = data.get('block', '')

            if survey_unit == search_term or block == search_term:
                if verbose:
                    print_verbose_info("Block name match: {} -> {}".format(search_term, data.get('SurveyUnitCode')))
                return data

        if verbose:
            print_verbose_info("No match found: {}".format(search_term))
        return None

    @staticmethod
    def validate_sryunit_codes(hierarchical_data, sryunit_codes, verbose=False):
        """Validate multiple survey unit codes"""
        valid_codes = []
        invalid_codes = []
        valid_data = []

        for code in sryunit_codes:
            match = SurveyMatch.find_by_sryunit_code(hierarchical_data, code, verbose)
            if match:
                valid_codes.append(code)
                valid_data.append(match)
            else:
                invalid_codes.append(code)

        result = {
            'valid_codes': valid_codes,
            'invalid_codes': invalid_codes,
            'valid_data': valid_data,
            'total': len(sryunit_codes),
            'valid_count': len(valid_codes),
            'invalid_count': len(invalid_codes)
        }

        if verbose:
            print_verbose_info("Validation: {}/{} codes valid".format(
                result['valid_count'], result['total']))

        return result

    @staticmethod
    def get_unique_wards(hierarchical_data):
        """Get list of unique wards from hierarchical data"""
        wards = {}
        for data in hierarchical_data:
            ward_code = data.get('WardCode', '')
            ward_name = data.get('Ward', '')
            state_code = data.get('StateCode', '')
            district_code = data.get('DistrictCode', '')
            ulb_code = data.get('UlbCode', '')

            if ward_code and ward_code not in wards:
                wards[ward_code] = {
                    'WardCode': ward_code, 'Ward': ward_name,
                    'StateCode': state_code, 'DistrictCode': district_code,
                    'UlbCode': ulb_code
                }

        return list(wards.values())


class ConfigLoader:
    """Configuration loading and management from input.json"""

    def __init__(self, config_file='input.json'):
        self.config_file = config_file
        self.config_data = {}
        self._load_config()

    def _load_config(self):
        """Load configuration from input.json file"""
        try:
            # Look for input.json in data/ folder
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', self.config_file)

            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    self.config_data = json.load(f)
                print_verbose_info("Configuration loaded from: {}".format(config_path))
            else:
                print_error("Configuration file not found: {}".format(config_path))
                # Set default values
                self.config_data = {
                    "flown": datetime.now().strftime("%d-%m-%Y"),
                    "wkid": 32644
                }
        except Exception as e:
            print_error("Error loading configuration: {}".format(e))
            # Set default values on error
            self.config_data = {
                "flown": datetime.now().strftime("%d-%m-%Y"),
                "wkid": 32644
            }

    def get_wkid(self):
        """Get WKID from configuration"""
        return self.config_data.get("wkid", 32644)

    def get_flown_date(self):
        """Get drone flown date from configuration"""
        return self.config_data.get("flown", datetime.now().strftime("%d-%m-%Y"))

    def get_config_value(self, key, default=None):
        """Get specific configuration value"""
        return self.config_data.get(key, default)

    def reload_config(self):
        """Reload configuration from file"""
        self._load_config()


# Global configuration instance
_config_loader = None

def get_config():
    """Get global configuration instance"""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
    return _config_loader


# Simple console functions
def print_error(msg):
    print("ERROR: {}".format(msg))

def print_verbose_info(msg, verbose=False):
    if verbose:
        print("INFO: {}".format(msg))