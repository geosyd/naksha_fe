#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data processing for CSV parsing and hierarchical data management
"""

import csv
import os
from datetime import datetime

# Simple console functions
def print_error(msg):
    print("ERROR: {}".format(format_message(msg)))

def print_verbose_info(msg, verbose=False):
    if verbose:
        print("VERBOSE: {}".format(format_message(msg)))

def format_message(msg, max_length=50):
    """
    Format long messages by breaking lines at specified length

    Args:
        msg (str): Message to format
        max_length (int): Maximum line length (default 50)

    Returns:
        str: Formatted message with line breaks
    """
    if not msg or len(msg) <= max_length:
        return msg

    words = msg.split(' ')
    lines = []
    current_line = ''

    for word in words:
        if len(current_line) + len(word) + 1 <= max_length:
            if current_line:
                current_line += ' ' + word
            else:
                current_line = word
        else:
            if current_line:
                lines.append(current_line)
                current_line = word
            else:
                # Word is longer than max_length, break it
                lines.append(word[:max_length])
                current_line = word[max_length:]

    if current_line:
        lines.append(current_line)

    return '\n'.join(lines)


class DataProc:
    """Data processing utilities for CSV and hierarchical data"""

    @staticmethod
    def parse_codes_csv(codes_path):
        """Parse the hierarchical codes CSV file with backward compatibility"""
        hierarchical_data = []

        try:
            if not os.path.exists(codes_path):
                print_error("Codes file not found: {}".format(format_message(codes_path)))
                return []

            with open(codes_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)

                for row in reader:
                    if len(row) >= 10:
                        if headers[8].lower() == 'block' and headers[9].lower() == 'block_sryunit':
                            data = {
                                'State': row[0], 'StateCode': row[1], 'District': row[2],
                                'DistrictCode': row[3], 'Ulb': row[4], 'UlbCode': row[5],
                                'Ward': row[6], 'WardCode': row[7], 'SurveyUnit': row[8],
                                'SurveyUnitCode': row[9], 'block': row[8], 'block_sryunit': row[9]
                            }
                        else:
                            data = {
                                'State': row[0], 'StateCode': row[1], 'District': row[2],
                                'DistrictCode': row[3], 'Ulb': row[4], 'UlbCode': row[5],
                                'Ward': row[6], 'WardCode': row[7], 'SurveyUnit': row[8],
                                'SurveyUnitCode': row[9], 'block': row[8], 'block_sryunit': row[9]
                            }
                        hierarchical_data.append(data)

        except Exception as e:
            print_error("Error parsing CSV file: {}".format(e))

        return hierarchical_data

    @staticmethod
    def parse_data_csv(data_path):
        """Parse the main data CSV file with sanitize support"""
        try:
            if not os.path.exists(data_path):
                print_error("Data file not found: {}".format(data_path))
                return {}

            data = {
                'prepare': [], 'validate': [], 'sanitize': [], 'upload': [],
                'prepare_status': [], 'validate_status': [], 'sanitize_status': [], 'upload_status': []
            }

            with open(data_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)

                prepare_idx = validate_idx = sanitize_idx = upload_idx = -1
                prepare_status_idx = validate_status_idx = sanitize_status_idx = upload_status_idx = -1
                for i, header in enumerate(headers):
                    header_lower = header.lower()
                    if header_lower == 'prepare':
                        prepare_idx = i
                    elif header_lower == 'validate':
                        validate_idx = i
                    elif header_lower == 'sanitize':
                        sanitize_idx = i
                    elif header_lower == 'upload':
                        upload_idx = i
                    elif header_lower == 'status' and i > 0:
                        # Determine which status column this is based on previous column
                        if i == prepare_idx + 1:
                            prepare_status_idx = i
                        elif i == validate_idx + 1:
                            validate_status_idx = i
                        elif i == sanitize_idx + 1:
                            sanitize_status_idx = i
                        elif i == upload_idx + 1:
                            upload_status_idx = i

                for row in reader:
                    if len(row) > max(prepare_idx, validate_idx, sanitize_idx, upload_idx,
                                  prepare_status_idx, validate_status_idx, sanitize_status_idx, upload_status_idx):
                        if prepare_idx >= 0 and row[prepare_idx]:
                            data['prepare'].append(row[prepare_idx].strip())
                            if prepare_status_idx >= 0 and len(row) > prepare_status_idx:
                                data['prepare_status'].append(row[prepare_status_idx].strip())
                        if validate_idx >= 0 and row[validate_idx]:
                            data['validate'].append(row[validate_idx].strip())
                            if validate_status_idx >= 0 and len(row) > validate_status_idx:
                                data['validate_status'].append(row[validate_status_idx].strip())
                        if sanitize_idx >= 0 and row[sanitize_idx]:
                            data['sanitize'].append(row[sanitize_idx].strip())
                            if sanitize_status_idx >= 0 and len(row) > sanitize_status_idx:
                                data['sanitize_status'].append(row[sanitize_status_idx].strip())
                        if upload_idx >= 0 and row[upload_idx]:
                            data['upload'].append(row[upload_idx].strip())
                            if upload_status_idx >= 0 and len(row) > upload_status_idx:
                                data['upload_status'].append(row[upload_status_idx].strip())

            return data

        except Exception as e:
            print_error("Error parsing data CSV: {}".format(e))
            return {}

    @staticmethod
    def find_survey_unit_info(hierarchical_data, survey_unit_code):
        """Find hierarchical data for the given survey unit code"""
        for data in hierarchical_data:
            if data['SurveyUnitCode'] == survey_unit_code:
                return data
        return None

    
    @staticmethod
    def validate_survey_unit_codes(hierarchical_data, survey_unit_codes):
        """Validate survey unit codes against hierarchical data"""
        valid_codes = []
        invalid_codes = []

        valid_code_set = set()
        for data in hierarchical_data:
            valid_code_set.add(data.get('SurveyUnitCode', ''))
            valid_code_set.add(data.get('block_sryunit', ''))

        for code in survey_unit_codes:
            if code in valid_code_set:
                valid_codes.append(code)
            else:
                invalid_codes.append(code)

        return {
            'valid': valid_codes,
            'invalid': invalid_codes,
            'total': len(survey_unit_codes),
            'valid_count': len(valid_codes),
            'invalid_count': len(invalid_codes)
        }

    @staticmethod
    def save_codes_to_csv(hierarchical_data, filename):
        """Save hierarchical data to CSV"""
        try:
            directory = os.path.dirname(filename)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)

            with open(filename, 'w', newline='') as f:
                f.write('state,state_code,district,district_code,ulb,ulb_code,ward,ward_code,block,block_sryunit\n')

                for data in hierarchical_data:
                    f.write('{},{},{},{},{},{},{},{},{},{}\n'.format(
                        DataProc.escape_csv_field(data['State']),
                        DataProc.escape_csv_field(data['StateCode']),
                        DataProc.escape_csv_field(data['District']),
                        DataProc.escape_csv_field(data['DistrictCode']),
                        DataProc.escape_csv_field(data['Ulb']),
                        DataProc.escape_csv_field(data['UlbCode']),
                        DataProc.escape_csv_field(data['Ward']),
                        DataProc.escape_csv_field(data['WardCode']),
                        DataProc.escape_csv_field(data['SurveyUnit']),
                        DataProc.escape_csv_field(data['SurveyUnitCode'])
                    ))

            return True

        except Exception as e:
            print_error("Error saving codes CSV: {}".format(e))
            return False

    
    @staticmethod
    def get_gdb_files_from_folder(gdb_folder):
        """Get all GDB files from the specified folder"""
        try:
            import glob
            gdb_files = glob.glob(os.path.join(gdb_folder, '*.gdb'))
            return sorted(gdb_files)
        except Exception as e:
            print_error("Error getting GDB files: {}".format(e))
            return []

    @staticmethod
    def extract_survey_unit_from_gdb_path(gdb_path):
        """Extract survey unit code from GDB file path"""
        try:
            basename = os.path.basename(gdb_path)
            if basename.endswith('.gdb'):
                return basename[:-4]  # Remove .gdb extension
            return basename
        except Exception as e:
            print_error("Error extracting survey unit from path: {}".format(e))
            return None

    @staticmethod
    def get_survey_unit_from_suc_csv(survey_unit_code, data_path='data/data.csv'):
        """Get survey unit from data.csv file - data.csv contains survey_unit_id header and survey unit codes"""
        try:
            if not os.path.exists(data_path):
                return []

            survey_units = []
            with open(data_path, 'r') as f:
                lines = f.readlines()

                for i, line in enumerate(lines):
                    line = line.strip()
                    # Skip header row (first line) and empty lines
                    if i == 0 or not line:
                        continue
                    if line.isdigit():  # Process lines that contain survey unit codes
                        survey_units.append(line)

            return survey_units

        except Exception as e:
            print_error("Error reading survey units from data.csv: {}".format(e))
            return []

    @staticmethod
    def save_status_to_csv(data, output_file):
        """Save status data to CSV file"""
        try:
            directory = os.path.dirname(output_file)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)

            with open(output_file, 'w') as f:
                if data and isinstance(data[0], dict):
                    fieldnames = data[0].keys()
                    import csv
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
                else:
                    writer = csv.writer(f)
                    writer.writerow(['survey_unit_id', 'status', 'message', 'timestamp'])
                    for item in data:
                        writer.writerow([
                            item.get('survey_unit_id', ''),
                            item.get('status', ''),
                            item.get('message', ''),
                            item.get('timestamp', datetime.now().isoformat())
                        ])

            return True

        except Exception as e:
            print("ERROR: Error saving status CSV: {}".format(e))
            return False

    @staticmethod
    def escape_csv_field(field):
        """Escape CSV field if necessary"""
        if field is None or field == '':
            return ""

        if isinstance(field, int) or isinstance(field, float):
            return str(field)

        field_str = str(field)
        if ',' in field_str or '"' in field_str or '\n' in field_str:
            return '"{}"'.format(field_str.replace('"', '""'))

        return field_str

    @staticmethod
    def read_column_from_csv(csv_path, column_name, skip_header=True):
        """Read specific column from CSV file"""
        try:
            if not os.path.exists(csv_path):
                print_error("CSV file not found: {}".format(csv_path))
                return []

            values = []
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)

                if skip_header:
                    headers = next(reader)
                    try:
                        col_index = headers.index(column_name)
                    except ValueError:
                        print_error("Column '{}' not found in CSV file".format(column_name))
                        return []
                else:
                    col_index = 0

                for row in reader:
                    if len(row) > col_index:
                        value = row[col_index].strip()
                        if value:
                            values.append(value)

            return values

        except Exception as e:
            print_error("Error reading CSV column: {}".format(e))
            return []

    @staticmethod
    def filter_data_by_survey_units(data, survey_units):
        """Filter data to include only specified survey units"""
        if not survey_units:
            return data

        survey_unit_set = set(survey_units)
        filtered_data = []

        for item in data:
            survey_unit_id = item.get('survey_unit_id') or item.get('SurveyUnitCode') or item.get('block_sryunit')
            if survey_unit_id in survey_unit_set:
                filtered_data.append(item)

        return filtered_data

    @staticmethod
    def chunk_data(data, chunk_size=500):
        """Split data into chunks for processing"""
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunks.append(data[i:i + chunk_size])
        return chunks

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

    @staticmethod
    def print_codes_summary(hierarchical_data):
        """Print summary of hierarchical data"""
        if not hierarchical_data:
            print("No data fetched.")
            return

        print("\n=== Hierarchical Data Summary ===")
        print("Total hierarchical entries: {}".format(len(hierarchical_data)))

        states = len(set(data['State'] for data in hierarchical_data if data['State']))
        districts = len(set(data['District'] for data in hierarchical_data if data['District']))
        ulbs = len(set(data['Ulb'] for data in hierarchical_data if data['Ulb']))
        wards = len(set(data['Ward'] for data in hierarchical_data if data['Ward']))
        total_survey_units = len([data for data in hierarchical_data if data['SurveyUnit']])
        unique_survey_unit_names = len(set(data['SurveyUnit'] for data in hierarchical_data if data['SurveyUnit']))

        print("States: {}".format(states))
        print("Districts: {}".format(districts))
        print("ULBs: {}".format(ulbs))
        print("Wards: {}".format(wards))
        print("Survey Units: {} ({} unique names)".format(total_survey_units, unique_survey_unit_names))


# Simple survey unit matching functions (from surmatch.py)
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
                    print_verbose_info("Direct match with survey unit code: {}".format(sryunit_code))
                return data

        if verbose:
            print_verbose_info("No direct match found for survey unit code: {}".format(format_message(sryunit_code)))
        return None

    @staticmethod
    def find_best_match(hierarchical_data, search_term, verbose=False):
        """Find best matching survey unit using multiple strategies"""
        search_term = str(search_term).strip()

        match = SurveyMatch.find_by_sryunit_code(hierarchical_data, search_term, verbose)
        if match:
            return match

        # Try exact match (new format: "1", "2", etc.)
        for data in hierarchical_data:
            survey_unit = data.get('SurveyUnit', '') or data.get('Block', '')
            if survey_unit == search_term:
                if verbose:
                    print_verbose_info("Match by block name: {} -> {}".format(search_term, data.get('SurveyUnitCode')))
                return data

        if verbose:
            print_verbose_info("No match found for search term: {}".format(format_message(search_term)))
        return None