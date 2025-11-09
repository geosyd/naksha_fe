#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data processing for CSV parsing and hierarchical data management
"""

import csv
import os
import random
import math
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


# New Sanitize Implementation - Simplified based on working reference
class PolygonSanitizer:
    """Simplified polygon sanitization based on working geodatabase_operations.py"""

    def __init__(self):
        """Initialize the polygon sanitizer"""
        pass

    def _is_truly_multipart(self, geom):
        """
        Check if geometry is truly multipart using getPart logic instead of isMultipart flag

        Args:
            geom: ArcPy geometry object

        Returns:
            bool: True if geometry has multiple parts (true multipart), False otherwise
        """
        try:
            if not geom or geom.type != "polygon":
                return False

            # Count actual parts using getPart method
            actual_part_count = 0
            for part_index in range(geom.partCount):
                part = geom.getPart(part_index)
                if part:
                    # Check if this part has actual geometry points
                    has_points = False
                    for point in part:
                        if point:
                            has_points = True
                            break
                    if has_points:
                        actual_part_count += 1

            return actual_part_count > 1

        except Exception as e:
            # If there's an error, fall back to isMultipart as backup
            try:
                return geom.isMultipart if hasattr(geom, 'isMultipart') else False
            except:
                return False

    def sanitize_feature_class(self, input_fc, cluster_tolerance=0.001, verbose=False, buffer_erase_cm=None, do_overlap_fix=None):
        """
        Sanitize a feature class using simplified approach based on working reference

        Args:
            input_fc (str): Path to input feature class
            cluster_tolerance (float): Tolerance for topology operations
            verbose (bool): Enable verbose output

        Returns:
            tuple: (success, message, feature_count)
        """
        try:
            import arcpy

            if not arcpy.Exists(input_fc):
                return False, "Feature class does not exist: {}".format(input_fc), 0

            print_info("=== Polygon Sanitization Based on Working Reference ===")
            print_info("Processing: {}".format(input_fc))

            # Set ArcPy environment - same as reference
            arcpy.env.overwriteOutput = True
            workspace = os.path.dirname(input_fc)
            arcpy.env.workspace = workspace

            # Get initial feature count
            try:
                count_result = arcpy.GetCount_management(input_fc)
                initial_feature_count = int(count_result.getOutput(0))
            except:
                initial_feature_count = 0

            print_info("Initial feature count: {}".format(initial_feature_count))

            # Apply simple 3-step sanitization based on working reference patterns

            # Step 1: Skip duplicate removal to preserve original overlapping geometries
            print_info("Step 1: Skipping duplicate removal to preserve overlaps...")

            # Step 2: Fix overlapping pairs using iterative buffer-erase (before multipart conversion)
            if do_overlap_fix:
                print_info("Step 2: Fixing overlapping pairs...")
                overlap_pairs_resolved = self._fix_overlapping_pairs_iterative(input_fc, verbose, buffer_erase_cm)
                print_info("    Resolved {} overlapping pairs".format(overlap_pairs_resolved))
            else:
                print_info("Step 2: Skipping overlapping pairs fixing (use --do-overlap-fix to enable)...")
                overlap_pairs_resolved = 0

            # Step 3: Convert multipolygons to single polygons (always check for multipart features)
            print_info("Step 3: Converting multipolygons to single polygons...")
            multipart_count = self._convert_multipart_simple(input_fc, verbose)
            print_info("    Converted {} multipart features".format(multipart_count))

            # Step 4: Remove contained features using geometry operations
            print_info("Step 4: Removing contained features...")
            contained_removed = self._remove_contained_simple(input_fc, verbose)
            print_info("    Removed {} contained features".format(contained_removed))

            # Step 5: Remove polygons with holes (first)
            print_info("Step 5: Removing polygons with holes...")
            holes_removed = self._remove_polygons_with_holes(input_fc, verbose)
            print_info("    Removed {} polygons with holes".format(holes_removed))

            # Step 6: Simplify complex geometries
            print_info("Step 6: Simplifying complex geometries...")
            simplified_count = self._simplify_complex_geometries(input_fc, verbose)
            print_info("    Simplified {} complex geometries".format(simplified_count))

            # Step 7: Fix geometries using ArcPy tools
            print_info("Step 7: Fixing geometries...")
            self._fix_geometries_simple(input_fc, verbose)
            print_info("    Applied geometry fixes")

            # Step 8: Recreate soi_uniq_id GlobalID field
            print_info("Step 8: Recreating GlobalID field...")
            globalid_fixed = self._recreate_globalid_field(input_fc, verbose)
            print_info("    GlobalID field recreation: {}".format("Success" if globalid_fixed else "Failed"))

            # Step 9: Renumber plot numbers sequentially
            print_info("Step 9: Renumbering plot numbers...")
            plot_renumbered = self._renumber_plot_numbers(input_fc, verbose)
            print_info("    Plot number renumbering: {}".format("Success" if plot_renumbered else "Failed"))

            # Step 10: Reset OBJECTIDs to start from 1
            print_info("Step 10: Resetting OBJECTIDs to start from 1...")
            objectid_reset = self._reset_objectids(input_fc, verbose)
            print_info("    OBJECTID reset: {}".format("Success" if objectid_reset else "Failed"))

            # Get final feature count
            try:
                count_result = arcpy.GetCount_management(input_fc)
                final_feature_count = int(count_result.getOutput(0))
            except:
                final_feature_count = 0

            net_change = final_feature_count - initial_feature_count
            print_info("Final feature count: {} (net change: {})".format(final_feature_count, net_change))

            return True, "Sanitization completed successfully", final_feature_count

        except Exception as e:
            return False, "Sanitization failed: {}".format(e), 0

    def _remove_duplicates_simple(self, input_fc, verbose=False):
        """Remove duplicates using simple spatial analysis like reference"""
        try:
            import arcpy

            duplicates_removed = 0

            # Create temporary feature class for processing
            temp_fc = "temp_duplicate_removal"
            if arcpy.Exists(temp_fc):
                arcpy.Delete_management(temp_fc)

            # Use spatial analysis to find and remove duplicates
            arcpy.Select_analysis(input_fc, temp_fc)

            if arcpy.Exists(temp_fc):
                # Simple duplicate removal based on geometry extent and centroid
                with arcpy.da.UpdateCursor(temp_fc, ["OID@", "SHAPE@"]) as cursor:
                    processed_geometries = set()
                    duplicate_oids = []

                    for oid, geom in cursor:
                        if geom:
                            # Create simple signature based on centroid and extent
                            try:
                                centroid = geom.centroid
                                extent = geom.extent
                                signature = "{:.3f}_{:.3f}_{:.3f}_{:.3f}".format(
                                    centroid.X, centroid.Y, extent.width, extent.height
                                )

                                if signature in processed_geometries:
                                    duplicate_oids.append(oid)
                                else:
                                    processed_geometries.add(signature)
                            except:
                                continue

                    # Delete duplicates
                    for oid in duplicate_oids:
                        try:
                            cursor.deleteRow()
                            duplicates_removed += 1
                        except:
                            continue

                # Clean up
                arcpy.Delete_management(temp_fc)

            return duplicates_removed

        except Exception as e:
            if verbose:
                print_info("    Duplicate removal failed: {}".format(e))
            return 0

    def _convert_multipart_simple(self, input_fc, verbose=False):
        """Convert multipart to singlepart using manual geometry processing"""
        try:
            import arcpy

            # Check for multipart features
            multipart_count = 0
            multipart_features = []

            with arcpy.da.SearchCursor(input_fc, ["OID@", "SHAPE@"] + [f.name for f in arcpy.ListFields(input_fc) if f.name not in ["OID@", "SHAPE@"]]) as cursor:
                for row in cursor:
                    oid = row[0]
                    geom = row[1]
                    if geom and self._is_truly_multipart(geom):
                        multipart_count += 1
                        multipart_features.append(row)

            if multipart_count == 0:
                return 0

            if verbose:
                print_info("    Found {} multipart features, converting manually...".format(multipart_count))

            # Get all field names for proper copying
            all_fields = [f.name for f in arcpy.ListFields(input_fc)]

            # Collect all singlepart features (including existing singlepart features)
            all_singlepart_features = []

            # First, collect existing singlepart features
            with arcpy.da.SearchCursor(input_fc, all_fields) as cursor:
                for row in cursor:
                    oid_idx = all_fields.index("OID@") if "OID@" in all_fields else 0
                    geom_idx = all_fields.index("SHAPE@") if "SHAPE@" in all_fields else 1

                    oid = row[oid_idx]
                    geom = row[geom_idx]

                    if not geom or not self._is_truly_multipart(geom):
                        # Already singlepart, keep as-is
                        all_singlepart_features.append(row)

            # Then, convert multipart features to singlepart
            converted_count = 0
            for multipart_row in multipart_features:
                oid_idx = all_fields.index("OID@") if "OID@" in all_fields else 0
                geom_idx = all_fields.index("SHAPE@") if "SHAPE@" in all_fields else 1

                oid = multipart_row[oid_idx]
                geom = multipart_row[geom_idx]

                if verbose:
                    print_info("    Converting OBJECTID {} with {} parts...".format(oid, geom.partCount if geom else 0))

                # Convert multipart to singlepart geometries
                if geom and (geom.partCount > 1 or self._is_truly_multipart(geom)):
                    # Handle both true multipart (partCount > 1) and complex single-part geometries (isMultipart=True, partCount=1)
                    if geom.partCount > 1:
                        # True multipart geometry - split into multiple singlepart features
                        for part_index in range(geom.partCount):
                            try:
                                # Extract individual part as singlepart geometry
                                part_geom = geom.getPart(part_index)

                                if part_geom and hasattr(part_geom, 'area'):
                                    # Create new row for this singlepart
                                    new_row = list(multipart_row)
                                    new_row[geom_idx] = part_geom
                                    all_singlepart_features.append(tuple(new_row))
                                    converted_count += 1

                            except Exception as part_error:
                                if verbose:
                                    print_info("    Warning: Could not extract part {} from OBJECTID {}: {}".format(part_index, oid, part_error))
                                continue
                    else:
                        # Complex single-part geometry - recreate as clean singlepart geometry
                        try:
                            # For complex single-part, recreate the geometry to ensure it's properly formed
                            recreated_geom = geom
                            if hasattr(recreated_geom, 'area'):
                                # Create new row with recreated geometry
                                new_row = list(multipart_row)
                                new_row[geom_idx] = recreated_geom
                                all_singlepart_features.append(tuple(new_row))
                                converted_count += 1
                            elif verbose:
                                print_info("    Warning: Could not recreate geometry for OBJECTID {}".format(oid))

                        except Exception as recreate_error:
                            if verbose:
                                print_info("    Warning: Could not recreate OBJECTID {}: {}".format(oid, recreate_error))
                            # Keep original if recreation fails
                            all_singlepart_features.append(multipart_row)

            if verbose:
                print_info("    Converted {} multipart features into {} singlepart features".format(multipart_count, converted_count))

            # Delete all features and reinsert all singlepart features
            arcpy.management.DeleteFeatures(input_fc)

            # Insert all singlepart features
            with arcpy.da.InsertCursor(input_fc, all_fields) as cursor:
                for feature_row in all_singlepart_features:
                    cursor.insertRow(feature_row)

            if verbose:
                print_info("    Successfully inserted {} total singlepart features".format(len(all_singlepart_features)))

            return multipart_count

        except Exception as e:
            if verbose:
                print_info("    Manual multipart conversion failed: {}".format(e))
                import traceback
                traceback.print_exc()
            return 0

    def _remove_contained_simple(self, input_fc, verbose=False):
        """Remove contained features using simple geometry operations"""
        try:
            import arcpy

            contained_removed = 0

            # Create temporary layer for processing
            layer_name = "temp_containment_layer"
            if arcpy.Exists(layer_name):
                arcpy.management.Delete(layer_name)

            arcpy.management.MakeFeatureLayer(input_fc, layer_name)

            # Find and remove contained features
            with arcpy.da.SearchCursor(layer_name, ["OID@", "SHAPE@"]) as cursor:
                features = list(cursor)

                for i, (oid1, geom1) in enumerate(features):
                    if not geom1:
                        continue

                    for j, (oid2, geom2) in enumerate(features):
                        if i >= j:  # Don't compare with self
                            continue

                        if not geom2:
                            continue

                        # Check containment
                        remove_oid = None
                        if geom1.contains(geom2):
                            remove_oid = oid2
                        elif geom2.contains(geom1):
                            remove_oid = oid1

                        if remove_oid:
                            try:
                                arcpy.management.SelectLayerByAttribute(layer_name, "NEW_SELECTION",
                                                                      "OBJECTID = {}".format(remove_oid))
                                selected_count = int(arcpy.GetCount_management(layer_name).getOutput(0))
                                if selected_count > 0:
                                    arcpy.management.DeleteFeatures(layer_name)
                                    contained_removed += 1
                            except:
                                continue

            # Clean up
            if arcpy.Exists(layer_name):
                arcpy.management.Delete(layer_name)

            return contained_removed

        except Exception as e:
            if verbose:
                print_info("    Containment removal failed: {}".format(e))
            return 0

    def _simplify_complex_geometries(self, input_fc, verbose=False):
        """Simplify complex geometries using direct in-place Generalize tool"""
        try:
            import arcpy

            if verbose:
                print_info("    Identifying complex geometries for simplification...")

            # Get all field names for processing
            all_fields = [f.name for f in arcpy.ListFields(input_fc)]

            # Count complex geometries (multipart or high vertex count)
            complex_count = 0
            total_vertices = 0

            complex_features = []
            with arcpy.da.SearchCursor(input_fc, all_fields) as cursor:
                for row in cursor:
                    oid_idx = all_fields.index("OID@")
                    geom_idx = all_fields.index("SHAPE@")

                    oid = row[oid_idx]
                    geom = row[geom_idx]

                    if geom and geom.type == "polygon":
                        # Consider complex if multipart or >100 vertices
                        if self._is_truly_multipart(geom) or geom.pointCount > 100:
                            complex_count += 1
                            total_vertices += geom.pointCount
                            complex_features.append(row)

            if complex_count == 0:
                if verbose:
                    print_info("    No complex geometries found for simplification")
                return 0

            if verbose:
                print_info("    Found {} complex geometries with {} total vertices".format(
                    complex_count, total_vertices))

            # Use ArcPy's Generalize tool directly on the feature class
            # This is safer and preserves all features
            try:
                if verbose:
                    print_info("    Applying Generalize with 0.1 meter tolerance...")

                # Get count before generalization
                original_count = int(arcpy.management.GetCount(input_fc)[0])

                # Apply Generalize to simplify complex geometries
                arcpy.edit.Generalize(input_fc, "0.1 Meters")

                # Get count after generalization
                simplified_count = int(arcpy.management.GetCount(input_fc)[0])

                # Count reduced vertices
                vertices_reduced = 0
                current_vertices = 0
                complex_after = 0

                with arcpy.da.SearchCursor(input_fc, ["SHAPE@"]) as cursor:
                    for geom, in cursor:
                        if geom and geom.type == "polygon":
                            current_vertices += geom.pointCount
                            if self._is_truly_multipart(geom) or geom.pointCount > 100:
                                complex_after += 1

                vertices_reduced = total_vertices - current_vertices

                if verbose:
                    print_info("    Generalization completed:")
                    print_info("      Features preserved: {} → {}".format(original_count, simplified_count))
                    print_info("      Complex geometries: {} → {}".format(complex_count, complex_after))
                    print_info("      Vertex reduction: {} → {} (saved {} vertices)".format(
                        total_vertices, current_vertices, vertices_reduced))

                    if vertices_reduced > 0:
                        print_info("      Average vertex reduction: {:.1f} vertices per feature".format(
                            vertices_reduced / complex_count if complex_count > 0 else 0))

                return complex_count

            except Exception as generalize_error:
                if verbose:
                    print_warning("    Generalization failed: {}".format(generalize_error))
                return 0

        except Exception as e:
            if verbose:
                print_warning("    Error simplifying complex geometries: {}".format(e))
                import traceback
                traceback.print_exc()
            return 0

    def _remove_polygons_with_holes(self, input_fc, verbose=False):
        """Repair polygons with holes by extracting exterior rings (removing interior rings)"""
        try:
            import arcpy

            if verbose:
                print_info("    Repairing polygons with holes...")

            # Repair polygons with holes by extracting exterior rings
            repaired_count = 0
            field_names = [f.name for f in arcpy.ListFields(input_fc) if f.name not in ["OID@", "SHAPE@"]]

            with arcpy.da.UpdateCursor(input_fc, ["OID@", "SHAPE@"] + field_names) as cursor:
                for row in cursor:
                    oid = row[0]
                    geom = row[1]

                    if geom and geom.type == "polygon":
                        # Check for holes using NULL separators
                        has_holes = False

                        for part_index in range(geom.partCount):
                            part = geom.getPart(part_index)
                            if part:
                                null_count = 0
                                for point in part:
                                    if point is None:
                                        null_count += 1

                                # NULL separators indicate interior rings (holes)
                                if null_count > 0:
                                    has_holes = True
                                    break

                        # Alternative check for multipart
                        if not has_holes and self._is_truly_multipart(geom):
                            has_holes = True

                        if has_holes:
                            try:
                                # Extract exterior ring(s) to remove holes
                                repaired_geom = self._extract_exterior_rings(geom, verbose)

                                if repaired_geom and not repaired_geom.isEmpty:
                                    # Update the geometry
                                    row[1] = repaired_geom
                                    cursor.updateRow(row)
                                    repaired_count += 1

                                    if verbose and repaired_count <= 5:
                                        print_info("      Repaired OBJECTID {} (removed interior rings)".format(oid))
                                else:
                                    if verbose and repaired_count <= 5:
                                        print_warning("      Could not repair OBJECTID {} - empty exterior ring".format(oid))

                            except Exception as repair_error:
                                if verbose and repaired_count <= 5:
                                    print_warning("      Could not repair OBJECTID {}: {}".format(oid, repair_error))

            if verbose:
                print_info("    Successfully repaired {} polygons with holes".format(repaired_count))

            return repaired_count

        except Exception as e:
            if verbose:
                print_warning("    Error removing polygons with holes: {}".format(e))
            return 0

    def _fix_geometries_simple(self, input_fc, verbose=False):
        """Fix geometries using ArcPy tools - simplified approach"""
        try:
            import arcpy

            if verbose:
                print_info("    Applying RepairGeometry...")

            # Use ArcPy RepairGeometry like reference uses tools directly
            arcpy.management.RepairGeometry(input_fc, "DELETE_NULL")

            # Remove null geometries
            layer_name = "temp_null_removal"
            if arcpy.Exists(layer_name):
                arcpy.management.Delete(layer_name)

            arcpy.management.MakeFeatureLayer(input_fc, layer_name)
            arcpy.management.SelectLayerByAttribute(layer_name, "NEW_SELECTION", "SHAPE IS NULL")

            null_count = int(arcpy.GetCount_management(layer_name).getOutput(0))
            if null_count > 0:
                arcpy.management.DeleteFeatures(layer_name)
                if verbose:
                    print_info("    Removed {} null geometries".format(null_count))

            # Clean up
            if arcpy.Exists(layer_name):
                arcpy.management.Delete(layer_name)

        except Exception as e:
            if verbose:
                print_info("    Geometry fixing failed: {}".format(e))

    def _fix_overlapping_pairs_iterative(self, input_fc, verbose=False, buffer_erase_cm=None):
        """Fix overlapping pairs using iterative buffer-erase approach"""
        try:
            import arcpy

            print_info("    Detecting overlapping pairs...")

            # Detect overlapping pairs
            overlap_pairs = self._detect_overlapping_pairs(input_fc, verbose)

            if not overlap_pairs:
                print_info("    No overlapping pairs found")
                return 0

            print_info("    Found {} overlapping pairs to resolve".format(len(overlap_pairs)))

            pairs_resolved = 0

            # Process each overlapping pair
            for pair_idx, (oid1, oid2) in enumerate(overlap_pairs, 1):
                print_info("    Processing pair {} of {}: OBJECTID {} & OBJECTID {}".format(
                    pair_idx, len(overlap_pairs), oid1, oid2))

                try:
                    # Apply iterative buffer-erase resolution directly
                    # (no need to re-check overlap since we just detected them)
                    print_info("      Starting iterative buffer-erase resolution...")
                    if self._resolve_pair_iterative_buffer_erase(input_fc, oid1, oid2, verbose, buffer_erase_cm):
                        print_info("      Successfully resolved overlapping pair")
                        pairs_resolved += 1
                    else:
                        print_info("      Failed to resolve overlapping pair")

                except Exception as e:
                    print_info("      Error resolving pair: {}".format(e))

            return pairs_resolved

        except Exception as e:
            print_info("    Error fixing overlapping pairs: {}".format(e))
            return 0

    def _detect_overlapping_pairs(self, input_fc, verbose=False):
        """
        Detect overlapping feature pairs using comprehensive 5-method detection
        Same logic as validate command's _validate_overlapping_polygons
        """
        try:
            import arcpy

            overlap_pairs = []

            # Create feature layer
            layer_name = "temp_overlap_detection"
            if arcpy.Exists(layer_name):
                arcpy.management.Delete(layer_name)
            arcpy.management.MakeFeatureLayer(input_fc, layer_name)

            # Get all features - same format as validate
            geometries = []
            with arcpy.da.SearchCursor(layer_name, ["OID@", "SHAPE@"]) as cursor:
                for oid, geom in cursor:
                    if geom and hasattr(geom, 'area'):
                        geometries.append((oid, geom))

            if len(geometries) < 2:
                if arcpy.Exists(layer_name):
                    arcpy.management.Delete(layer_name)
                return overlap_pairs

            print_info("    Performing comprehensive overlap analysis using 5 detection methods...")

            # Phase 1: ArcPy SelectLayerByLocation method (most comprehensive) - same as validate
            print_info("    Using ArcPy SelectLayerByLocation for comprehensive intersection detection...")
            arcpy_intersect_pairs = []

            try:
                # Create a temporary feature layer for spatial queries - same as validate
                arcpy.MakeFeatureLayer_management(input_fc, "temp_overlap_layer")

                # For each feature, find all intersecting features using SelectLayerByLocation - same as validate
                for oid, geom in geometries:
                    # Create a layer with just this feature
                    arcpy.SelectLayerByAttribute_management("temp_overlap_layer", "NEW_SELECTION", "OBJECTID = {}".format(oid))

                    # Find all features that intersect this feature (excluding itself) - same as validate
                    arcpy.SelectLayerByLocation_management("temp_overlap_layer", "INTERSECT", "temp_overlap_layer", "", "NEW_SELECTION")

                    # Get the intersecting OBJECTIDs
                    with arcpy.da.SearchCursor("temp_overlap_layer", ["OBJECTID"]) as cursor:
                        intersecting_oids = [row[0] for row in cursor if row[0] != oid]

                        for intersect_oid in intersecting_oids:
                            # Ensure consistent ordering and avoid duplicates
                            pair = tuple(sorted([oid, intersect_oid]))
                            if pair not in arcpy_intersect_pairs:
                                arcpy_intersect_pairs.append(pair)

                # Clean up the temporary layer
                arcpy.Delete_management("temp_overlap_layer")

                print_info("    ArcPy method found {} intersecting pairs".format(len(arcpy_intersect_pairs)))

            except Exception as arcpy_error:
                if verbose:
                    print_info("    ArcPy SelectLayerByLocation method failed: {}".format(str(arcpy_error)))
                arcpy_intersect_pairs = []

            # Phase 2: Enhanced pairwise validation using multiple geometry methods - same as validate
            print_info("    Performing detailed geometry analysis for all {} pairs...".format(len(geometries) * (len(geometries) - 1) // 2))

            processed_pairs = set()

            for i in range(len(geometries)):
                for j in range(i + 1, len(geometries)):
                    # Extract OBJECTID and geometry - same as validate
                    oid1 = geometries[i][0]
                    oid2 = geometries[j][0]
                    geom1 = geometries[i][1]
                    geom2 = geometries[j][1]

                    # Skip invalid geometries
                    if not geom1 or not geom2:
                        continue

                    # Create ordered pair to avoid duplicates
                    pair = tuple(sorted([oid1, oid2]))
                    if pair in processed_pairs:
                        continue
                    processed_pairs.add(pair)

                    try:
                        # Check if this pair was detected by ArcPy method
                        arcpy_detected = pair in arcpy_intersect_pairs

                        # Perform comprehensive overlap detection using 5 methods - same as validate
                        overlap_detected = False
                        overlap_area = 0.0
                        overlap_type = ""

                        # Method 1: ArcPy detection using [intersect analysis] (most reliable)
                        if arcpy_detected:
                            overlap_detected = True
                            overlap_type = "arcpy_intersect"
                            # Calculate actual intersection area using bracket analysis
                            intersect_geom = geom1.intersect(geom2, 4)
                            if intersect_geom:
                                overlap_area = intersect_geom.area

                        # Method 2: geom_overlaps functionality using direct overlap detection
                        elif geom1.overlaps(geom2):
                            overlap_detected = True
                            overlap_geom = geom1.intersect(geom2, 4)
                            if overlap_geom:
                                overlap_area = overlap_geom.area
                            overlap_type = "geom_overlaps"

                        # Method 3: Intersection analysis using [intersect geometry] detection
                        elif not overlap_detected:
                            intersect_geom = geom1.intersect(geom2, 4)
                            if intersect_geom and intersect_geom.area > 0.0001:
                                overlap_detected = True
                                overlap_area = intersect_geom.area
                                overlap_type = "intersect_analysis"

                        # Method 4: Containment detection
                        elif geom1.contains(geom2) or geom2.contains(geom1):
                            overlap_detected = True
                            overlap_area = min(geom1.area, geom2.area)
                            overlap_type = "containment"

                        # Method 5: Boundary touching detection
                        if geom1.touches(geom2):
                            overlap_detected = True
                            overlap_area = 0.0
                            if not overlap_type:
                                overlap_type = "boundary_touch"

                        # If overlap detected, record it - same logic as validate
                        if overlap_detected:
                            # Only add as overlap pair if it's not just boundary touching OR has meaningful overlap
                            if not (overlap_type == "boundary_touch" and overlap_area <= 0.0001):
                                overlap_pairs.append(pair)

                                if verbose:
                                    # Calculate overlap statistics
                                    area1 = geom1.area
                                    area2 = geom2.area
                                    min_area = min(area1, area2)
                                    overlap_percent = (overlap_area / min_area * 100) if min_area > 0 else 0

                                    # Enhanced messaging with bracket analysis and geom_overlaps terminology
                                    if overlap_type == "arcpy_intersect":
                                        method_desc = "overlapping polygons using [intersect analysis] (ArcPy)"
                                    elif overlap_type == "geom_overlaps":
                                        method_desc = "overlapping polygons using geom_overlaps functionality"
                                    elif overlap_type == "intersect_analysis":
                                        method_desc = "overlapping polygons using [intersect geometry] analysis"
                                    else:
                                        method_desc = "overlapping polygons - type: {}".format(overlap_type)

                                    print_info("      Found {}: OBJECTID {} & OBJECTID {} - area: {:.2f} ({:.1f}%)".format(
                                        method_desc, oid1, oid2, overlap_area, overlap_percent))

                    except Exception as geom_error:
                        if verbose:
                            print_info("      Geometry comparison error between OBJECTID {} and {}: {}".format(oid1, oid2, str(geom_error)))
                        continue

            # Clean up
            if arcpy.Exists(layer_name):
                arcpy.management.Delete(layer_name)

            print_info("    Comprehensive detection found {} overlapping pairs".format(len(overlap_pairs)))
            return overlap_pairs

        except Exception as e:
            print_info("    Error detecting overlapping pairs: {}".format(e))
            return []

    def _features_overlap(self, input_fc, oid1, oid2):
        """Check if two features overlap in the feature class using comprehensive 5-method detection"""
        try:
            import arcpy

            # Get geometries
            geom1 = None
            geom2 = None
            with arcpy.da.SearchCursor(input_fc, ["OID@", "SHAPE@"], "OBJECTID IN ({}, {})".format(oid1, oid2)) as cursor:
                for oid, geom in cursor:
                    if oid == oid1:
                        geom1 = geom
                    elif oid == oid2:
                        geom2 = geom

            if not geom1 or not geom2:
                return False

            # Use comprehensive 5-method overlap detection - same as validate
            return self._comprehensive_overlap_check(geom1, geom2)

        except:
            return False

    def _comprehensive_overlap_check(self, geom1, geom2):
        """
        Comprehensive overlap check using 5 methods - same logic as validate
        """
        try:
            overlap_detected = False
            overlap_area = 0.0
            overlap_type = ""

            # Method 1: ArcPy SelectLayerByLocation simulation - use direct geometry check
            # (We can't use SelectLayerByLocation here for individual geometries, so we use direct methods)

            # Method 2: Direct overlap detection
            if geom1.overlaps(geom2):
                overlap_detected = True
                overlap_geom = geom1.intersect(geom2, 4)
                if overlap_geom:
                    overlap_area = overlap_geom.area
                overlap_type = "overlap"

            # Method 3: Intersection area detection
            if not overlap_detected:
                intersect_geom = geom1.intersect(geom2, 4)
                if intersect_geom and intersect_geom.area > 0.0001:
                    overlap_detected = True
                    overlap_area = intersect_geom.area
                    overlap_type = "intersection"

            # Method 4: Containment detection
            if not overlap_detected and (geom1.contains(geom2) or geom2.contains(geom1)):
                overlap_detected = True
                overlap_area = min(geom1.area, geom2.area)
                overlap_type = "containment"

            # Method 5: Boundary touching detection
            boundary_touches = geom1.touches(geom2)
            if boundary_touches:
                overlap_detected = True
                overlap_area = 0.0
                if not overlap_type:
                    overlap_type = "boundary_touch"

            # Only consider it an actual overlap if it's not just boundary touching OR has meaningful overlap
            return overlap_detected and not (overlap_type == "boundary_touch" and overlap_area <= 0.0001)

        except:
            return False

    def _resolve_pair_iterative_buffer_erase(self, input_fc, oid1, oid2, verbose=False, buffer_erase_cm=None):
        """Resolve overlapping pair using iterative buffer-erase with progressive tolerances"""
        try:
            import arcpy

            # Check if specific buffer distance is provided
            if buffer_erase_cm is not None:
                print_info("      Using specific buffer distance: {}cm".format(buffer_erase_cm))
                buffer_distance = "{} Meters".format(buffer_erase_cm / 100.0)

                # Warning for very large buffer distances that may erase features
                if buffer_erase_cm > 50:  # Warning for buffers > 50cm
                    print_info("      WARNING: Large buffer distance ({}cm) may erase entire features".format(buffer_erase_cm))
                    print_info("      Consider using smaller values (5-30cm) for typical parcel data")

                print_info("        Applying {} buffer distance...".format(buffer_distance))

                # Apply buffer-erase operation once with specific distance
                if self._apply_buffer_erase_operation(input_fc, oid1, oid2, buffer_distance, verbose):
                    print_info("        Buffer-erase operation completed with {}".format(buffer_distance))

                    # Continue processing even if overlap may remain (as requested)
                    print_info("        Buffer-erase applied with {} (continuing process)".format(buffer_distance))
                    return True
                else:
                    print_info("        Buffer-erase operation failed with {}".format(buffer_distance))
                    return False
            else:
                print_info("      Starting iterative buffer-erase resolution...")

                # Buffer distances: 1cm, 2cm, 3cm, ..., 80cm (1cm increments)
                buffer_distances = []
                for cm in range(1, 81):  # 1cm to 80cm in steps of 1cm
                    meters = cm / 100.0  # Convert cm to meters
                    buffer_distances.append("{} Meters".format(meters))

                # Skip overlap check since we already detected them with comprehensive method
                if verbose:
                    print_info("      Proceeding with buffer-erase resolution (overlap pre-detected)...")

                for buffer_idx, buffer_distance in enumerate(buffer_distances, 1):
                    print_info("        Iteration {}: Trying {} buffer distance...".format(buffer_idx, buffer_distance))

                    # Apply buffer-erase operation following the reference pattern
                    if self._apply_buffer_erase_operation(input_fc, oid1, oid2, buffer_distance, verbose):
                        print_info("        Buffer-erase operation completed with {}".format(buffer_distance))

                        # Verify if overlap is actually resolved after this buffer-erase
                        if self._verify_overlap_resolved(input_fc, oid1, oid2, verbose):
                            print_info("        Overlap resolved with {} buffer distance".format(buffer_distance))
                            return True
                        else:
                            print_info("        Overlap still exists, trying larger buffer...")
                    else:
                        print_info("        Buffer-erase operation failed with {}".format(buffer_distance))

                print_info("        Failed to resolve overlap after all buffer distances")
                return False

        except Exception as e:
            print_info("        Error in iterative buffer-erase resolution: {}".format(e))
            return False

    def _apply_buffer_erase_operation(self, input_fc, oid1, oid2, buffer_distance, verbose=False):
        """
        Apply buffer-erase operation following the working reference pattern:
        1. Buffer the first feature
        2. Erase the second feature using the buffered feature
        3. Retain the unbuffered first feature and erased second feature
        """
        try:
            import arcpy

            # Set environment
            arcpy.env.overwriteOutput = True
            workspace = os.path.dirname(input_fc)

            # Create temporary feature classes - following reference naming pattern
            timestamp = str(int(__import__('time').time() * 1000))

            # Temporary paths in in_memory workspace (FIXED - this is the correct format)
            temp_feature1 = "in_memory\\temp_feature1_{}".format(oid1)
            temp_feature2 = "in_memory\\temp_feature2_{}".format(oid2)
            temp_buffered = "in_memory\\temp_buffered_{}".format(oid1)
            temp_erased = "in_memory\\temp_erased_{}".format(oid2)

            # Clean up any existing temp files
            for temp_path in [temp_feature1, temp_feature2, temp_buffered, temp_erased]:
                if arcpy.Exists(temp_path):
                    arcpy.management.Delete(temp_path)

            try:
                # Step 1: Extract the two overlapping features - use direct ArcPy methods like reference
                arcpy.Select_analysis(input_fc, temp_feature1, "OBJECTID = {}".format(oid1))
                arcpy.Select_analysis(input_fc, temp_feature2, "OBJECTID = {}".format(oid2))

                if not arcpy.Exists(temp_feature1) or not arcpy.Exists(temp_feature2):
                    if verbose:
                        print_info("          Failed to extract features OBJECTID {} and {}".format(oid1, oid2))
                    return False

                # Step 2: Buffer the first feature (this is the erase boundary) - use direct ArcPy method
                arcpy.Buffer_analysis(temp_feature1, temp_buffered, buffer_distance)

                if not arcpy.Exists(temp_buffered):
                    if verbose:
                        print_info("          Failed to create buffer for OBJECTID {}".format(oid1))
                    return False

                # Step 3: Erase the second feature using the buffered feature - use direct ArcPy method
                arcpy.Erase_analysis(temp_feature2, temp_buffered, temp_erased)

                if not arcpy.Exists(temp_erased):
                    if verbose:
                        print_info("          Failed to erase OBJECTID {} using buffered OBJECTID {}".format(oid2, oid1))
                    return False

                # Step 4: Update the original feature class
                # Keep the first feature unbuffered (as it was originally)
                # Update the second feature with the erased geometry

                # Check if erase result has features
                erase_count = int(arcpy.management.GetCount(temp_erased)[0])

                if erase_count > 0:
                    # Get the erased geometry
                    erased_geometry = None
                    with arcpy.da.SearchCursor(temp_erased, ["SHAPE@"]) as cursor:
                        for geom, in cursor:
                            erased_geometry = geom
                            break

                    if erased_geometry:
                        # Update the second feature in the original feature class
                        updated = False
                        with arcpy.da.UpdateCursor(input_fc, ["SHAPE@"], "OBJECTID = {}".format(oid2)) as cursor:
                            for row in cursor:
                                try:
                                    cursor.updateRow([erased_geometry])
                                    updated = True
                                    break
                                except Exception as e:
                                    if verbose:
                                        print_info("          Failed to update OBJECTID {}: {}".format(oid2, e))
                                    break

                        if updated:
                            if verbose:
                                print_info("          Successfully updated OBJECTID {} with erased geometry".format(oid2))
                            return True
                        else:
                            if verbose:
                                print_info("          Failed to update OBJECTID {} with erased geometry".format(oid2))
                            return False
                    else:
                        if verbose:
                            print_info("          No erased geometry found in result")
                        return False
                else:
                    # No geometry left after erase - remove the feature entirely
                    try:
                        arcpy.MakeFeatureLayer_management(input_fc, "temp_erase_layer", "OBJECTID = {}".format(oid2))
                        arcpy.DeleteFeatures_management("temp_erase_layer")
                        if arcpy.Exists("temp_erase_layer"):
                            arcpy.Delete_management("temp_erase_layer")

                        if verbose:
                            print_info("          Removed OBJECTID {} - no geometry left after erase".format(oid2))
                        return True
                    except Exception as e:
                        if verbose:
                            print_info("          Failed to remove OBJECTID {}: {}".format(oid2, e))
                        return False

            finally:
                # Clean up temporary files
                for temp_path in [temp_feature1, temp_feature2, temp_buffered, temp_erased, "temp_erase_layer"]:
                    if arcpy.Exists(temp_path):
                        try:
                            arcpy.Delete_management(temp_path)
                        except:
                            pass

            return False

        except Exception as e:
            if verbose:
                print_info("          Error in buffer-erase operation: {}".format(e))
            return False

    def _verify_overlap_resolved(self, input_fc, oid1, oid2, verbose=False):
        """Verify if the overlap between two features has been resolved"""
        try:
            import arcpy

            # Check if both features still exist
            layer1 = "temp_check_layer1_{}".format(oid1)
            layer2 = "temp_check_layer2_{}".format(oid2)

            try:
                arcpy.management.MakeFeatureLayer(input_fc, layer1, "OBJECTID = {}".format(oid1))
                arcpy.management.MakeFeatureLayer(input_fc, layer2, "OBJECTID = {}".format(oid2))

                count1 = int(arcpy.management.GetCount(layer1)[0])
                count2 = int(arcpy.management.GetCount(layer2)[0])

                # If either feature was completely removed during erase operation, overlap is resolved
                if count1 == 0 or count2 == 0:
                    if verbose:
                        print_info("          Feature removed during erase - overlap resolved")
                    return True

                # Check for intersection between remaining features
                arcpy.management.SelectLayerByLocation(layer2, "INTERSECT", layer1, "", "NEW_SELECTION")
                intersect_count = int(arcpy.management.GetCount(layer2)[0])

                if verbose:
                    print_info("          Intersection check: {} overlapping features".format(intersect_count))

                return intersect_count == 0

            finally:
                # Clean up temporary layers
                for layer in [layer1, layer2]:
                    try:
                        arcpy.management.Delete(layer)
                    except:
                        pass

        except Exception as e:
            if verbose:
                print_info("          Error verifying overlap resolution: {}".format(e))
            return False

    def _recreate_globalid_field(self, input_fc, verbose=False):
        """Recreate soi_uniq_id GlobalID field after sanitization operations"""
        try:
            import arcpy
            import uuid

            print_info("    Checking soi_uniq_id field...")

            # Check if soi_uniq_id field exists
            fields = [f.name for f in arcpy.ListFields(input_fc)]

            if "soi_uniq_id" in fields:
                # Check if it's a GlobalID field
                field_type = None
                for field in arcpy.ListFields(input_fc, "soi_uniq_id"):
                    field_type = field.type
                    break

                if field_type == "GlobalID":
                    print_info("    soi_uniq_id field already exists as GlobalID - no changes needed")
                    return True
                else:
                    # Delete existing non-GlobalID soi_uniq_id field
                    print_info("    Removing existing soi_uniq_id field to recreate as GlobalID")
                    arcpy.management.DeleteField(input_fc, "soi_uniq_id")
                    if verbose:
                        print_info("      Deleted existing soi_uniq_id field")

            # Check if soi_uniq_id field already exists and has GlobalID type
            soi_uniq_id_exists = False
            soi_uniq_id_type = None

            for field in arcpy.ListFields(input_fc, "soi_uniq_id"):
                soi_uniq_id_exists = True
                soi_uniq_id_type = field.type
                if verbose:
                    print_info("      Found existing soi_uniq_id field: {} ({})".format(field.name, field.type))
                break

            if soi_uniq_id_exists:
                if soi_uniq_id_type == "GlobalID":
                    print_info("    soi_uniq_id field already exists as GlobalID type - no changes needed")
                    return True
                else:
                    # Try to delete existing soi_uniq_id field if it's not a GlobalID type
                    print_info("    soi_uniq_id field exists but type is {} - attempting to recreate".format(soi_uniq_id_type))
                    try:
                        arcpy.management.DeleteField(input_fc, "soi_uniq_id")
                        if verbose:
                            print_info("      Deleted existing soi_uniq_id field")
                    except Exception as delete_error:
                        print_info("    WARNING: Cannot delete soi_uniq_id field: {}".format(delete_error))
                        print_info("    Continuing with existing field (may not be GlobalID type)")
                        # Consider this acceptable - the field exists even if not perfect type
                        return True

            # Create soi_uniq_id field with GlobalID data type manually
            print_info("    Creating soi_uniq_id field with GlobalID data type...")

            # Try to add field with GlobalID type
            try:
                arcpy.management.AddField(input_fc, "soi_uniq_id", "GUID")

                # Verify the field was created
                for field in arcpy.ListFields(input_fc, "soi_uniq_id"):
                    if field.name == "soi_uniq_id":
                        if field.type == "GUID":
                            print_info("    Successfully created soi_uniq_id field with GUID data type")
                            if verbose:
                                print_info("      Field name: {}, Type: {}, Length: {}".format(field.name, field.type, field.length))
                            return True
                        else:
                            print_info("    ERROR: soi_uniq_id field created but not as GUID type: {}".format(field.type))
                            return False

            except Exception as field_error:
                print_info("    ERROR creating soi_uniq_id field: {}".format(field_error))
                return False

            print_info("    ERROR: Failed to create soi_uniq_id field")
            return False

        except Exception as e:
            print_info("    ERROR recreating GlobalID field: {}".format(e))
            return False

    def _renumber_plot_numbers(self, input_fc, verbose=False):
        """Renumber soi_plot_no and clr_plot_no fields sequentially after sanitization"""
        try:
            import arcpy

            print_info("    Starting plot number renumbering...")

            # Check if required fields exist
            fields = [f.name for f in arcpy.ListFields(input_fc)]

            soi_plot_field = None
            clr_plot_field = None

            # Find the correct field names (check for variations)
            for field in fields:
                field_lower = field.lower()
                if "soi_plot" in field_lower and "no" in field_lower:
                    soi_plot_field = field
                elif "clr_plot" in field_lower and "no" in field_lower:
                    clr_plot_field = field

            if verbose:
                print_info("      Found soi_plot field: {}".format(soi_plot_field))
                print_info("      Found clr_plot field: {}".format(clr_plot_field))

            # Count total features
            total_features = int(arcpy.management.GetCount(input_fc)[0])
            print_info("      Total features to renumber: {}".format(total_features))

            if total_features == 0:
                print_info("      No features to renumber")
                return True

            # Get the sort order (prefer soi_drone_survey_date, then OBJECTID)
            sort_field = "OBJECTID"  # Default fallback
            available_fields = [f.name for f in arcpy.ListFields(input_fc)]

            if "soi_drone_survey_date" in available_fields:
                sort_field = "soi_drone_survey_date"
                if verbose:
                    print_info("      Using soi_drone_survey_date for sorting")
            else:
                if verbose:
                    print_info("      Using OBJECTID for sorting (soi_drone_survey_date not found)")

            # Create update field list
            update_fields = [sort_field]
            if soi_plot_field:
                update_fields.append(soi_plot_field)
            if clr_plot_field:
                update_fields.append(clr_plot_field)

            if len(update_fields) == 1:  # Only sort field, no plot fields
                print_info("      WARNING: No plot fields found to renumber")
                return True

            # Get features in sorted order and renumber
            with arcpy.da.UpdateCursor(input_fc, update_fields) as cursor:
                plot_number = 1
                for row in cursor:
                    # Update soi_plot_no if field exists
                    if soi_plot_field:
                        soi_plot_index = update_fields.index(soi_plot_field)
                        row[soi_plot_index] = plot_number

                    # Update clr_plot_no if field exists
                    if clr_plot_field:
                        clr_plot_index = update_fields.index(clr_plot_field)
                        row[clr_plot_index] = plot_number

                    # Update the row
                    cursor.updateRow(row)
                    plot_number += 1

                    if verbose and plot_number % 100 == 0:
                        print_info("        Renumbered {} of {} plots".format(plot_number - 1, total_features))

            print_info("    Successfully renumbered {} plots sequentially".format(total_features - 1))

            # Verify the renumbering
            if verbose:
                print_info("      Verification: checking renumbered plot numbers")
                with arcpy.da.SearchCursor(input_fc, update_fields[:1] + [f for f in update_fields[1:] if f]) as verify_cursor:
                    first_few = []
                    for i, row in enumerate(verify_cursor):
                        if i < 5:  # Show first 5
                            first_few.append(row)
                        elif i >= 5:
                            break

                    for row in first_few:
                        print_info("      {}: {} {}".format(sort_field, *[str(val) for val in row[1:]]))

            return True

        except Exception as e:
            print_info("    ERROR renumbering plot numbers: {}".format(e))
            import traceback
            traceback.print_exc()
            return False

    def _reset_objectids(self, input_fc, verbose=False):
        """Reset OBJECTIDs to start from 1 using safe copy-rename approach"""
        try:
            import arcpy

            if verbose:
                print_info("    Starting OBJECTID reset to start from 1...")

            # Check current OBJECTID range
            current_oids = []
            with arcpy.da.SearchCursor(input_fc, ["OID@"]) as cursor:
                for row in cursor:
                    current_oids.append(row[0])

            if not current_oids:
                if verbose:
                    print_info("      No features found for OBJECTID reset")
                return True

            current_min = min(current_oids)
            current_max = max(current_oids)

            if verbose:
                print_info("      Current OBJECTID range: {} to {}".format(current_min, current_max))
                print_info("      Total features: {}".format(len(current_oids)))

            # If OBJECTIDs already start from 1 and are sequential, skip reset
            expected_oids = list(range(1, len(current_oids) + 1))
            if current_oids == expected_oids:
                if verbose:
                    print_info("      OBJECTIDs already start from 1 and are sequential - skipping reset")
                return True

            # Get workspace and feature class name
            workspace = os.path.dirname(input_fc)
            fc_name = os.path.basename(input_fc)

            # Create temporary names
            backup_fc_name = "backup_before_oid_reset_{}".format(fc_name)
            backup_fc_path = os.path.join(workspace, backup_fc_name)
            temp_fc_name = "temp_for_oid_reset_{}".format(fc_name)
            temp_fc_path = os.path.join(workspace, temp_fc_name)

            # Clean up any existing temp/backup files
            for temp_path in [backup_fc_path, temp_fc_path]:
                if arcpy.Exists(temp_path):
                    arcpy.management.Delete(temp_path)

            if verbose:
                print_info("      Creating backup copy...")

            # Step 1: Copy original to backup (preserves original OBJECTIDs)
            arcpy.management.CopyFeatures(input_fc, backup_fc_path)

            if verbose:
                print_info("      Creating new feature class with sequential OBJECTIDs...")

            # Step 2: Copy to new location to get sequential OBJECTIDs
            arcpy.management.CopyFeatures(input_fc, temp_fc_path)

            # Verify the copy has sequential OBJECTIDs
            temp_oids = []
            with arcpy.da.SearchCursor(temp_fc_path, ["OID@"]) as cursor:
                for row in cursor:
                    temp_oids.append(row[0])

            if verbose:
                print_info("      New OBJECTID range: {} to {}".format(min(temp_oids), max(temp_oids)))

            # Step 3: Replace original with the sequential one
            if verbose:
                print_info("      Replacing original feature class...")

            # Delete original
            arcpy.management.Delete(input_fc)

            # Rename temp to original name
            arcpy.management.Rename(temp_fc_path, fc_name)

            # Verify the final result
            final_oids = []
            with arcpy.da.SearchCursor(input_fc, ["OID@"]) as cursor:
                for row in cursor:
                    final_oids.append(row[0])

            expected_final_oids = list(range(1, len(final_oids) + 1))
            if final_oids == expected_final_oids:
                if verbose:
                    print_info("      OBJECTID reset completed successfully")
                    print_info("      Final OBJECTID range: {} to {}".format(min(final_oids), max(final_oids)))

                # Clean up backup file
                if arcpy.Exists(backup_fc_path):
                    arcpy.management.Delete(backup_fc_path)
                    if verbose:
                        print_info("      Cleaned up backup file")

                return True
            else:
                # Restore from backup if reset failed
                if verbose:
                    print_info("      OBJECTID reset failed - restoring from backup...")
                if arcpy.Exists(backup_fc_path):
                    arcpy.management.CopyFeatures(backup_fc_path, input_fc)
                raise Exception("OBJECTID reset failed - sequential numbering not achieved")

        except Exception as e:
            print_info("    ERROR resetting OBJECTIDs: {}".format(e))
            import traceback
            traceback.print_exc()

            # Emergency cleanup - try to restore from backup if it exists
            try:
                if 'backup_fc_path' in locals() and arcpy.Exists(backup_fc_path):
                    arcpy.management.CopyFeatures(backup_fc_path, input_fc)
                    if verbose:
                        print_info("      Restored from backup due to error")
            except:
                pass

            # Clean up any temporary files
            try:
                for temp_path in ['backup_fc_path', 'temp_fc_path']:
                    if temp_path in locals() and arcpy.Exists(temp_path):
                        arcpy.management.Delete(temp_path)
            except:
                pass

            return False

    def _extract_exterior_rings(self, geom, verbose=False):
        """Extract exterior rings from polygon geometry, removing interior rings (holes)"""
        try:
            import arcpy

            if geom.type != "polygon":
                return geom

            # Collect all exterior rings
            exterior_polygons = []

            for part_index in range(geom.partCount):
                part = geom.getPart(part_index)
                if part:
                    # Extract points for this part
                    exterior_points = []
                    for point in part:
                        if point:
                            exterior_points.append(point)
                        else:
                            # NULL separator indicates end of exterior ring, start of interior ring
                            # Stop at first NULL to get only exterior ring
                            break

                    # Create polygon from exterior points only
                    if len(exterior_points) >= 3:
                        try:
                            exterior_poly = arcpy.Polygon(arcpy.Array(exterior_points))
                            if not exterior_poly.isEmpty:
                                exterior_polygons.append(exterior_poly)
                        except:
                            if verbose:
                                print_warning("        Could not create exterior polygon from part {}".format(part_index))

            # If we have multiple exterior polygons, create multipart or return the largest
            if not exterior_polygons:
                return None
            elif len(exterior_polygons) == 1:
                return exterior_polygons[0]
            else:
                # Return the largest exterior polygon
                largest_poly = max(exterior_polygons, key=lambda p: p.area)
                if verbose:
                    print_info("        Multiple exterior parts found, using largest (area: {:.1f})".format(largest_poly.area))
                return largest_poly

        except Exception as e:
            if verbose:
                print_warning("        Error extracting exterior rings: {}".format(e))
            return None

    def _restart_sanitization(self, input_fc, verbose=False):
        """Restart sanitization process from beginning after hole deletion"""
        if verbose:
            print_info("    Restarting sanitization from beginning after hole deletion")

        # Recursive call to restart sanitization with fresh data
        return self.sanitize_feature_class(input_fc, verbose)


# Alias for backward compatibility
def print_info(msg):
    """Print info message"""
    print(format_message(msg))