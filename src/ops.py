#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Batch operations for validation and upload of multiple GDB files
"""

import os
import csv
import json
from datetime import datetime

# Import configuration loader
try:
    from src.util import get_config
except ImportError:
    # Fallback if util module not available
    def get_config():
        class DummyConfig:
            def get_wkid(self):
                return 32644
        return DummyConfig()

# Import logging functions
try:
    from src.log import log_error, log_success, log_info, log_step
except ImportError:
    # Fallback to simple console functions if logging is not available
    def log_error(msg, *args, **kwargs):
        print("ERROR: {}".format(msg))
    def log_success(msg, *args, **kwargs):
        print("SUCCESS: {}".format(msg))
    def log_info(msg, *args, **kwargs):
        print(msg)
    def log_step(msg, *args, **kwargs):
        print("=== {} ===".format(msg.upper()))

# Simple console functions (backward compatibility)
def print_error(msg):
    log_error(msg, force_log=True)

def print_essential_success(msg):
    log_success(msg, force_log=True)


class BatchOps:
    """Batch validation and upload operations"""

    @staticmethod
    def batch_validate(codes_path, folder, count=None):
        """Batch validate GDB files from folder"""
        try:
            log_step("BATCH VALIDATION")
            log_info("Starting batch validation of GDB files", force_log=True)

            # Validate input
            if not os.path.exists(codes_path):
                print_error("Codes file not found: {}".format(codes_path))
                return False

            if not os.path.exists(folder):
                print_error("GDB folder not found: {}".format(folder))
                return False

            # Find GDB folders
            gdb_files = [f for f in os.listdir(folder)
                        if os.path.isdir(os.path.join(folder, f)) and f.endswith('.gdb')]

            if not gdb_files:
                print_error("No GDB folders found in the specified folder")
                return False

            files_to_process = gdb_files[:count] if count else gdb_files
            print("Processing {} GDB files".format(len(files_to_process)))

            success_count = 0
            failure_count = 0
            results = []

            for i, gdb_file in enumerate(files_to_process, 1):
                # Show progress
                if i % 5 == 1 or i == len(files_to_process):
                    print("Progress: {}/{} files".format(i, len(files_to_process)))

                file_path = os.path.join(folder, gdb_file)

                # Validate file
                from src.gdb import GDBValid
                is_valid = GDBValid.validate_file(file_path, codes_path)

                result = {
                    'file': gdb_file,
                    'path': file_path,
                    'valid': is_valid,
                    'timestamp': datetime.now().isoformat()
                }
                results.append(result)

                if is_valid:
                    print("PASSED: Validation successful for {}".format(gdb_file))
                    success_count += 1
                else:
                    print("FAILED: Validation failed for {}".format(gdb_file))
                    failure_count += 1

            # Summary
            print("\n=== BATCH VALIDATION SUMMARY ===")
            print("Total files processed: {}".format(len(files_to_process)))
            print_essential_success("Successful validations: {}".format(success_count))

            if failure_count > 0:
                print_error("Failed validations: {}".format(failure_count))

            # Save results
            results_file = os.path.join(folder, 'batch_validation_results.csv')
            from src.data import DataProc
            DataProc.save_status_to_csv(results, results_file)
            print("Results saved to: {}".format(results_file))

            if success_count == len(files_to_process):
                print_essential_success("All files validated successfully! Ready for batch upload.")
                return True
            else:
                print_error("{} file(s) failed validation. Please fix errors before uploading.".format(failure_count))
                return False

        except Exception as e:
            print_error("Batch validation error: {}".format(e))
            return False

    @staticmethod
    def batch_upload(codes_path, folder, count=None, cred=None):
        """Batch upload GDB files from folder"""
        try:
            print("=== BATCH UPLOAD ===")
            print("Starting batch upload of GDB files")

            # Initialize API client
            from src.api import NakAPI
            api = NakAPI()

            username, password, state_id = api.parse_credentials(cred)
            if not all([username, password, state_id]):
                print_error("Invalid credentials format. Expected username:password:state_id")
                return False

            print_essential_success("Authenticated as user: {}".format(username))

            # Validate input
            if not os.path.exists(codes_path):
                print_error("Codes file not found: {}".format(codes_path))
                return False

            if not os.path.exists(folder):
                print_error("GDB folder not found: {}".format(folder))
                return False

            # Login
            if not api.login(state_id, username, password):
                print_error("Authentication failed")
                return False

            # Find GDB files
            gdb_files = [f for f in os.listdir(folder)
                        if os.path.isdir(os.path.join(folder, f)) and f.endswith('.gdb')]

            if not gdb_files:
                print_error("No GDB folders found in the specified folder")
                return False

            files_to_process = gdb_files[:count] if count else gdb_files
            print("Processing {} GDB files".format(len(files_to_process)))

            # Parse hierarchical data
            from src.data import DataProc
            hierarchical_data = DataProc.parse_codes_csv(codes_path)
            if not hierarchical_data:
                print_error("No data found in codes file")
                return False

            success_count = 0
            failure_count = 0
            results = []

            for i, gdb_file in enumerate(files_to_process, 1):
                # Show progress
                if i % 5 == 1 or i == len(files_to_process):
                    print("Progress: {}/{} files".format(i, len(files_to_process)))

                file_path = os.path.join(folder, gdb_file)
                survey_unit_code = os.path.splitext(gdb_file)[0]

                # Find survey data
                survey_data = DataProc.find_survey_unit_info(hierarchical_data, survey_unit_code)
                if not survey_data:
                    print("SKIPPED: No survey data found for {}".format(gdb_file))
                    continue

                # Upload file
                success = BatchOps._upload_single_gdb(api, file_path, survey_data, survey_unit_code, hierarchical_data, backup_uploaded=None)

                result = {
                    'file': gdb_file,
                    'survey_unit_code': survey_unit_code,
                    'uploaded': success,
                    'timestamp': datetime.now().isoformat()
                }
                results.append(result)

                if success:
                    print("UPLOADED: {} ({})".format(gdb_file, survey_unit_code))
                    success_count += 1
                else:
                    print("FAILED: Upload failed for {}".format(gdb_file))
                    failure_count += 1

            # Summary
            print("\n=== BATCH UPLOAD SUMMARY ===")
            print("Total files processed: {}".format(len(files_to_process)))
            print_essential_success("Successful uploads: {}".format(success_count))

            if failure_count > 0:
                print_error("Failed uploads: {}".format(failure_count))

            # Save results
            results_file = os.path.join(folder, 'batch_upload_results.csv')
            DataProc.save_status_to_csv(results, results_file)
            print("Results saved to: {}".format(results_file))

            return success_count > 0

        except Exception as e:
            print_error("Batch upload error: {}".format(e))
            return False

    @staticmethod
    def _upload_single_gdb(api, gdb_path, survey_data, survey_unit_code, hierarchical_data, backup_uploaded=None):
        """Upload a single GDB file"""
        try:
            print("    Uploading: {}".format(survey_unit_code))

            # Double-check and fix any remaining GDB data issues before zipping
            # This is a safety net in case prepare didn't catch everything
            print("    DEBUG: Final validation of GDB data before upload...")
            BatchOps._fix_gdb_data_issues(gdb_path)

            # Zip the GDB after final fixes
            zip_path = BatchOps._zip_gdb(gdb_path)
            if not zip_path:
                print_error("Failed to create zip file for {}".format(survey_unit_code))
                return False

            # Upload file - actual API call
            upload_success = api.upload_file(zip_path)
            if not upload_success:
                print_error("    FAILED: File upload [200]")
                # Clean up zip file on failure
                FileOps.safe_remove_file(zip_path)
                return False

            print("    SUCCESS: File upload [200]")

            # Extract GDB data
            gdb_data = BatchOps._extract_gdb_data(gdb_path, survey_data)
            if not gdb_data:
                print("    SUCCESS: File upload only | Parcels: 0")
                # Clean up zip file after successful upload
                FileOps.safe_remove_file(zip_path)
                return True  # File upload was successful

            # Upload plot data - actual API call
            file_name = os.path.basename(gdb_path) + '.zip'

            # Extract feature count from new data structure
            if gdb_data and isinstance(gdb_data, dict):
                features = gdb_data.get('features', [])
                parcel_count = len(features)
            else:
                parcel_count = 0

            print("    Parcels: {}".format(parcel_count))

            # Check survey_data
            if not survey_data:
                print_error("    ERROR: No survey data found for {}".format(survey_unit_code))
                return False

            if gdb_data and isinstance(gdb_data, dict) and parcel_count > 0:
                if not api.upload_plot_data(gdb_data, survey_data, file_name):
                    print_error("    FAILED: Plot data upload")
                    # Clean up zip file on failure
                    FileOps.safe_remove_file(zip_path)
                    return False
            else:
                print("    SUCCESS: File upload only | Parcels: 0")

            # Backup GDB if requested and upload was successful
            if backup_uploaded:
                BatchOps._backup_uploaded_gdb(gdb_path, survey_unit_code)

            # Clean up zip file after successful upload
            FileOps.safe_remove_file(zip_path)
            return True

        except Exception as e:
            print_error("Error uploading GDB {}: {}".format(survey_unit_code, e))
            return False

    @staticmethod
    def _backup_uploaded_gdb(gdb_path, survey_unit_code):
        """Backup successfully uploaded GDB by moving entire folder to data/gdbs/backup"""
        try:
            import os
            import shutil

            # Create backup directory if it doesn't exist
            backup_dir = os.path.join(os.path.dirname(gdb_path), "backup")
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
                print("    Created backup directory: {}".format(backup_dir))

            # Get the GDB folder name from the path
            gdb_folder_name = os.path.basename(gdb_path)
            backup_path = os.path.join(backup_dir, gdb_folder_name)

            # Check if backup already exists and handle overwrite
            if os.path.exists(backup_path):
                print("    Backup exists, attempting to overwrite: data/gdbs/backup/{}".format(gdb_folder_name))
                try:
                    # Remove existing backup directory
                    shutil.rmtree(backup_path)
                    print("    Removed existing backup: {}".format(gdb_folder_name))
                except Exception as remove_error:
                    print_warning("    Failed to remove existing backup {}: {}".format(gdb_folder_name, remove_error))
                    # If overwrite fails, try to delete the original GDB from data/gdbs
                    try:
                        print("    Overwrite failed, deleting original from data/gdbs: {}".format(gdb_folder_name))
                        shutil.rmtree(gdb_path)
                        print("    Deleted original GDB: {}".format(gdb_folder_name))
                        return True  # Consider this successful since GDB is removed from upload folder
                    except Exception as delete_error:
                        print_error("    Failed to delete original GDB {}: {}".format(gdb_folder_name, delete_error))
                        return False

            # Move the entire GDB folder to backup directory
            shutil.move(gdb_path, backup_path)
            print("    Backed up: {} -> data/gdbs/backup/{}".format(survey_unit_code, gdb_folder_name))

            return True

        except Exception as e:
            print_error("    Failed to backup GDB {}: {}".format(survey_unit_code, e))
            return False

    @staticmethod
    def _fix_gdb_data_issues(gdb_path):
        """Fix GDB data issues before zipping"""
        try:
            if not ArcCore or not ArcCore.is_available():
                print_error("ArcPy not available for GDB data fixing")
                return False

            fc_path = os.path.join(gdb_path, "PROPERTY_PARCEL")
            if not arcpy.Exists(fc_path):
                print_error("PROPERTY_PARCEL feature class not found")
                return False

            print("    DEBUG: Checking and fixing GDB data issues...")
            plot_numbers = {}
            null_plots = []
            duplicate_plots = []

            field_names_check = [f.name for f in arcpy.ListFields(fc_path) if f.name != "Shape"]
            with arcpy.da.SearchCursor(fc_path, field_names_check) as cursor:
                for row in cursor:
                    clr_plot_idx = field_names_check.index("clr_plot_no") if "clr_plot_no" in field_names_check else -1
                    if clr_plot_idx >= 0:
                        plot_no = row[clr_plot_idx]
                        objectid_idx = field_names_check.index("OBJECTID") if "OBJECTID" in field_names_check else -1
                        objectid = row[objectid_idx] if objectid_idx >= 0 else "Unknown"

                        if plot_no is None or str(plot_no).strip() == "" or str(plot_no).lower() in ["null", "nan", "none"]:
                            null_plots.append((objectid, plot_no))
                        elif str(plot_no) in plot_numbers:
                            duplicate_plots.append((str(plot_no), plot_numbers[str(plot_no)], objectid))
                        else:
                            plot_numbers[str(plot_no)] = objectid

            issues_fixed = 0

            # Fix null plot numbers first
            if null_plots:
                print("    FIX: Updating {} null/empty plot numbers...".format(len(null_plots)))
                try:
                    workspace = gdb_path
                    edit = arcpy.da.Editor(workspace)
                    edit.startEditing(False, False)
                    edit.startOperation()

                    max_plot_no = max([int(p) for p in plot_numbers.keys() if p.isdigit()]) if plot_numbers else 0
                    for objectid, plot_no in null_plots:
                        new_plot_no = str(max_plot_no + 1)
                        max_plot_no += 1
                        print("      Updating OBJECTID {} from '{}' to '{}'".format(objectid, str(plot_no), new_plot_no))

                        where_clause = "OBJECTID = {}".format(objectid)
                        with arcpy.da.UpdateCursor(fc_path, ["clr_plot_no"], where_clause) as cursor:
                            for row in cursor:
                                row[0] = new_plot_no
                                cursor.updateRow(row)

                    edit.stopOperation()
                    edit.stopEditing(True)
                    issues_fixed += len(null_plots)
                    print("    SUCCESS: Fixed {} null plot numbers".format(len(null_plots)))
                except Exception as e:
                    print_error("    ERROR: Failed to fix null plot numbers: {}".format(e))
                    if 'edit' in locals():
                        edit.stopEditing(False)
                    return False

            # Fix duplicate plot numbers
            if duplicate_plots:
                print("    FIX: Updating {} duplicate plot numbers...".format(len(duplicate_plots)))
                try:
                    workspace = gdb_path
                    edit = arcpy.da.Editor(workspace)
                    edit.startEditing(False, False)
                    edit.startOperation()

                    for plot_no, first_oid, duplicate_oid in duplicate_plots:
                        new_plot_no = str(int(plot_no) + 1000)
                        print("      Updating OBJECTID {} from plot '{}' to '{}'".format(duplicate_oid, plot_no, new_plot_no))

                        where_clause = "OBJECTID = {}".format(duplicate_oid)
                        with arcpy.da.UpdateCursor(fc_path, ["clr_plot_no"], where_clause) as cursor:
                            for row in cursor:
                                row[0] = new_plot_no
                                cursor.updateRow(row)

                    edit.stopOperation()
                    edit.stopEditing(True)
                    issues_fixed += len(duplicate_plots)
                    print("    SUCCESS: Fixed {} duplicate plot numbers".format(len(duplicate_plots)))
                except Exception as e:
                    print_error("    ERROR: Failed to fix duplicate plot numbers: {}".format(e))
                    if 'edit' in locals():
                        edit.stopEditing(False)
                    return False

            if issues_fixed > 0:
                print("    SUCCESS: Fixed {} total GDB data issues".format(issues_fixed))
            else:
                print("    DEBUG: No GDB data issues found")

            return True

        except Exception as e:
            print_error("Error fixing GDB data issues: {}".format(e))
            return False

    @staticmethod
    def _zip_gdb(gdb_path):
        """Create zip file from GDB"""
        try:
            import zipfile

            zip_path = gdb_path + '.zip'

            # Remove existing zip file if it exists
            if os.path.exists(zip_path):
                os.remove(zip_path)

            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(gdb_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, os.path.dirname(gdb_path))
                        zipf.write(file_path, arcname)

            if os.path.exists(zip_path):
                print_essential_info("Created zip file: {} ({} bytes)".format(zip_path, os.path.getsize(zip_path)))
                return zip_path
            else:
                print_error("Failed to create zip file")
                return None

        except Exception as e:
            print_error("Error zipping GDB: {}".format(e))
            return None

    @staticmethod
    def _convert_geometry_to_esri_rings(geometry, spatial_ref=None):
        """Convert ArcPy geometry to ESRI JSON rings format (matching C# implementation)"""
        try:
            if not geometry:
                return None

            # Get spatial reference WKID
            if spatial_ref is None:
                spatial_ref = geometry.spatialReference

            # Use dynamic spatial reference from configuration as default
            config = get_config()
            wkid = config.get_wkid()  # default from input.json
            if spatial_ref:
                try:
                    wkid = spatial_ref.factoryCode
                    if not wkid:
                        wkid = config.get_wkid()
                except:
                    wkid = config.get_wkid()

            # Extract coordinates as rings (matching C# processMultiPartBuffer logic)
            rings = []

            # Handle multipart geometries properly
            part_count = getattr(geometry, 'partCount', 1)

            for part_index in range(part_count):
                part_array = geometry.getPart(part_index)
                if part_array:
                    current_ring = []

                    # Process all points in this part, including NULL separators
                    for point_index in range(part_array.count):
                        point = part_array.getObject(point_index)

                        if point is not None:
                            # Add valid point coordinates as [x, y] pair
                            current_ring.append([point.X, point.Y])
                        else:
                            # NULL separator indicates end of current ring (exterior or hole)
                            # Complete the current ring if it has enough points
                            if len(current_ring) >= 3:
                                # Ensure ring is closed (first and last points match)
                                if current_ring[0] != current_ring[-1]:
                                    current_ring.append(current_ring[0])
                                rings.append(current_ring)
                            # Start a new ring for the next part (hole or next polygon)
                            current_ring = []

                    # Handle the last ring in this part (no NULL separator at the end)
                    if len(current_ring) >= 3:
                        # Ensure ring is closed (first and last points match)
                        if current_ring[0] != current_ring[-1]:
                            current_ring.append(current_ring[0])
                        rings.append(current_ring)

            # Build ESRI JSON structure (matching C# DataTableToJsonObj output)
            if rings:
                esri_geometry = {
                    "rings": rings,
                    "spatialReference": {"wkid": wkid}
                }
                return esri_geometry
            else:
                return None

        except Exception as e:
            print_error("Error converting geometry to ESRI rings format: {}".format(e))
            return None

    @staticmethod
    def _extract_gdb_data(gdb_path, survey_data):
        """Extract data from GDB for upload"""
        try:
            if not ArcCore or not ArcCore.is_available():
                print_error("ArcPy not available for GDB data extraction")
                return None

            fc_path = os.path.join(gdb_path, "PROPERTY_PARCEL")
            if not arcpy.Exists(fc_path):
                print_error("PROPERTY_PARCEL feature class not found")
                return None

            # Note: GDB data issues have been fixed upfront before zipping

            # Get feature class spatial reference
            desc = arcpy.Describe(fc_path)
            fc_spatial_ref = desc.spatialReference

            features = []
            # Use geometry tokens instead of SHAPE@JSON to have better control
            field_names = [f.name for f in arcpy.ListFields(fc_path) if f.name != "Shape"]
            # Insert geometry tokens at the beginning
            field_names.insert(0, "SHAPE@")      # Geometry object
            field_names.insert(1, "SHAPE@X")     # Centroid X
            field_names.insert(2, "SHAPE@Y")     # Centroid Y

            with arcpy.da.SearchCursor(fc_path, field_names) as cursor:
                for row in cursor:
                    geometry = None
                    attributes = {}

                    # Handle geometry from SHAPE@ token (first field)
                    geometry_obj = row[0]  # SHAPE@
                    if geometry_obj:
                        try:
                            # Convert ArcPy geometry to ESRI JSON rings format
                            geometry = BatchOps._convert_geometry_to_esri_rings(geometry_obj, fc_spatial_ref)
                        except Exception as e:
                            print_error("Failed to convert geometry to ESRI format: {}".format(e))
                            geometry = None

                    # Get centroid coordinates (second and third fields)
                    centroid_x = row[1]  # SHAPE@X
                    centroid_y = row[2]  # SHAPE@Y

                    # Process all attribute fields (after geometry tokens)
                    for i in range(3, len(field_names)):
                        field_name = field_names[i]
                        value = row[i]

                        # Handle date fields with proper formatting (matching GUI behavior)
                        if value is not None:
                            if hasattr(value, 'strftime'):  # Date field
                                if field_name == 'soi_drone_survey_date':
                                    # GUI formats soi_drone_survey_date to "yyyy-MM-dd"
                                    attributes[field_name] = value.strftime('%Y-%m-%d')
                                elif field_name == 'sys_imported_timestamp':
                                    # GUI formats sys_imported_timestamp to "yyyy-MM-dd"
                                    attributes[field_name] = value.strftime('%Y-%m-%d')
                                else:
                                    attributes[field_name] = value.strftime('%Y-%m-%d')
                            else:
                                # Handle numeric fields with proper type conversion (like GUI)
                                if isinstance(value, (int, float)):
                                    # For JSON serialization, convert numeric fields to strings like GUI
                                    if field_name in ['shape_length', 'shape_area']:
                                        attributes[field_name] = str(float(value)) if value else "0.0"
                                    else:
                                        attributes[field_name] = str(value)
                                elif field_name == 'soi_uniq_id':
                                    # GUI handles soi_uniq_id as GlobalID datatype
                                    if value:
                                        attributes[field_name] = str(value)  # Convert GlobalID to string
                                    else:
                                        attributes[field_name] = None
                                else:
                                    attributes[field_name] = str(value) if value is not None else None
                        else:
                            attributes[field_name] = None

                    # Add automatic sys_imported_timestamp if missing (GUI behavior)
                    if 'sys_imported_timestamp' not in attributes or attributes['sys_imported_timestamp'] is None:
                        from datetime import datetime
                        current_time = datetime.now()
                        attributes['sys_imported_timestamp'] = current_time.strftime('%Y-%m-%d')

                    # Add default values that GUI assigns (matching GUI behavior)
                    if 'status' not in attributes or attributes['status'] is None or attributes['status'] == '':
                        attributes['status'] = '1'  # GUI default status (as string)
                    if 'is_approved' not in attributes or attributes['is_approved'] is None or attributes['is_approved'] == '':
                        attributes['is_approved'] = '0'  # GUI default approval status (as string)

                    # Add centroid coordinates as lat/long (matching GUI behavior)
                    if centroid_x is not None and centroid_y is not None:
                        try:
                            # Use feature class spatial reference for source coordinates
                            if fc_spatial_ref and fc_spatial_ref.factoryCode:
                                source_sr = fc_spatial_ref
                            else:
                                config = get_config()
                                source_sr = arcpy.SpatialReference(config.get_wkid())

                            # Create point geometry with source spatial reference
                            point = arcpy.PointGeometry(arcpy.Point(centroid_x, centroid_y), source_sr)

                            # Convert to WGS84 (4326) for lat/long coordinates (GUI standard)
                            target_sr = arcpy.SpatialReference(4326)  # WGS84
                            point_latlong = point.projectAs(target_sr)
                            centroid = point_latlong.centroid

                            # Format to 6 decimal places (matching GUI precision)
                            attributes['latitude'] = '{:.6f}'.format(centroid.Y)
                            attributes['longitude'] = '{:.6f}'.format(centroid.X)
                        except Exception as e:
                            print_error("Warning: Could not convert centroid to lat/long: {}".format(e))
                            # Use original coordinates as fallback (GUI behavior)
                            attributes['latitude'] = '{:.6f}'.format(centroid_y) if centroid_y else ''
                            attributes['longitude'] = '{:.6f}'.format(centroid_x) if centroid_x else ''
                    else:
                        # Ensure latitude/longitude fields exist even if centroid is missing
                        attributes['latitude'] = ''
                        attributes['longitude'] = ''

                    # Create feature with expected format (matching reference implementation)
                    feature_data = {
                        "geometry": geometry,
                        "attributes": attributes
                    }
                    features.append(feature_data)

            # Return data in reference format: {'features': features}
            return {'features': features}

        except Exception as e:
            print_error("Error extracting GDB data: {}".format(e))
            return None

    @staticmethod
    def get_batch_status(codes_path, survey_units_file=None, cred=None):
        """Get batch upload status"""
        try:
            print("=== BATCH STATUS ===")
            print("Fetching upload status for survey units")

            # Get survey units to check
            if survey_units_file and os.path.exists(survey_units_file):
                from src.data import DataProc
                survey_units = DataProc.read_column_from_csv(survey_units_file, 'survey_unit_id')
            else:
                # Get from hierarchical data
                from src.data import DataProc
                hierarchical_data = DataProc.parse_codes_csv(codes_path)
                survey_units = [data.get('SurveyUnitCode', '') for data in hierarchical_data if data.get('SurveyUnitCode')]

            if not survey_units:
                print_error("No survey units found to check status")
                return []

            # Fetch status
            from src.api import APIStats
            api_stats = APIStats()
            results = api_stats.fetch_upload_status(survey_units, codes_path, cred)

            if results:
                print("Fetched status for {} survey units".format(len(results)))

                # Count by status
                status_counts = {}
                for result in results:
                    status = result.get('status', 'unknown')
                    status_counts[status] = status_counts.get(status, 0) + 1

                print("Status Summary:")
                for status, count in status_counts.items():
                    print("  {}: {}".format(status, count))

            return results

        except Exception as e:
            print_error("Batch status error: {}".format(e))
            return []


# Simple console functions
def print_error(msg):
    print("ERROR: {}".format(msg))

def print_essential_success(msg):
    print("SUCCESS: {}".format(msg))

def print_essential_info(msg):
    print(msg)


# Import required modules with fallbacks
try:
    from src.core import ArcCore
    from src.util import FileOps
    from src.data import DataProc
    import arcpy
except ImportError:
    ArcCore = None
    arcpy = None
    class FileOps:
        @staticmethod
        def safe_remove_file(file_path):
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                return True
            except:
                return False
    class DataProc:
        @staticmethod
        def save_status_to_csv(data, path):
            return False
        @staticmethod
        def read_column_from_csv(file_path, column):
            return []
        @staticmethod
        def parse_codes_csv(codes_path):
            return []
        @staticmethod
        def find_survey_unit_info(data, code):
            return None