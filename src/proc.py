#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data processing workflow orchestration for CSV operations and command coordination
"""

import os
import sys
from datetime import datetime

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

def print_essential_info(msg):
    print(format_message(msg))  # Keep this as direct print for now

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

# Import required modules with fallbacks
try:
    from src.core import ArcCore
    from src.data import DataProc
    from src.gdb import GDBProc, GDBValid
    from src.util import FileOps
    from src.ops import BatchOps
    import arcpy
except ImportError:
    ArcCore = None
    arcpy = None
    class DataProc:
        @staticmethod
        def parse_data_csv(data_path): return {'prepare': [], 'validate': [], 'upload': []}
        @staticmethod
        def parse_codes_csv(codes_path): return []
        @staticmethod
        def find_survey_unit_info(data, code): return None
        @staticmethod
        def save_status_to_csv(data, path): return False
    class FileOps:
        @staticmethod
        def ensure_dir_exists(path):
            try: os.makedirs(path); return True
            except: return False
    class GDBProc:
        @staticmethod
        def prepare_gdbs(*args, **kwargs): return False
    class GDBValid:
        @staticmethod
        def validate_file(*args, **kwargs): return False
    class BatchOps:
        @staticmethod
        def _upload_single_gdb(*args, **kwargs): return False


class DataWorkflows:
    """Main workflow orchestrator for data processing operations"""

    @staticmethod
    def process_prepare_column(codes_path, blocks_gdb, parcels_gdb, output_folder='gdbs', count=None, force=False, buffer_distance=100, featcount=None):
        """Process prepare column from data.csv"""
        try:
            log_step("PROCESS PREPARE COLUMN")
            log_info("Processing survey unit codes from data.csv", force_log=True)

            # Get survey unit codes from data.csv
            from src.data import DataProc
            data_path = os.path.join(os.path.dirname(codes_path), 'data.csv')
            prepare_list = DataProc.get_survey_unit_from_suc_csv('', data_path)
            if not prepare_list:
                print_error("No survey unit codes found in data.csv")
                return False

            # Limit count if specified
            if count:
                prepare_list = prepare_list[:count]

            log_info("Found {} survey units to prepare".format(len(prepare_list)), force_log=True)

            # Process each survey unit, skipping already processed ones
            success_count = 0
            already_processed_count = 0
            for i, survey_unit in enumerate(prepare_list, 1):
                print("\nProcessing {}/{}: {}".format(i, len(prepare_list), survey_unit))

                # Check if GDB already exists and has PROPERTY_PARCEL
                gdb_path = os.path.join(output_folder, survey_unit + '.gdb')

                # Skip if GDB exists and has PROPERTY_PARCEL (unless force flag is set)
                if os.path.exists(gdb_path) and not force:
                    # Verify PROPERTY_PARCEL exists in the GDB
                    fc_path = os.path.join(gdb_path, "PROPERTY_PARCEL")
                    if arcpy.Exists(fc_path):
                        print("    SKIPPED: {} GDB with PROPERTY_PARCEL already exists".format(survey_unit))
                        print("    Use --force flag to overwrite existing GDBs")
                        already_processed_count += 1
                        continue
                    else:
                        print("    WARNING: {} GDB exists but PROPERTY_PARCEL missing - recreating".format(survey_unit))
                elif os.path.exists(gdb_path) and force:
                    print("    FORCE: Overwriting existing GDB for {}".format(survey_unit))

                # Find survey data
                hierarchical_data = DataProc.parse_codes_csv(codes_path)
                survey_data = DataProc.find_survey_unit_info(hierarchical_data, survey_unit)

                if not survey_data:
                    print("SKIPPED: No survey data found for {}".format(format_message(survey_unit)))
                    continue

                # Create GDB for this specific survey unit
                print("Creating GDB for: {}".format(format_message(survey_unit)))
                success = GDBProc.create_survey_unit_gdb(
                    survey_data, blocks_gdb, parcels_gdb, output_folder, force=force, buffer_distance=buffer_distance, featcount=featcount
                )

                if success:
                    success_count += 1
                    log_success("GDB created for {}".format(format_message(survey_unit)), force_log=True)
                else:
                    print("FAILED: GDB creation failed for {}".format(format_message(survey_unit)))

            # Summary
            print("\n=== PREPARE SUMMARY ===")
            print("Total survey units: {}".format(len(prepare_list)))
            print("Already processed: {}".format(already_processed_count))
            print("Successfully processed: {}".format(success_count))
            print("Failed: {}".format(len(prepare_list) - success_count - already_processed_count))

            return (success_count > 0) or (already_processed_count > 0)

        except Exception as e:
            print_error("Error processing prepare column: {}".format(e))
            return False

    @staticmethod
    def process_validate_column(codes_path, gdb_folder, count=None):
        """Process validate all GDB files in folder"""
        try:
            print("=== PROCESS VALIDATE COLUMN ===")
            print("Validating all GDB files in data/gdbs folder")

            # Get GDB files from folder
            from src.data import DataProc
            gdb_files = DataProc.get_gdb_files_from_folder(gdb_folder)

            # Convert GDB files to survey unit list for processing
            validate_list = [DataProc.extract_survey_unit_from_gdb_path(gdb_path) for gdb_path in gdb_files]
            validate_list = [su for su in validate_list if su]  # Remove None values
            if not validate_list:
                print_error("No GDB files found in data/gdbs folder")
                return False

            # Limit count if specified
            if count:
                validate_list = validate_list[:count]

            print("Found {} GDB files to validate".format(len(validate_list)))

            # Validate each survey unit
            success_count = 0
            results = []

            for i, survey_unit in enumerate(validate_list, 1):
                print("\nValidating {}/{}: {}".format(i, len(validate_list), survey_unit))

                # Check GDB file exists
                gdb_path = os.path.join(gdb_folder, survey_unit + '.gdb')

                # Find GDB file
                if not os.path.exists(gdb_path):
                    print("SKIPPED: GDB file not found for {}".format(survey_unit))
                    continue

                # Validate GDB
                is_valid = GDBValid.validate_file(gdb_path, codes_path)

                result = {
                    'survey_unit': survey_unit,
                    'gdb_path': gdb_path,
                    'valid': is_valid,
                    'timestamp': datetime.now().isoformat()
                }
                results.append(result)

                # Report validation result
                if is_valid:
                    success_count += 1
                    print("VALID: {}".format(survey_unit))
                else:
                    print("INVALID: {}".format(survey_unit))

            # Summary
            print("\n=== VALIDATE SUMMARY ===")
            print("Total survey units: {}".format(len(validate_list)))
            print("Valid: {}".format(success_count))
            print("Invalid: {}".format(len(validate_list) - success_count))
            
            return success_count > 0

        except Exception as e:
            print_error("Error processing validate column: {}".format(e))
            return False

    @staticmethod
    def process_upload_column(codes_path, gdb_folder, cred=None, count=None, backup_uploaded=None, force=False, debug=False):
        """Process upload all GDB files in folder"""
        try:
            print("=== PROCESS UPLOAD COLUMN ===")
            print("Uploading all GDB files in data/gdbs folder")

            # Get GDB files from folder
            from src.data import DataProc
            gdb_files = DataProc.get_gdb_files_from_folder(gdb_folder)

            # Convert GDB files to survey unit list for processing
            upload_list = [DataProc.extract_survey_unit_from_gdb_path(gdb_path) for gdb_path in gdb_files]
            upload_list = [su for su in upload_list if su]  # Remove None values
            if not upload_list:
                print_error("No GDB files found in data/gdbs folder")
                return False

            # Limit count if specified
            if count:
                upload_list = upload_list[:count]

            print("Found {} GDB files to upload".format(len(upload_list)))

            # Upload each survey unit
            success_count = 0
            skipped_count = 0
            results = []
            successful_uploads = []

            # Initialize API with NakshaUploader
            from src.api import NakshaUploader
            from src.auth import NakAuth
            api = NakshaUploader()
            auth = NakAuth()
            username, password, state_id = auth.parse_credentials(cred)

            if not all([username, password, state_id]):
                print_error("Invalid credentials provided")
                return False

            if not api.login(state_id, username, password):
                print_error("Authentication failed")
                return False

            hierarchical_data = DataProc.parse_codes_csv(codes_path)

            for i, survey_unit in enumerate(upload_list, 1):
                print("\nUploading {}/{}: {}".format(i, len(upload_list), survey_unit))

                # Check GDB file exists
                gdb_path = os.path.join(gdb_folder, survey_unit + '.gdb')

                # Find GDB file
                if not os.path.exists(gdb_path):
                    print("SKIPPED: GDB file not found for {}".format(survey_unit))
                    skipped_count += 1
                    continue

                # Find survey data
                survey_data = DataProc.find_survey_unit_info(hierarchical_data, survey_unit)
                if not survey_data:
                    print("SKIPPED: No survey data found for {}".format(format_message(survey_unit)))
                    skipped_count += 1
                    continue

                # Check upload status (GUI behavior)
                if force:
                    print("    Force mode: Skipping upload status check")
                    print("    PROCEEDING: Force uploading data...")
                else:
                    print("    Checking upload status...")
                    try:
                        status_response = api.check_gdb_upload_status(
                            state_id=survey_data.get('StateCode', 0),
                            district_id=survey_data.get('DistrictCode', 0),
                            ulb_id=survey_data.get('UlbCode', 0),
                            ward_id=survey_data.get('WardCode', 0),
                            survey_unit_id=long(survey_unit)
                        )

                        if status_response and status_response.get("message") == "GDB is already uploaded":
                            print("    WARNING: GDB is already uploaded!")
                            print("    Survey unit {} is already on the server.".format(survey_unit))

                            # Ask user for confirmation (matching GUI behavior)
                            try:
                                response = raw_input("    Do you want to upload again? (y/N): ")
                                if response.lower() not in ['y', 'yes']:
                                    print("    SKIPPED: Upload cancelled by user")
                                    skipped_count += 1
                                    continue
                                else:
                                    print("    PROCEEDING: Re-uploading existing data...")
                            except (KeyboardInterrupt, EOFError):
                                print("\n    SKIPPED: Upload cancelled")
                                skipped_count += 1
                                continue
                        else:
                            print("    Status: Ready to upload (new data)")

                    except Exception as e:
                        print("    Warning: Could not check upload status: {}".format(e))
                        print("    Proceeding with upload...")

                # Upload GDB
                success = BatchOps._upload_single_gdb(api, gdb_path, survey_data, survey_unit, hierarchical_data, backup_uploaded, debug=debug)

                result = {
                    'survey_unit': survey_unit,
                    'gdb_path': gdb_path,
                    'uploaded': success,
                    'timestamp': datetime.now().isoformat()
                }
                results.append(result)

                # Report upload result
                if success:
                    success_count += 1
                    successful_uploads.append(survey_unit)
                    print("UPLOADED: {}".format(survey_unit))
                else:
                    print("FAILED: {}".format(survey_unit))

            # Summary
            print("\n=== UPLOAD SUMMARY ===")
            print("Total survey units: {}".format(len(upload_list)))
            print("Uploaded: {}".format(success_count))
            if skipped_count > 0:
                print("Skipped: {} (already uploaded)".format(skipped_count))
            failed_count = len(upload_list) - success_count - skipped_count
            if failed_count > 0:
                print("Failed: {}".format(failed_count))
            
            return success_count > 0 or skipped_count > 0

        except Exception as e:
            print_error("Error processing upload column: {}".format(e))
            return False

    @staticmethod
    def process_export_image(survey_unit, codes_path='data/codes.csv'):
        """Process export command to download survey snapshot image from MapServer"""
        try:
            print("=== GET SNAPSHOT IMAGE ===")
            print("Downloading snapshot image for survey unit: {}".format(survey_unit))

            # Validate survey unit parameter
            if not survey_unit or not survey_unit.isdigit():
                print_error("Invalid survey unit number: {}".format(survey_unit))
                return False

            # Create data/export directory if it doesn't exist
            export_dir = 'data/export'
            if not os.path.exists(export_dir):
                os.makedirs(export_dir)
                print("Created directory: {}".format(export_dir))

            # Parse codes data to get hierarchical information
            hierarchical_data = DataProc.parse_codes_csv(codes_path)
            if not hierarchical_data:
                print_error("Failed to parse codes.csv")
                return False

            # Find survey data for the survey unit
            survey_data = DataProc.find_survey_unit_info(hierarchical_data, survey_unit)
            if not survey_data:
                print_error("No survey data found for survey unit: {}".format(survey_unit))
                return False

            print("Found survey data:")
            print("  State Code: {}".format(survey_data.get('StateCode', 'N/A')))
            print("  District Code: {}".format(survey_data.get('DistrictCode', 'N/A')))
            print("  ULB Code: {}".format(survey_data.get('UlbCode', 'N/A')))
            print("  Ward Code: {}".format(survey_data.get('WardCode', 'N/A')))

            # Build MapServer export URL using the provided template
            base_url = "https://nakshauat.dolr.gov.in/server/rest/services/NakshaSurvey/Naksha_Survey_Plots_43/MapServer/export"

            # Use the provided template parameters
            token = "yGwT6OrYwbrV8Q2arOj4j1PY-aRLhONgmxfwOPgbarRBcNGrCNbEof57FPzQWgL1E7Ch-uHxX1nlpdiY6B8WNxDh_IeS19LJdwS2PuRcVrvF3Qu5HOAbFXx65L06mzbKNGzWwOTSzOoHVV7-lTSwHg.."
            ulb_code = survey_data.get('UlbCode', '252943')

            # Build layer definition manually to avoid encoding issues
            layer_defs = "%7B%220%22%3A%22status+%3D+1+AND+ulb_lgd_cd+%3D+%27{}%27+AND+survey_unit_id%3D%27{}%27%22%7D".format(ulb_code, survey_unit)

            # Build the complete URL
            params = {
                'F': 'image',
                'FORMAT': 'PNG32',
                'TRANSPARENT': 'true',
                'token': token,
                'projection': 'EPSG:32643',
                'LAYERS': '0',
                'layerDefs': layer_defs,
                'SIZE': '1464,942',
                'BBOX': '823796.8277517445,1061269.2979925158,824856.6223482555,1061951.215007484',
                'BBOXSR': '32643',
                'IMAGESR': '32643',
                'DPI': '108'
            }

            # Build query string
            query_string = '&'.join(['{}={}'.format(k, v) for k, v in params.items()])
            full_url = "{}?{}".format(base_url, query_string)

            print("Downloading image from MapServer...")

            # Download the image
            try:
                import urllib2
                response = urllib2.urlopen(full_url, timeout=60)

                if response.code == 200:
                    # Save the image to data/export folder
                    output_file = os.path.join(export_dir, "{}.png".format(survey_unit))

                    with open(output_file, 'wb') as f:
                        f.write(response.read())

                    print("SUCCESS: Snapshot image saved to: {}".format(output_file))
                    print("Survey unit: {} snapshot downloaded successfully".format(survey_unit))
                    return True
                else:
                    print_error("Failed to download image: HTTP {}".format(response.code))
                    return False

            except Exception as e:
                print_error("Failed to download image: {}".format(e))
                return False

        except Exception as e:
            print_error("Error processing snap command: {}".format(e))
            return False

    @staticmethod
    def process_sanitize_column(gdb_folder, count=None, buffer_erase_cm=None, do_overlap_fix=None, remove_slivers=False):
        """Process sanitize all GDB files in folder"""
        try:
            print("=== PROCESS SANITIZE COLUMN ===")
            print("Sanitizing all GDB files in data/gdbs folder")

            # Get GDB files from folder
            from src.data import DataProc
            gdb_files = DataProc.get_gdb_files_from_folder(gdb_folder)

            # Convert GDB files to survey unit list for processing
            sanitize_list = [DataProc.extract_survey_unit_from_gdb_path(gdb_path) for gdb_path in gdb_files]
            sanitize_list = [su for su in sanitize_list if su]  # Remove None values
            if not sanitize_list:
                print("ERROR: No GDB files found in data/gdbs folder")
                return False

            # Limit count if specified
            if count:
                sanitize_list = sanitize_list[:count]

            print("Found {} GDB files to sanitize".format(len(sanitize_list)))

            # Sanitize each survey unit
            success_count = 0
            already_processed_count = 0

            for i, survey_unit in enumerate(sanitize_list, 1):
                print("\nSanitizing {}/{}: {}".format(i, len(sanitize_list), survey_unit))

                # Check GDB file exists
                gdb_path = os.path.join(gdb_folder, survey_unit + '.gdb')

                # Find GDB file
                if not os.path.exists(gdb_path):
                    print("SKIPPED: GDB file not found for {}".format(survey_unit))
                    continue

                # Sanitize GDB
                try:
                    from src.sani import PolygonSanitizer
                    sanitizer = PolygonSanitizer()

                    # Sanitize the PROPERTY_PARCEL feature class in the GDB
                    fc_path = os.path.join(gdb_path, "PROPERTY_PARCEL")

                    # Check if arcpy is available and feature class exists
                    try:
                        import arcpy
                        if not arcpy.Exists(fc_path):
                            print("SKIPPED: PROPERTY_PARCEL not found in {}".format(survey_unit))
                            continue
                    except ImportError:
                        print("ERROR: ArcPy not available for sanitization")
                        continue

                    print("    Sanitizing PROPERTY_PARCEL feature class...")
                    success, message, feature_count = sanitizer.sanitize_feature_class(fc_path, buffer_erase_cm=buffer_erase_cm, do_overlap_fix=do_overlap_fix, remove_slivers=remove_slivers)

                    if success:
                        success_count += 1
                        print("CLEAN: {}".format(survey_unit))
                        print("    {}".format(message))
                    else:
                        print("FAILED: {}".format(survey_unit))
                        print("    ERROR: {}".format(message))

                except Exception as e:
                    print("ERROR: Sanitization failed for {}: {}".format(survey_unit, e))

            # Summary
            print("\n=== SANITIZE SUMMARY ===")
            print("Total survey units: {}".format(len(sanitize_list)))
            print("Already processed: {}".format(already_processed_count))
            print("Successfully processed: {}".format(success_count))
            print("Failed: {}".format(len(sanitize_list) - success_count - already_processed_count))
            
            return success_count > 0

        except Exception as e:
            print("ERROR: Error processing sanitize column: {}".format(e))
            return False

    @staticmethod
    def process_all_columns(codes_path, blocks_gdb, parcels_gdb, gdb_folder, cred=None, count=None):
        """Process all operations: prepare from data.csv, then validate/sanitize/upload on data/gdbs"""
        try:
            print("=== PROCESS ALL COLUMNS ===")
            print("Processing prepare from data.csv, then validate/sanitize/upload on data/gdbs")

            overall_success = True

            # Step 1: Prepare
            print("\n" + "="*50)
            print("STEP 1: PREPARE")
            print("="*50)
            prepare_success = DataWorkflows.process_prepare_column(
                codes_path, blocks_gdb, parcels_gdb, gdb_folder, count
            )
            overall_success = overall_success and prepare_success

            if not prepare_success:
                print_error("Prepare step failed, stopping workflow")
                return False

            # Step 2: Validate
            print("\n" + "="*50)
            print("STEP 2: VALIDATE")
            print("="*50)
            validate_success = DataWorkflows.process_validate_column(codes_path, gdb_folder, count)
            overall_success = overall_success and validate_success

            if not validate_success:
                print_error("Validate step failed, stopping workflow")
                return False

            # Step 3: Sanitize
            print("\n" + "="*50)
            print("STEP 3: SANITIZE")
            print("="*50)
            sanitize_success = DataWorkflows.process_sanitize_column(gdb_folder, count)
            overall_success = overall_success and sanitize_success

            # Step 4: Upload
            print("\n" + "="*50)
            print("STEP 4: UPLOAD")
            print("="*50)
            upload_success = DataWorkflows.process_upload_column(
                codes_path, gdb_folder, cred, count
            )
            overall_success = overall_success and upload_success

            # Final summary
            print("\n" + "="*50)
            print("FINAL SUMMARY")
            print("="*50)
            if overall_success:
                print_essential_success("All steps completed successfully!")
            else:
                print_error("One or more steps failed")

            return overall_success

        except Exception as e:
            print_error("Error processing all columns: {}".format(e))
            return False

    @staticmethod
    def process_single_gdb(codes_path, gdb_file, output_folder='gdbs'):
        """Prepare a single GDB file for upload"""
        try:
            print("=== PROCESS SINGLE GDB ===")
            print("Preparing single GDB file")

            # Validate inputs
            if not os.path.exists(codes_path):
                print_error("Codes file not found: {}".format(codes_path))
                return False

            if not os.path.exists(gdb_file):
                print_error("GDB file not found: {}".format(gdb_file))
                return False

            # Create output folder
            FileOps.ensure_dir_exists(output_folder)

            # Parse hierarchical data
            from src.data import DataProc
            hierarchical_data = DataProc.parse_codes_csv(codes_path)
            if not hierarchical_data:
                print_error("No data found in codes file")
                return False

            # Extract survey unit code from filename
            file_name = os.path.basename(gdb_file)
            if file_name.endswith('.gdb'):
                survey_unit_code = file_name[:-4]  # Remove .gdb
            elif file_name.endswith('.gdb.zip'):
                survey_unit_code = file_name[:-8]  # Remove .gdb.zip
            else:
                print_error("Invalid GDB file format: {}".format(file_name))
                return False

            print("Survey unit code: {}".format(survey_unit_code))

            # Find survey data
            survey_data = DataProc.find_survey_unit_info(hierarchical_data, survey_unit_code)
            if not survey_data:
                print_error("Survey unit code not found in codes file: {}".format(survey_unit_code))
                return False

            # Process with ArcPy
            if ArcCore and ArcCore.is_available():
                return DataWorkflows._process_single_gdb_with_arcpy(
                    gdb_file, output_folder, survey_data, survey_unit_code
                )
            else:
                print_error("ArcPy not available for GDB processing")
                return False

        except Exception as e:
            print_error("Error processing single GDB: {}".format(e))
            return False

    @staticmethod
    def _process_single_gdb_with_arcpy(gdb_file, output_folder, survey_data, survey_unit_code):
        """Process single GDB using ArcPy"""
        try:
            print("Processing GDB with ArcPy...")

            # Handle zip files
            if gdb_file.endswith('.gdb.zip'):
                gdb_path = DataWorkflows._extract_gdb_zip(gdb_file)
                if not gdb_path:
                    return False
            else:
                gdb_path = gdb_file

            # Set ArcPy environment
            ArcCore.set_arcpy_environment(gdb_path)

            # Check for PROPERTY_PARCEL
            fc_path = os.path.join(gdb_path, "PROPERTY_PARCEL")
            if not arcpy.Exists(fc_path):
                print_error("PROPERTY_PARCEL feature class not found in GDB")
                return False

            # Create output GDB
            output_gdb = ArcCore.create_gdb(output_folder, survey_unit_code)
            if not output_gdb:
                return False

            # Copy feature class
            output_fc = ArcCore.create_feature_class(output_gdb, "PROPERTY_PARCEL", "POLYGON")
            if not output_fc:
                return False

            arcpy.CopyFeatures_management(fc_path, output_fc)

            # Create required fields
            ArcCore.create_parcel_fields(output_gdb, "PROPERTY_PARCEL")

            # Populate attributes
            DataWorkflows._populate_parcel_attributes(output_fc, survey_data)

            # Convert multipart to singlepart
            ArcCore.convert_multipolygon_to_single(output_fc)

            print_essential_success("Successfully processed GDB for survey unit: {}".format(survey_unit_code))
            return True

        except Exception as e:
            print_error("Error in ArcPy processing: {}".format(e))
            return False

    @staticmethod
    def _extract_gdb_zip(zip_path):
        """Extract GDB from zip file"""
        try:
            import zipfile

            zip_dir = os.path.dirname(zip_path)
            zip_name = os.path.basename(zip_path)
            base_name = zip_name[:-8] if zip_name.endswith('.gdb.zip') else os.path.splitext(zip_name)[0]
            gdb_path = os.path.join(zip_dir, base_name + '.gdb')

            if not os.path.exists(gdb_path):
                print("Extracting {} to {}".format(zip_path, gdb_path))
                os.makedirs(gdb_path)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(gdb_path)

            return gdb_path

        except Exception as e:
            print_error("Error extracting GDB zip: {}".format(e))
            return None

    @staticmethod
    def _populate_parcel_attributes(fc_path, survey_data):
        """Populate parcel attributes with survey data"""
        try:
            field_mapping = {
                'state_lgd_cd': survey_data.get('StateCode', ''),
                'dist_lgd_cd': survey_data.get('DistrictCode', ''),
                'ulb_lgd_cd': survey_data.get('UlbCode', ''),
                'ward_lgd_cd': survey_data.get('WardCode', ''),
                'vill_lgd_cd': survey_data.get('WardCode', ''),
                'col_lgd_cd': survey_data.get('UlbCode', ''),
                'survey_unit_id': survey_data.get('SurveyUnitCode', '')
            }

            with arcpy.da.UpdateCursor(fc_path, list(field_mapping.keys())) as cursor:
                for row in cursor:
                    new_row = [field_mapping[field] for field in field_mapping.keys()]
                    cursor.updateRow(new_row)

        except Exception as e:
            print_error("Error populating attributes: {}".format(e))

