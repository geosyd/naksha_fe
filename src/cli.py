#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Command line interface and argument parsing for Naksha FE utility
"""

import sys
import os
from src.log import log_info, log_success, log_error, log_warning, log_header


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


class CLI:
    """Command line interface for Naksha FE utility"""

    def __init__(self):
        self.colors = Colors()

    def parse_arguments(self):
        """Parse command line arguments - simplified with hardcoded values"""
        if len(sys.argv) < 2:
            self.show_help()
            sys.exit(0)

        command = sys.argv[1]

        # Create a simple args object with parsed values
        class SimpleArgs:
            def __init__(self, command):
                self.command = command
                self.state = None
                self.clear_gdbs = False
                self.clear_logs = False
                self.force = False
                self.buffer_erase = None
                self.backup_uploaded = None
                self.do_overlap_fix = False

                # Parse flags for different commands
                i = 2
                while i < len(sys.argv):
                    if sys.argv[i] == '--state' and i + 1 < len(sys.argv):
                        self.state = sys.argv[i + 1]
                        i += 2
                    elif sys.argv[i] == '--gdbs':
                        self.clear_gdbs = True
                        i += 1
                    elif sys.argv[i] == '--logs':
                        self.clear_logs = True
                        i += 1
                    elif sys.argv[i] == '--force':
                        self.force = True
                        i += 1
                    elif sys.argv[i] == '--buffer-erase' and i + 1 < len(sys.argv):
                        try:
                            self.buffer_erase = int(sys.argv[i + 1])
                            i += 2
                        except ValueError:
                            print("ERROR: --buffer-erase requires a number (centimeters)")
                            i += 2
                    elif sys.argv[i] == '--backup-uploaded':
                        self.backup_uploaded = True
                        i += 1
                    elif sys.argv[i] == '--do-overlap-fix':
                        self.do_overlap_fix = True
                        i += 1
                    else:
                        i += 1

                # Default behavior for clear command: clear gdbs
                if command == 'clear' and not self.clear_gdbs and not self.clear_logs:
                    self.clear_gdbs = True

        return SimpleArgs(command)

    def show_help(self):
        """Show help information"""
        log_header("Naksha FE Utility - Optimized Version", force_log=True)
        print("=" * 50)
        print
        print("Available Commands:")
        print("  codes          Download hierarchical codes from API")
        print("  stats          Fetch upload status for survey units")
        print("  prepare        Process prepare operations from data/data.csv")
        print("  validate       Validate GDB files in data/gdbs folder")
        print("  upload         Upload GDB files in data/gdbs folder")
        print("  sanitize       Sanitize GDB files in data/gdbs folder")
        print("  clear          Clear GDB files and/or logs")
        print
        print("Options:")
        print("  --state STATE  Specify state for codes command")
        print("  --gdbs         Clear GDB files (default for clear command)")
        print("  --logs         Clear log file (data/log.txt)")
        print("  --force        Force overwrite existing GDB files (prepare command)")
        print("  --buffer-erase N  Use specific buffer distance (N centimeters) for sanitize command")
        print("                   Recommended: 5-30cm for typical parcel data")
        print("  --backup-uploaded  Backup GDBs after successful upload to data/gdbs/backup (upload command)")
        print("  --do-overlap-fix     Perform overlapping pairs fixing in sanitize command")
        print
        print("Logging:")
        print("  All console output is logged to data/log.txt with timestamps")
        print("  New messages appear at the top of the log file")
        print("  Log files accumulate - manually remove to clear history")
        print
        print("Examples:")
        print("  python main.py codes")
        print("  python main.py codes --state \"Tamil Nadu\"")
        print("  python main.py prepare")
        print("  python main.py upload")
        print("  python main.py upload --backup-uploaded  # Backup GDBs after successful upload")
        print("  python main.py sanitize")
        print("  python main.py sanitize --do-overlap-fix     # Perform overlap fixing")
        print("  python main.py sanitize --buffer-erase 20   # Use 20cm buffer distance (recommended)")
        print("  python main.py clear              # Clear GDB files (default)")
        print("  python main.py clear --gdbs       # Clear GDB files")
        print("  python main.py clear --logs       # Clear log file")
        print("  python main.py clear --gdbs --logs # Clear both")

    def _print_msg(self, msg):
        """Print message with logging support"""
        log_info(msg)

    def run_command(self, args):
        """Execute the specified command"""
        # Show version info
        log_info("Naksha FE Utility - Optimized", force_log=True)
        print  # Add a newline for proper spacing

        try:
            if args.command == 'codes':
                success = self._run_codes(args)
            elif args.command == 'stats':
                success = self._run_stats(args)
            elif args.command == 'prepare':
                success = self._run_prepare(args)
            elif args.command == 'validate':
                success = self._run_validate(args)
            elif args.command == 'upload':
                success = self._run_upload(args)
            elif args.command == 'sanitize':
                success = self._run_sanitize(args)
            elif args.command == 'clear':
                success = self._run_clear(args)
            else:
                self.show_help()
                success = False

            # Show result
            if success:
                log_success("Command completed successfully", force_log=True)
            else:
                log_error("Command failed")
            return success

        except Exception as e:
            log_error("Command execution error: {}".format(e), exception=e)
            return False

    def _run_codes(self, args):
        """Run codes command"""
        # Default to Tamil Nadu if no state is specified
        state = args.state if args.state else 'Tamil Nadu'
        log_info("Starting download of hierarchical codes for: {}".format(state), force_log=True)
        print("Downloading hierarchical codes for: {}".format(state))
        from src.api import APIStats
        api_stats = APIStats()
        return api_stats.download_codes(state, None, 'data/codes.csv', None)

    def _run_stats(self, args):
        """Run stats command"""
        log_info("Starting upload status fetch...", force_log=True)
        print("Fetching upload status...")
        from src.api import APIStats
        api_stats = APIStats()

        # Get all survey units directly from API, not limited by local files
        results = api_stats.get_all_upload_status()
        if len(results) > 0:
            # Save results to summary and details CSV files
            summary_success = self._save_summary_to_csv(results, 'data/stats.summary.csv')
            details_success = self._save_details_to_csv(results, 'data/stats.detailed.csv')

            if summary_success and details_success:
                print("Summary stats saved to data/stats.summary.csv")
                print("Detailed survey unit codes saved to data/stats.detailed.csv")
            return summary_success and details_success
        return False

    def _save_status_to_csv(self, survey_units, output_file):
        """Save survey unit status to CSV file"""
        try:
            import os

            # Ensure output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Group by ULB
            ulb_groups = {}
            for unit in survey_units:
                if isinstance(unit, dict):
                    ulb_name = unit.get('ulbName', 'Unknown ULB')
                    ulb_id = unit.get('ulbid', 0)

                    key = (ulb_id, ulb_name)
                    if key not in ulb_groups:
                        ulb_groups[key] = []
                    ulb_groups[key].append(unit)

            # Sort by ULB name
            sorted_ulbs = sorted(ulb_groups.keys(), key=lambda x: x[1])

            with open(output_file, 'w') as f:
                # Write header
                f.write('ulb,survey_units,fe_uploaded\n')

                # Write data for each ULB
                for ulb_id, ulb_name in sorted_ulbs:
                    units = ulb_groups[(ulb_id, ulb_name)]
                    total_units = len(units)
                    uploaded_count = 0

                    # Count uploaded units
                    for unit in units:
                        if isinstance(unit, dict):
                            upload_status = unit.get('is_map_uploaded', '') or ''
                            is_uploaded = upload_status.lower() == 'yes'
                            if is_uploaded:
                                uploaded_count += 1

                    # Escape commas and quotes in ULB name
                    escaped_ulb_name = ulb_name
                    if ',' in ulb_name or '"' in ulb_name:
                        escaped_ulb_name = '"{}"'.format(ulb_name.replace('"', '""'))

                    f.write('{},{},{}\n'.format(escaped_ulb_name, total_units, uploaded_count))

            return True

        except Exception as e:
            log_error("Error saving status CSV: {}".format(e), exception=e)
            return False

    def _save_summary_to_csv(self, survey_units, output_file):
        """Save summary stats to CSV file"""
        try:
            import os

            # Ensure output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Group by ULB
            ulb_groups = {}
            for unit in survey_units:
                if isinstance(unit, dict):
                    ulb_name = unit.get('ulbName', 'Unknown ULB')
                    ulb_id = unit.get('ulbid', 0)

                    key = (ulb_id, ulb_name)
                    if key not in ulb_groups:
                        ulb_groups[key] = []
                    ulb_groups[key].append(unit)

            # Sort by ULB name
            sorted_ulbs = sorted(ulb_groups.keys(), key=lambda x: x[1])

            with open(output_file, 'w') as f:
                # Write header
                f.write('ulb,survey_units,fe_uploaded\n')

                # Write data for each ULB
                for ulb_id, ulb_name in sorted_ulbs:
                    units = ulb_groups[(ulb_id, ulb_name)]
                    total_units = len(units)
                    uploaded_count = 0

                    # Count uploaded units
                    for unit in units:
                        if isinstance(unit, dict):
                            upload_status = unit.get('is_map_uploaded', '') or ''
                            is_uploaded = upload_status.lower() == 'yes'
                            if is_uploaded:
                                uploaded_count += 1

                    # Escape commas and quotes in ULB name
                    escaped_ulb_name = ulb_name
                    if ',' in ulb_name or '"' in ulb_name:
                        escaped_ulb_name = '"{}"'.format(ulb_name.replace('"', '""'))

                    f.write('{},{},{}\n'.format(escaped_ulb_name, total_units, uploaded_count))

            return True

        except Exception as e:
            print("ERROR: {}".format(e))
            return False

    def _save_details_to_csv(self, survey_units, output_file):
        """Save detailed survey unit information to CSV file with one row per survey unit"""
        try:
            import os
            from datetime import datetime

            # Ensure output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Load hierarchical data from codes.csv to get district and ward information
            from src.data import DataProc
            codes_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'codes.csv')
            hierarchical_data = DataProc.parse_codes_csv(codes_path)

            # Create a mapping from survey unit ID to hierarchical data
            survey_unit_mapping = {}
            for data in hierarchical_data:
                survey_unit_id = str(data.get('SurveyUnitCode', ''))  # Ensure string type
                if survey_unit_id:
                    survey_unit_mapping[survey_unit_id] = data

            with open(output_file, 'w') as f:
                # Write header with reorganized column order
                f.write('state,state_code,district,district_code,ulb,ulb_id,ward_name,ward_id,survey_unit_name,survey_unit_id,status,upload_date,ori_upload_date,timestamp\n')

                timestamp = datetime.now().isoformat()

                # Create enhanced survey unit list with hierarchical information for proper sorting
                enhanced_units = []
                for unit in survey_units:
                    if not isinstance(unit, dict):
                        continue

                    # Extract basic information
                    ulb_name = unit.get('ulbName', '')
                    ulb_id = unit.get('ulbid', '')
                    ward_id = unit.get('wardID', '')
                    ward_name = unit.get('wardName', '')
                    survey_unit_id = unit.get('survey_unit_id', '')
                    survey_unit_name = unit.get('survey_unit', '')

                    # Determine status
                    upload_status = unit.get('is_map_uploaded', '') or ''
                    is_uploaded = upload_status.lower() == 'yes'
                    status = 'completed' if is_uploaded else 'pending'

                    # Get district and state information from hierarchical data
                    survey_unit_id_str = str(survey_unit_id)
                    state = district = state_code = district_code = ''
                    if survey_unit_id_str in survey_unit_mapping:
                        hierarchical_info = survey_unit_mapping[survey_unit_id_str]
                        state = hierarchical_info.get('State', '')
                        state_code = hierarchical_info.get('StateCode', '')
                        district = hierarchical_info.get('District', '')
                        district_code = hierarchical_info.get('DistrictCode', '')

                    # Add to enhanced list with all information
                    enhanced_units.append({
                        'state': state,
                        'state_code': state_code,
                        'district': district,
                        'district_code': district_code,
                        'ulb_name': ulb_name,
                        'ulb_id': ulb_id,
                        'ward_id': ward_id,
                        'ward_name': ward_name,
                        'survey_unit_name': survey_unit_name,
                        'survey_unit_id': survey_unit_id,
                        'status': status,
                        'upload_date': unit.get('map_uploaded_on', ''),
                        'ori_upload_date': unit.get('orI_Upload_Date', '')
                    })

                # Sort by: state -> district -> status (pending first, completed last) -> ulb -> ward -> survey unit name
                sorted_units = sorted(enhanced_units, key=lambda x: (
                    x['state'] or '',
                    x['district'] or '',
                    0 if x['status'] == 'pending' else 1,  # pending (0) comes before completed (1)
                    x['ulb_name'] or '',
                    x['ward_name'] or '',
                    x['survey_unit_name'] or ''
                ))

                # Escape commas and quotes in text fields
                def escape_field(field):
                    if field is None:
                        return ''
                    field_str = str(field)
                    if ',' in field_str or '"' in field_str:
                        return '"{}"'.format(field_str.replace('"', '""'))
                    return field_str

                for unit in sorted_units:
                    # Write one row per survey unit with new column order (ward_name before ward_id)
                    f.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(
                        escape_field(unit['state']),
                        escape_field(unit['state_code']),
                        escape_field(unit['district']),
                        escape_field(unit['district_code']),
                        escape_field(unit['ulb_name']),
                        escape_field(unit['ulb_id']),
                        escape_field(unit['ward_name']),
                        escape_field(unit['ward_id']),
                        escape_field(unit['survey_unit_name']),
                        escape_field(unit['survey_unit_id']),
                        escape_field(unit['status']),
                        escape_field(unit['upload_date']),
                        escape_field(unit['ori_upload_date']),
                        escape_field(timestamp)
                    ))

            return True

        except Exception as e:
            print("ERROR: {}".format(e))
            return False

    def _run_prepare(self, args):
        """Run prepare command"""
        import os

        # Check ArcPy availability first
        try:
            from src.core import ArcCore
            if not ArcCore.is_available():
                log_error("ArcPy is not available. The prepare command requires ArcGIS/ArcPy to create geodatabases.")
                print("Please run 'env.bat' to set up the correct ArcGIS Python environment.")
                print("Expected Python: C:\\Python27\\ArcGIS10.7\\python.exe")
                return False
        except ImportError:
            log_error("ArcCore module not found. Please ensure ArcGIS libraries are properly installed.")
            return False

        # Check if data.csv exists
        data_csv_path = 'data/data.csv'
        if not os.path.exists(data_csv_path):
            log_error("data/data.csv file not found. Please ensure the survey unit codes file exists before running prepare command.")
            return False

        log_info("Starting survey unit codes processing...", force_log=True)
        print("Processing survey unit codes...")
        from src.proc import DataWorkflows
        return DataWorkflows.process_prepare_column(
            'data/codes.csv', 'data/nblocks.gdb', 'data/nparcels.gdb', 'data/gdbs', None, force=args.force
        )

    def _run_validate(self, args):
        """Run validate command"""
        import os

        # Check ArcPy availability first
        try:
            from src.core import ArcCore
            if not ArcCore.is_available():
                log_error("ArcPy is not available. The validate command requires ArcGIS/ArcPy to validate geodatabases.")
                print("Please run 'env.bat' to set up the correct ArcGIS Python environment.")
                print("Expected Python: C:\\Python27\\ArcGIS10.7\\python.exe")
                return False
        except ImportError:
            log_error("ArcCore module not found. Please ensure ArcGIS libraries are properly installed.")
            return False

        # Check if gdbs folder exists
        gdbs_path = 'data/gdbs'
        if not os.path.exists(gdbs_path):
            log_error("data/gdbs folder not found. Please run prepare command first to create GDB files.")
            return False

        print("Validating GDB files...")
        from src.proc import DataWorkflows
        return DataWorkflows.process_validate_column('data/codes.csv', 'data/gdbs', None)

    def _run_upload(self, args):
        """Run upload command"""
        import os

        # Check ArcPy availability first
        try:
            from src.core import ArcCore
            if not ArcCore.is_available():
                log_error("ArcPy is not available. The upload command requires ArcGIS/ArcPy to process geodatabases before upload.")
                print("Please run 'env.bat' to set up the correct ArcGIS Python environment.")
                print("Expected Python: C:\\Python27\\ArcGIS10.7\\python.exe")
                return False
        except ImportError:
            log_error("ArcCore module not found. Please ensure ArcGIS libraries are properly installed.")
            return False

        # Check if gdbs folder exists
        gdbs_path = 'data/gdbs'
        if not os.path.exists(gdbs_path):
            log_error("data/gdbs folder not found. Please run prepare command first to create GDB files.")
            return False

        # Check if credentials file exists
        cred_file = 'data/cred.json'
        if not os.path.exists(cred_file):
            print("ERROR: {} file not found. Please ensure the credentials file exists before running upload command.".format(cred_file))
            return False

        print("Uploading GDB files...")
        from src.proc import DataWorkflows
        return DataWorkflows.process_upload_column('data/codes.csv', 'data/gdbs', cred_file, None, backup_uploaded=args.backup_uploaded)

    def _run_sanitize(self, args):
        """Run sanitize command - sanitize all GDB files in data/gdbs folder"""
        import os
        import glob

        print("Sanitizing GDB files...")

        # Check if GDB folder exists
        gdbs_folder = 'data/gdbs'
        if not os.path.exists(gdbs_folder):
            print("ERROR: GDB folder not found at: {}".format(gdbs_folder))
            print("Available options:")
            print("  1. Run prepare command first to create GDBs:")
            print("     python main.py prepare")
            print("  2. Ensure GDB files exist in the gdbs folder")
            return False

        # Check if GDB files exist in the folder
        gdb_files = glob.glob(os.path.join(gdbs_folder, '*.gdb'))
        if not gdb_files:
            print("ERROR: No GDB files found in {}".format(gdbs_folder))
            print("Available options:")
            print("  1. Run prepare command first to create GDBs:")
            print("     python main.py prepare")
            print("  2. Ensure GDB files are properly created")
            print("Found GDB files: {}".format(len(gdb_files)))
            return False

        print("Found {} GDB files to sanitize".format(len(gdb_files)))

        # Process sanitize column
        from src.proc import DataWorkflows
        return DataWorkflows.process_sanitize_column(gdbs_folder, None, buffer_erase_cm=args.buffer_erase, do_overlap_fix=args.do_overlap_fix)

    def _run_clear(self, args):
        """Run clear command with support for --gdbs and --logs flags"""
        import os
        import shutil
        import glob

        success = True
        cleared_anything = False

        # Clear GDB files if requested (default behavior)
        if args.clear_gdbs:
            print
            print("Clearing GDB files...")

            gdbs_path = 'data/gdbs'
            if os.path.exists(gdbs_path):
                try:
                    # Count GDB files before deletion
                    gdb_files = glob.glob(os.path.join(gdbs_path, '*.gdb'))
                    gdb_count = len(gdb_files)

                    if gdb_count > 0:
                        # Remove all GDB files
                        for gdb_file in gdb_files:
                            try:
                                shutil.rmtree(gdb_file)
                                print("Removed: {}".format(os.path.basename(gdb_file)))
                            except Exception as e:
                                print("ERROR removing {}: {}".format(os.path.basename(gdb_file), e))
                                success = False

                        if success:
                            print("Successfully removed {} GDB files".format(gdb_count))
                            cleared_anything = True
                        else:
                            print("ERROR: Some GDB files could not be removed")
                    else:
                        print("No GDB files found to clear")
                        cleared_anything = True

                except Exception as e:
                    print("ERROR clearing GDBs folder: {}".format(e))
                    success = False
            else:
                print("GDBs folder does not exist: {}".format(gdbs_path))
                cleared_anything = True

        # Clear log files if requested
        if args.clear_logs:
            if args.clear_gdbs:  # Add newline if both operations are performed
                print
            print("Clearing log file...")

            log_file = 'data/log.txt'
            if os.path.exists(log_file):
                try:
                    os.remove(log_file)
                    print("Removed: {}".format(log_file))
                    cleared_anything = True
                except Exception as e:
                    print("ERROR removing {}: {}".format(log_file, e))
                    success = False
            else:
                print("Log file does not exist: {}".format(log_file))
                cleared_anything = True

        if not cleared_anything:
            print("WARNING: Nothing to clear - no files found")
            return False

        return success

