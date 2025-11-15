#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Debug utilities for upload command
Saves request data and compares with proxy logs
"""

import os
import json
import datetime
from src.log import log_info, log_success, log_error

class DebugUploader:
    """Debug utility for upload command"""

    @staticmethod
    def save_request_to_txt(gdb_path, payload, response, survey_unit_code):
        """Save upload request and response to txt file in gdbs folder"""
        try:
            # Generate timestamp for filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

            # Create debug filename
            debug_filename = "{}_debug_{}.txt".format(survey_unit_code, timestamp)
            debug_path = os.path.join(os.path.dirname(gdb_path), debug_filename)

            # Create debug content
            debug_content = []
            debug_content.append("=" * 60)
            debug_content.append("DEBUG REQUEST FILE - {}".format(survey_unit_code))
            debug_content.append("Generated: {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            debug_content.append("=" * 60)
            debug_content.append("")

            # Add request details
            debug_content.append("REQUEST:")
            debug_content.append("GDB Path: {}".format(gdb_path))
            debug_content.append("Survey Unit: {}".format(survey_unit_code))
            debug_content.append("")

            # Add JSON payload as single line string
            debug_content.append("JSON PAYLOAD:")
            debug_content.append("-" * 40)
            debug_content.append(json.dumps(payload, separators=(',', ':')))
            debug_content.append("")

            # Add response details
            debug_content.append("RESPONSE:")
            debug_content.append("-" * 40)
            if hasattr(response, 'status_code'):
                debug_content.append("Status Code: {}".format(response.status_code))
                debug_content.append("Headers: {}".format(dict(response.headers)))
                debug_content.append("")

                try:
                    response_text = response.text
                    debug_content.append("Response Body:")
                    debug_content.append(response_text)
                except:
                    debug_content.append("Could not read response body")
            else:
                debug_content.append("Response: {}".format(str(response)))

            debug_content.append("")
            debug_content.append("=" * 60)
            debug_content.append("END DEBUG FILE")
            debug_content.append("")

            # Write debug file
            with open(debug_path, 'w') as f:
                f.write('\n'.join(debug_content))

            log_success("DEBUG: Saved request to {}".format(debug_path))
            print("DEBUG: Saved request to {}".format(debug_path))

            return debug_path

        except Exception as e:
            log_error("Failed to save debug request: {}".format(e))
            print("ERROR: Failed to save debug request: {}".format(e))
            return None

    @staticmethod
    def compare_with_proxy_logs(payload, proxy_logs_path=None):
        """Compare JSON payload with proxy logs"""
        try:
            if not proxy_logs_path:
                # Default proxy logs path
                proxy_logs_path = os.path.join(os.path.dirname(__file__), '..', '..', 'proxy_logs')

            # Check if proxy_logs folder exists
            if not os.path.exists(proxy_logs_path):
                log_info("Proxy logs folder not found: {}".format(proxy_logs_path))
                return False

            # Find proxy log files
            proxy_files = [f for f in os.listdir(proxy_logs_path) if f.startswith('req_') and f.endswith('.txt')]
            if not proxy_files:
                log_info("No proxy log files found in {}".format(proxy_logs_path))
                return False

            # Get latest proxy log file
            latest_proxy_file = max(proxy_files)
            latest_proxy_path = os.path.join(proxy_logs_path, latest_proxy_file)

            # Read latest proxy log
            with open(latest_proxy_path, 'r') as f:
                proxy_content = f.read()

            # Extract JSON from proxy log
            json_start = proxy_content.find('{"villageCode"')
            if json_start == -1:
                log_error("No JSON found in proxy log: {}".format(latest_proxy_path))
                return False

            proxy_json = proxy_content[json_start:]
            try:
                proxy_data = json.loads(proxy_json)
            except json.JSONDecodeError as e:
                log_error("Failed to parse proxy JSON: {}".format(e))
                return False

            # Compare key fields
            comparison_results = []

            # Compare villageCode
            our_village_code = payload.get('villageCode')
            proxy_village_code = proxy_data.get('villageCode')
            if our_village_code == proxy_village_code:
                comparison_results.append("MATCH villageCode matches: {}".format(our_village_code))
            else:
                comparison_results.append("MISMATCH villageCode mismatch: ours={}, proxy={}".format(our_village_code, proxy_village_code))

            # Compare userid
            our_userid = payload.get('userid')
            proxy_userid = proxy_data.get('userid')
            if our_userid == proxy_userid:
                comparison_results.append("MATCH userid matches: {}".format(our_userid))
            else:
                comparison_results.append("MISMATCH userid mismatch: ours={}, proxy={}".format(our_userid, proxy_userid))

            # Compare survey_unit_id
            our_survey_unit = payload.get('survey_unit_id')
            proxy_survey_unit = proxy_data.get('survey_unit_id')
            if our_survey_unit == proxy_survey_unit:
                comparison_results.append("MATCH survey_unit_id matches: {}".format(our_survey_unit))
            else:
                comparison_results.append("MISMATCH survey_unit_id mismatch: ours={}, proxy={}".format(our_survey_unit, proxy_survey_unit))

            # Compare plot count
            our_plot_count = len(payload.get('plots', []))
            proxy_plot_count = len(proxy_data.get('plots', []))
            if our_plot_count == proxy_plot_count:
                comparison_results.append("MATCH plot count matches: {}".format(our_plot_count))
            else:
                comparison_results.append("MISMATCH plot count mismatch: ours={}, proxy={}".format(our_plot_count, proxy_plot_count))

            # Compare utm_zone
            our_utm_zone = payload.get('utm_zone')
            proxy_utm_zone = proxy_data.get('utm_zone')
            if our_utm_zone == proxy_utm_zone:
                comparison_results.append("MATCH utm_zone matches: {}".format(our_utm_zone))
            else:
                comparison_results.append("MISMATCH utm_zone mismatch: ours={}, proxy={}".format(our_utm_zone, proxy_utm_zone))

            # Compare extent (first few characters)
            our_extent = payload.get('extent', '')
            proxy_extent = proxy_data.get('extent', '')
            if our_extent and proxy_extent:
                if our_extent[:20] == proxy_extent[:20]:
                    comparison_results.append("MATCH extent matches (first 20 chars): {}".format(our_extent[:20]))
                else:
                    comparison_results.append("MISMATCH extent mismatch: ours={}, proxy={}".format(our_extent[:20], proxy_extent[:20]))

            # Print comparison results
            print("DEBUG: Comparison with proxy log ({})".format(latest_proxy_file))
            print("DEBUG: " + "-" * 50)
            for result in comparison_results:
                print("DEBUG: " + result)
            print("DEBUG: " + "-" * 50)

            # Count matches
            matches = sum(1 for result in comparison_results if result.startswith("MATCH"))
            total_checks = len(comparison_results)

            if matches == total_checks:
                log_success("DEBUG: All {} fields match with proxy logs!".format(total_checks))
                print("DEBUG: SUCCESS All {} fields match with proxy logs!".format(total_checks))
            else:
                log_info("DEBUG: {}/{} fields match with proxy logs".format(matches, total_checks))
                print("DEBUG: {}/{} fields match with proxy logs".format(matches, total_checks))

            return matches == total_checks

        except Exception as e:
            log_error("Failed to compare with proxy logs: {}".format(e))
            print("ERROR: Failed to compare with proxy logs: {}".format(e))
            return False

    @staticmethod
    def analyze_payload_structure(payload):
        """Analyze and print payload structure"""
        try:
            print("DEBUG: Payload Structure Analysis")
            print("DEBUG: " + "=" * 40)

            # Top-level fields
            print("DEBUG: Top-level fields:")
            for key in sorted(payload.keys()):
                if key != 'plots':
                    value = payload[key]
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."
                    print("DEBUG:   {}: {}".format(key, value))

            # Plot analysis
            plots = payload.get('plots', [])
            print("DEBUG: Plot count: {}".format(len(plots)))

            if plots:
                first_plot = plots[0]
                print("DEBUG: First plot structure:")
                print("DEBUG:   Attributes: {} fields".format(len(first_plot.get('attributes', {}))))

                geometry = first_plot.get('geometry', {})
                if geometry:
                    rings = geometry.get('rings', [])
                    print("DEBUG:   Geometry rings: {}".format(len(rings)))
                    if rings:
                        print("DEBUG:   First ring vertices: {}".format(len(rings[0])))

            print("DEBUG: " + "=" * 40)

        except Exception as e:
            print("ERROR: Failed to analyze payload structure: {}".format(e))