#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API communications for Naksha services including hierarchical data fetching and status management
"""

import json
import os
import requests
from datetime import datetime

from src.auth import NakAuth
from src.base import NakBaseAPI


# Configuration constants
API_BASE_URL = "https://nakshauat.dolr.gov.in"
API_TIMEOUT = 30
RESPONSE_SUCCESS_CODE = "S-00"

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

def print_essential_info(msg):
    print(msg)  # Keep as direct print for verbose output

def print_essential_success(msg):
    log_success(msg, force_log=True)


class NakshaUploader:
    """Naksha API uploader class - based on reference implementation"""
    def __init__(self):
        self.base_url = "https://nakshauat.dolr.gov.in"
        self.session = requests.Session()
        self.auth_token = None

    def parse_credentials(self, cred_string):
        """Parse credentials in format username;password"""
        parts = cred_string.split(';', 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    def login(self, state_code, username, password):
        """Login to the Naksha API"""
        try:
            login_url = "{}/NakshaPortalAPI/api/Auth/LogIn".format(self.base_url)

            payload = {
                "loginID": username,
                "password": password,
                "stateID": str(state_code),
                "userTypeID": 3,
                "source": "desktop"
            }

            headers = {
                'Content-Type': 'application/json'
            }

            json_data = json.dumps(payload)

            response = self.session.post(login_url, data=json_data, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                if response_data.get('status') == True and response_data.get('data'):
                    self.auth_token = response_data['data'].get('token')
                    self.session.headers.update({
                        'Authorization': 'Bearer {}'.format(self.auth_token)
                    })
                    return True

            return False

        except Exception as e:
            return False

    def upload_file(self, file_path):
        """Upload the GDB file to the server"""
        try:
            upload_url = "{}/NakshaPortalAPI/api/Desktop/upload".format(self.base_url)

            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/zip')}

                headers = {
                    'folder': 'vector',
                    'file_id': os.path.basename(file_path)
                }

                response = self.session.post(upload_url, files=files, headers=headers)

                if response.status_code == 200:
                    return True
                else:
                    return False

        except Exception as e:
            return False

    def _upload_plot_chunk(self, features, survey_unit_info, file_name, wkid="32644"):
        """Upload a single chunk of plot data"""
        try:
            import uuid
            upload_url = "{}/NakshaPortalAPI/api/Desktop/UploadGDB".format(self.base_url)

            data_version_guid = str(uuid.uuid4())
            survey_unit_code = os.path.splitext(os.path.splitext(file_name)[0])[0]

            # Debug: Log essential upload info (verbose output disabled)
            # print("    DEBUG: === UPLOAD PAYLOAD ANALYSIS ===")
            # print("    DEBUG: survey_unit_info: {}".format(survey_unit_info))
            print("    DEBUG: Processing {} features for upload".format(len(features)))

            payload = {
                "villageCode": survey_unit_info['UlbCode'],
                "utm_zone": wkid,
                "plots": features,
                "userid": "1",
                "shapeFileName": file_name,
                "stateID": survey_unit_info['StateCode'],
                "ulb_lgd_cd": int(survey_unit_info['UlbCode']),
                "survey_unit_id": long(survey_unit_code),
                "dist_lgd_cd": int(survey_unit_info['DistrictCode']),
                "ward_lgd_cd": int(survey_unit_info['WardCode']),
                "extent": "0,0,0,0",
                "plotCount": len(features),
                "data_version_guid": data_version_guid
            }

            # Debug output disabled to reduce console verbosity
            print("    DEBUG: Ready to upload {} plot features".format(len(features)))

            json_payload = json.dumps(payload)
            headers = {'Content-Type': 'application/json'}

            response = self.session.post(upload_url, data=json_payload, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                result = response_data.get('result', {})
                response_code = result.get('responseCode')

                if response_code == 'S-00':
                    print_essential_info("    SUCCESS: Plot data chunk upload [S-00]")
                    return True
                elif response_code == 'E-00':
                    error_msg = result.get('responseMessage') or result.get('message') or 'Server error'
                    print_error("    FAILED: Plot data upload [E-00] - {}".format(error_msg[:50]))
                    return False
                else:
                    print_error("    FAILED: Unknown response code [{}]".format(response_code))
                    return False
            else:
                # Debug: Log server error response
                print("\n" + "="*60)
                print("SERVER ERROR RESPONSE DEBUG:")
                print("HTTP Status Code: {}".format(response.status_code))
                print("Response Headers:")
                for header, value in response.headers.items():
                    print("  {}: {}".format(header, value))
                print("Response Content:")
                try:
                    print(response.content.decode('utf-8'))
                except:
                    print(response.content)
                print("="*60 + "\n")

                print_error("    FAILED: Plot data upload [{}]".format(response.status_code))
                return False

        except Exception as e:
            print_error("Plot data chunk upload error: {}".format(e))
            return False

    def upload_plot_data(self, gdb_data, survey_unit_info, file_name):
        """Upload plot data in chunks"""
        try:
            features = gdb_data.get('features', []) if isinstance(gdb_data, dict) else []

            if not features:
                print_essential_info("No features found - file upload only")
                return True

            wkid = "32644"
            if features and features[0].get('geometry', {}).get('spatialReference', {}).get('wkid'):
                wkid = str(features[0]['geometry']['spatialReference']['wkid'])

            chunk_size = 500
            chunks = [features[i:i + chunk_size] for i in range(0, len(features), chunk_size)]

            print("    Uploading {} chunks of plot data...".format(len(chunks)))

            for i, chunk in enumerate(chunks):
                success = self._upload_plot_chunk(chunk, survey_unit_info, file_name, wkid)
                if not success:
                    print_error("    FAILED: Plot data chunk {}".format(i + 1))
                    return False

            return True

        except Exception as e:
            print_error("Plot data upload error: {}".format(e))
            return False


class NakAPI(NakBaseAPI):
    """Naksha API client with hierarchical data fetching capabilities"""

    def __init__(self):
        NakBaseAPI.__init__(self, API_BASE_URL, API_TIMEOUT)
        self.auth = NakAuth()

    def parse_credentials(self, cred_string=None):
        """Parse credentials using auth module"""
        return self.auth.parse_credentials(cred_string)

    def login(self, state_id, username, password):
        """Login using auth module"""
        return self.auth.login(self.session, self.base_url, state_id, username, password)

    def is_authenticated(self):
        """Check if authenticated using auth module"""
        return self.auth.is_authenticated()

    def get_districts(self, state_code):
        """Get districts for a state"""
        try:
            url = "{}/NakshaPortalAPI/api/Master/GetDistrict/{}".format(self.base_url, state_code)
            response = self.session.get(url)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                if response_data.get('data'):
                    return response_data['data']
            return []
        except Exception as e:
            print_error("Error getting districts: {}".format(e))
            return []

    def get_ulbs(self, state_code, district_code):
        """Get ULBs for a district (requires authentication)"""
        if not self.auth.auth_token:
            return []

        try:
            url = "{}/NakshaPortalAPI/api/MastersMenu/GetAssignedULBList".format(self.base_url)

            payload = {
                "Token": self.auth.auth_token,
                "DistrictID": district_code,
                "StateID": int(state_code),
                "UserID": 1012,
                "LoginUserID": 1012
            }

            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(payload)

            response = self.session.post(url, data=json_data, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                if response_data.get('data'):
                    return response_data['data']
                else:
                    # Match .NET version - check status and provide better feedback
                    status_value = response_data.get('status')
                    if status_value is not None:
                        if str(status_value).lower() == 'true' or str(status_value) == '1':
                            print("ULB status is True but no data field found")
                        else:
                            print("ULB status is False - {}".format(response_data.get('message', 'No message')))
                    else:
                        print("ULB response missing both data and status fields")
            return []
        except Exception as e:
            print_error("Error getting ULBs: {}".format(e))
            return []

    def get_wards(self, state_code, district_code, ulb_code):
        """Get wards for a ULB (requires authentication)"""
        if not self.auth.auth_token:
            return []

        try:
            url = "{}/NakshaPortalAPI/api/MastersMenu/GetAssignedWardList".format(self.base_url)

            payload = {
                "Token": self.auth.auth_token,
                "DistrictID": district_code,
                "StateID": int(state_code),
                "UlbID": ulb_code,
                "UserID": 1012,
                "LoginUserID": 1012
            }

            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(payload)

            response = self.session.post(url, data=json_data, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                if response_data.get('data'):
                    return response_data['data']
            return []
        except Exception as e:
            print_error("Error getting wards: {}".format(e))
            return []

    def get_survey_units(self, ward_code, district_code, state_code, ulb_code):
        """Get survey units for a ward (requires authentication) - matches .NET robust parsing"""
        if not self.auth.auth_token:
            return []

        try:
            url = "{}/NakshaPortalAPI/api/ManageSectorWard/GetSurveyUnitList".format(self.base_url)

            payload = {
                "UserID": 1012,
                "LoginUserID": 1012,
                "StateID": int(state_code),
                "DistrictID": district_code,
                "UlbID": int(ulb_code),
                "WardID": int(ward_code)
            }

            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(payload)

            response = self.session.post(url, data=json_data, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                # Match .NET robust JSON parsing logic
                survey_units = []

                # Check for status in multiple formats (like .NET version)
                status_value = response_data.get('status')
                has_status = (status_value is not None and
                             (str(status_value).lower() == 'true' or str(status_value) == '1'))

                if has_status and response_data.get('data'):
                    data_obj = response_data['data']
                    survey_data = str(data_obj) if data_obj else ''

                    if survey_data.startswith('['):
                        # Parse as array
                        try:
                            survey_units = json.loads(survey_data)
                            if not isinstance(survey_units, list):
                                survey_units = []
                        except:
                            survey_units = []
                    elif survey_data:
                        # Parse as single object
                        try:
                            single_unit = json.loads(survey_data)
                            if single_unit:
                                survey_units = [single_unit]
                        except:
                            survey_units = []
                elif response_data.get('data'):
                    # Alternative parsing path
                    data_obj = response_data['data']
                    survey_data = str(data_obj) if data_obj else ''

                    if survey_data.startswith('['):
                        try:
                            survey_units = json.loads(survey_data)
                            if not isinstance(survey_units, list):
                                survey_units = []
                        except:
                            survey_units = []
                    elif survey_data:
                        try:
                            single_unit = json.loads(survey_data)
                            if single_unit:
                                survey_units = [single_unit]
                        except:
                            survey_units = []

                return survey_units
            else:
                print_error("Survey units request failed with status {}: {}".format(response.status_code, response.text[:200]))
            return []
        except Exception as e:
            print_error("Error getting survey units: {}".format(e))
            return []

    def get_survey_unit_details(self, state_id, district_id, ulb_id, ward_id=0, survey_unit_id=0):
        """Get survey unit details for a ULB"""
        if not self.auth.auth_token:
            return []

        try:
            url = "{}/NakshaPortalAPI/api/ManageSectorWard/GetSurveyUnitDetails".format(self.base_url)

            payload = {
                "userID": 4,  # Hardcoded as per .NET implementation
                "stateID": state_id,
                "districtID": district_id,
                "ulbID": ulb_id,
                "wardID": ward_id,
                "surveyUnitID": survey_unit_id
            }

            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(payload)

            response = self.session.post(url, data=json_data, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                if response_data.get('data'):
                    return response_data['data']

            return []

        except Exception as e:
            print_error("Error fetching survey unit details for ULB {}: {}".format(ulb_id, e))
            return []

    def upload_plot_data(self, gdb_data, survey_unit_info, file_name):
        """Upload plot data in chunks (reference implementation)"""
        try:
            if not self.auth.auth_token:
                print_error("Not authenticated - cannot upload plot data")
                return False

            # Extract features from gdb_data (should be {'features': features})
            features = gdb_data.get('features', []) if isinstance(gdb_data, dict) else []

            if not features:
                print_essential_info("No features found - file upload only")
                return True

            # Determine WKID from spatial reference
            wkid = "32644"
            if features and features[0].get('geometry', {}).get('spatialReference', {}).get('wkid'):
                wkid = str(features[0]['geometry']['spatialReference']['wkid'])

            # Upload in chunks of 500 features (matching reference implementation)
            chunk_size = 500
            chunks = [features[i:i + chunk_size] for i in range(0, len(features), chunk_size)]

            print("    Uploading {} chunks of plot data...".format(len(chunks)))

            for i, chunk in enumerate(chunks):
                success = self._upload_plot_chunk(chunk, survey_unit_info, file_name, wkid)
                if not success:
                    print_error("    FAILED: Plot data chunk {}".format(i + 1))
                    return False

            return True

        except Exception as e:
            print_error("Plot data upload error: {}".format(e))
            return False

    def _upload_plot_chunk(self, chunk, survey_unit_info, file_name, wkid):
        """Upload a single chunk of plot data (reference implementation)"""
        try:
            if not self.auth.auth_token:
                print_error("Not authenticated - cannot upload plot data")
                return False

            import os
            import uuid
            survey_unit_code = os.path.splitext(os.path.splitext(file_name)[0])[0]

            upload_url = "{}/NakshaPortalAPI/api/Desktop/UploadGDB".format(self.base_url)
            data_version_guid = str(uuid.uuid4())

            # Upload data is properly formatted (debug output removed)

            # Prepare the upload data using reference implementation format
            payload = {
                "villageCode": survey_unit_info.get('UlbCode', ''),
                "utm_zone": wkid,
                "plots": chunk,
                "userid": "1",
                "shapeFileName": file_name,
                "stateID": survey_unit_info.get('StateCode', ''),
                "ulb_lgd_cd": int(survey_unit_info.get('UlbCode', '0')),
                "survey_unit_id": long(survey_unit_code),
                "dist_lgd_cd": int(survey_unit_info.get('DistrictCode', '0')),
                "ward_lgd_cd": int(survey_unit_info.get('WardCode', '0')),
                "extent": "0,0,0,0",
                "plotCount": len(chunk),
                "data_version_guid": data_version_guid
            }

            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(payload)

            response = self.session.post(upload_url, data=json_data, headers=headers)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                # Debug: Log complete server response
                print("\n" + "="*60)
                print("UPLOAD RESPONSE DEBUG:")
                print("Complete response data:")
                print(json.dumps(response_data, indent=2, default=str))

                result = response_data.get('result', {})
                print("\nResult section:")
                print(json.dumps(result, indent=2, default=str))

                response_code = result.get('responseCode')
                response_message = result.get('responseMessage', 'No message')
                print("\nResponse code: '{}'".format(response_code))
                print("Response message: '{}'".format(response_message))
                print("="*60 + "\n")

                if response_code == 'S-00':
                    print_essential_info("    SUCCESS: Plot data chunk upload [S-00]")
                    return True
                elif response_code == 'E-00':
                    error_msg = result.get('responseMessage') or result.get('message') or 'Server error'
                    print_error("    FAILED: Plot data upload [E-00] - {}".format(error_msg[:50]))
                    # Note: E-00 might be expected with certain data formats as mentioned in reference
                    # This could be a server-side validation issue rather than a data format issue
                    return False
                else:
                    print_error("    FAILED: Unknown response code [{}]".format(response_code))
                    return False
            else:
                # Debug: Log server error response
                print("\n" + "="*60)
                print("SERVER ERROR RESPONSE DEBUG:")
                print("HTTP Status Code: {}".format(response.status_code))
                print("Response Headers:")
                for header, value in response.headers.items():
                    print("  {}: {}".format(header, value))
                print("Response Content:")
                try:
                    print(response.content.decode('utf-8'))
                except:
                    print(response.content)
                print("="*60 + "\n")

                print_error("    FAILED: Plot data upload [{}]".format(response.status_code))
                return False

        except Exception as e:
            print_error("Plot data chunk upload error: {}".format(e))
            return False


class APIStats:
    """API statistics and status fetching utilities"""

    def __init__(self):
        self.api = NakAPI()

    def download_codes(self, state="Tamil Nadu", cred=None, output='data/codes.csv', count=None):
        """Download hierarchical codes from API"""
        try:
            print_essential_info("Downloading hierarchical codes for state: {}".format(state))

            username, password, state_id = self.api.parse_credentials(cred)
            if not all([username, password, state_id]):
                print_error("Invalid credentials provided")
                return False

            if not self.api.login(state_id, username, password):
                print_error("Authentication failed")
                return False

            # Build hierarchical data by calling multiple API endpoints
            print_essential_info("Building hierarchical data for {}...".format(state))

            # Get state code
            state_code = self.api.get_state_code_by_name(state)
            if not state_code:
                print_error("State '{}' not found in API".format(state))
                return False

            print_essential_success("Found state code: {}".format(state_code))

            # Get districts
            districts = self.api.get_districts(str(state_code))
            if not districts:
                print_error("No districts found or error occurred")
                return False

            print_essential_success("Found {} districts".format(len(districts)))

            if count:
                districts = districts[:count]

            hierarchical_data = []

            for district in districts:
                district_code = district.get('districtCode', '')
                district_name = district.get('districtName', '')

                # Get ULBs for this district
                ulbs = self.api.get_ulbs(str(state_code), district_code)

                if not ulbs:
                    continue

                for ulb in ulbs:
                    ulb_code = (ulb.get('UlbLgdCode', '') or ulb.get('ulblgdcode', '') or
                               ulb.get('ulb_lgd_code', '') or ulb.get('UlbLgdCode', ''))
                    ulb_name = (ulb.get('UlbName', '') or ulb.get('ulbname', '') or
                               ulb.get('ulb_name', '') or ulb.get('UlbName', ''))

                    # Get wards for this ULB
                    wards = self.api.get_wards(str(state_code), district_code, ulb_code)

                    if not wards:
                        continue

                    for ward in wards:
                        ward_code = (ward.get('wardSurveyUnitId', '') or ward.get('WardSurveyUnitId', '') or
                                   ward.get('ID', '') or ward.get('id', '') or '')
                        ward_name = (ward.get('wardSurveyUnitName', '') or ward.get('WardSurveyUnitName', '') or
                                   ward.get('StateNameEn', '') or ward.get('statenameen', '') or '')

                        # Get survey units for this ward
                        if ward_code:
                            survey_units = self.api.get_survey_units(str(ward_code), district_code, str(state_code), ulb_code)

                            if survey_units:
                                for unit in survey_units:
                                    unit_code = (unit.get('survey_unit_id', '') or unit.get('SurveyUnitId', '') or
                                               unit.get('ID', '') or unit.get('id', ''))
                                    unit_name = (unit.get('survey_unit', '') or unit.get('SurveyUnitName', '') or
                                               unit.get('stateNameEn', '') or unit.get('statenameen', '') or '')

                                    if unit_name and unit_code:
                                        unit_code_str = str(unit_code).strip() if unit_code else ''
                                        if len(unit_code_str) > 10:
                                            unit_code_str = unit_code_str[:6]

                                        hierarchical_data.append({
                                            'State': state,
                                            'StateCode': str(state_code),
                                            'District': district_name,
                                            'DistrictCode': district_code,
                                            'Ulb': ulb_name,
                                            'UlbCode': ulb_code,
                                            'Ward': ward_name,
                                            'WardCode': ward_code,
                                            'SurveyUnit': unit_name,
                                            'SurveyUnitCode': unit_code_str
                                        })

            if hierarchical_data:
                return self._save_codes_to_csv(hierarchical_data, output, count)
            else:
                print_error("No hierarchical data could be fetched")
                return False

        except Exception as e:
            print_error("Error downloading codes: {}".format(e))
            return False

    def _save_codes_to_csv(self, hierarchy_data, output, count=None):
        """Save hierarchy data to CSV file"""
        try:
            import csv
            import os

            # Ensure output directory exists
            output_dir = os.path.dirname(output)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Apply count limit if specified
            if count and isinstance(hierarchy_data, list):
                hierarchy_data = hierarchy_data[:count]

            # Write to CSV
            with open(output, 'wb') as csvfile:
                fieldnames = ['State', 'StateCode', 'District', 'DistrictCode', 'Ulb', 'UlbCode', 'Ward', 'WardCode', 'SurveyUnit', 'SurveyUnitCode']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                # Extract and write hierarchy data
                if isinstance(hierarchy_data, list):
                    for item in hierarchy_data:
                        writer.writerow({
                            'State': item.get('State', ''),
                            'StateCode': item.get('StateCode', ''),
                            'District': item.get('District', ''),
                            'DistrictCode': item.get('DistrictCode', ''),
                            'Ulb': item.get('Ulb', ''),
                            'UlbCode': item.get('UlbCode', ''),
                            'Ward': item.get('Ward', ''),
                            'WardCode': item.get('WardCode', ''),
                            'SurveyUnit': item.get('Block', item.get('SurveyUnit', '')),
                            'SurveyUnitCode': item.get('BlockCode', item.get('SurveyUnitCode', ''))
                        })

            print_essential_success("Codes downloaded successfully to: {}".format(output))
            return True

        except Exception as e:
            print_error("Error saving codes to CSV: {}".format(e))
            return False

    def get_all_upload_status(self):
        """Get upload status for all survey units by fetching ULB data"""
        try:
            print_essential_info("Fetching upload status for all survey units")

            username, password, state_id = self.api.parse_credentials(None)
            if not all([username, password, state_id]):
                print_error("Invalid credentials provided")
                return []

            if not self.api.login(state_id, username, password):
                print_error("Authentication failed")
                return []

            # Try to read hierarchical data from codes.csv
            try:
                from src.data import DataProc
                codes_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'codes.csv')
                if not os.path.exists(codes_path):
                    print_error("codes.csv file not found. Please run 'codes' command first.")
                    return []

                hierarchical_data = DataProc.parse_codes_csv(codes_path)
                if not hierarchical_data:
                    print_error("No data found in codes.csv")
                    return []

                print_essential_info("Found {} hierarchical entries from codes.csv".format(len(hierarchical_data)))

            except Exception as e:
                print_error("Error reading codes.csv: {}".format(e))
                return []

            # Group ULBs by state and district
            state_district_ulbs = {}
            for data in hierarchical_data:
                if not data.get('UlbCode'):
                    continue

                state_key = data.get('StateCode')
                district_key = data.get('DistrictCode')
                ulb_code = data.get('UlbCode')
                ulb_name = data.get('Ulb')

                if state_key not in state_district_ulbs:
                    state_district_ulbs[state_key] = {}
                if district_key not in state_district_ulbs[state_key]:
                    state_district_ulbs[state_key][district_key] = []

                # Add ULB only once
                ulb_exists = any(ulb['code'] == ulb_code for ulb in state_district_ulbs[state_key][district_key])
                if not ulb_exists:
                    state_district_ulbs[state_key][district_key].append({
                        'name': ulb_name,
                        'code': ulb_code
                    })

            all_survey_units = []
            ulb_count = 0

            # Process each state
            for state_id, districts in state_district_ulbs.items():
                try:
                    state_id_int = int(state_id)

                    # Process each district
                    for district_id, ulbs in districts.items():
                        try:
                            district_id_int = int(district_id)

                            # Fetch survey units for each ULB
                            for ulb in ulbs:
                                ulb_name = ulb['name']
                                ulb_code = ulb['code']

                                try:
                                    ulb_id = int(ulb_code)
                                    ulb_count += 1

                                    survey_units = self.api.get_survey_unit_details(state_id_int, district_id_int, ulb_id)
                                    all_survey_units.extend(survey_units)

                                    if ulb_count % 10 == 0:
                                        print_essential_info("Processed {} ULBs...".format(ulb_count))

                                except ValueError:
                                    # Invalid ULB code - skip
                                    continue

                        except ValueError:
                            # Invalid district code - skip
                            continue

                except ValueError:
                    # Invalid state code - skip
                    continue

            print_essential_success("Fetched status for {} survey units from {} ULBs".format(len(all_survey_units), ulb_count))
            return all_survey_units

        except Exception as e:
            print_error("Error fetching upload status: {}".format(e))
            return []

    def fetch_upload_status(self, survey_units_codes, codes_path, cred_string=None):
        """Fetch upload status for survey units"""
        try:
            username, password, state_id = self.api.parse_credentials(cred_string)
            if not all([username, password, state_id]):
                print_error("Invalid credentials provided")
                return []

            if not self.api.login(state_id, username, password):
                print_error("Authentication failed")
                return []

            from src.data import DataProc
            hierarchical_data = DataProc.parse_codes_csv(codes_path)
            if not hierarchical_data:
                print_error("No hierarchical data found")
                return []

            results = []
            for code in survey_units_codes:
                survey_info = DataProc.find_survey_unit_info(hierarchical_data, code)
                if survey_info:
                    results.append({
                        'survey_unit_id': code,
                        'status': 'found',
                        'timestamp': datetime.now().isoformat()
                    })

            return results

        except Exception as e:
            print_error("Error fetching upload status: {}".format(e))
            return []

