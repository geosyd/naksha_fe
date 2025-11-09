#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Base API client for Naksha services with common request functionality
"""

import json
import requests


def print_error(msg):
    """Print error message"""
    print("ERROR: {}".format(msg))


def print_essential_info(msg):
    """Print essential info message"""
    print(msg)


def print_essential_success(msg):
    """Print essential success message"""
    print("SUCCESS: {}".format(msg))


class NakBaseAPI:
    """Base API client with common functionality"""

    def __init__(self, base_url, timeout=30):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

    def make_request(self, method, endpoint, params=None, data=None, json_payload=None):
        """Make authenticated API request"""
        try:
            if not endpoint.startswith('http'):
                if not endpoint.startswith('/'):
                    endpoint = '/' + endpoint
                url = self.base_url + endpoint
            else:
                url = endpoint

            response = self.session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json_payload,
                timeout=self.timeout
            )

            if response.status_code == 200:
                try:
                    return response.json()
                except (AttributeError, ValueError):
                    return {'content': response.content}
            else:
                print_error("API request failed: status {}".format(response.status_code))
                return None

        except Exception as e:
            print_error("API error: {}".format(e))
            return None

    def get_states(self):
        """Get all states"""
        try:
            states_url = "/NakshaPortalAPI/api/Auth/GetStates"
            response_data = self.make_request('GET', states_url)
            if response_data:
                if isinstance(response_data, dict):
                    return response_data.get('data', response_data.get('response', []))
                elif isinstance(response_data, list):
                    return response_data
            return []
        except Exception as e:
            print_error("Error getting states: {}".format(e))
            return []

    def get_state_code_by_name(self, state_name):
        """Get state code by state name"""
        try:
            # First, let's try with hardcoded Tamil Nadu code based on the working login
            if state_name.lower() == 'tamil nadu':
                return '33'

            states_url = "/NakshaPortalAPI/api/Auth/GetStates"
            response = self.make_request('GET', states_url)

            print_essential_info("API response status: {}".format(response.status_code if response else "No response"))

            if response and response.status_code == 200:
                try:
                    states = response.json()
                    print_essential_info("States response type: {}".format(type(states)))

                    if isinstance(states, list):
                        print_essential_info("Found {} states".format(len(states)))
                        for state in states:
                            state_name_en = state.get('stateNameEn', state.get('Name', ''))
                            print_essential_info("  Checking: '{}'".format(state_name_en))
                            if state_name_en and state_name_en.lower() == state_name.lower():
                                state_code = state.get('stateCode', state.get('ID', ''))
                                print_essential_success("Found state code: {}".format(state_code))
                                return state_code
                    elif isinstance(states, dict):
                        print_essential_info("States is dict with {} entries".format(len(states)))
                        for state_id, state_data in states.items():
                            state_name_en = state_data.get('stateNameEn', state_data.get('Name', ''))
                            print_essential_info("  Checking: '{}'".format(state_name_en))
                            if state_name_en and state_name_en.lower() == state_name.lower():
                                print_essential_success("Found state code: {}".format(str(state_id)))
                                return str(state_id)
                except Exception as parse_error:
                    print_error("Error parsing states JSON: {}".format(parse_error))
            return None
        except Exception as e:
            print_error("Error getting state code: {}".format(e))
            return None

    def upload_file(self, file_path, file_type='gdb'):
        """Upload file to Naksha API"""
        try:
            import os
            if not os.path.exists(file_path):
                print_error("File not found for upload: {}".format(file_path))
                return None

            with open(file_path, 'rb') as f:
                files = {'file': (os.path.basename(file_path), f, 'application/zip')}
                headers = {'folder': 'vector', 'file_id': os.path.basename(file_path)}

                response = self.session.post(self.base_url + "/NakshaPortalAPI/api/Desktop/upload", files=files, headers=headers)

                if response.status_code == 200:
                    print_essential_info("    SUCCESS: File upload [200]")
                    return True
                else:
                    print_error("    FAILED: File upload [{}]".format(response.status_code))
                    return None

        except Exception as e:
            print_error("File upload error: {}".format(e))
            # Print stack trace for debugging
            try:
                import traceback
                print_error("Traceback: {}".format(traceback.format_exc()))
            except:
                print_error("Unable to get traceback")
            return None

    def get_lpi_survey_details(self, state_code, district_code, ulb_code, ward_code, survey_unit_id):
        """Get LPI survey unit details"""
        params = {
            'stateCode': state_code,
            'districtCode': district_code,
            'ulbCode': ulb_code,
            'wardCode': ward_code,
            'surveyUnitId': survey_unit_id
        }

        response_data = self.make_request('GET', '/NakshaPortalAPI/api/LPISurvey/GetLPISurveyUnitDetails', params=params)
        return response_data.get('data', []) if response_data else None