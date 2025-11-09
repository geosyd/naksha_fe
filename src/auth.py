#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Authentication handling for Naksha API services
"""

import json
import os


def print_error(msg):
    """Print error message"""
    print("ERROR: {}".format(msg))


class NakAuth:
    """Authentication handler for Naksha API"""

    def __init__(self):
        self.auth_token = None
        self.state_id = None

    def parse_credentials(self, cred_string=None):
        """Parse credentials from string or cred.json file"""
        if cred_string:
            parts = cred_string.split(':', 1)
            if len(parts) == 2:
                return parts[0], parts[1], None  # username, password, state_id

            parts = cred_string.split(':')
            if len(parts) >= 3:
                return parts[0], parts[1], parts[2]  # username, password, state_id

        # Try to read from cred.json file
        try:
            cred_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'cred.json')
            if os.path.exists(cred_path):
                with open(cred_path, 'r') as f:
                    cred_data = json.load(f)
                    # Hardcode state_id as 33
                    return cred_data.get('user'), cred_data.get('pass'), "33"
        except Exception as e:
            print_error("Error reading credentials: {}".format(e))

        return None, None, None

    def login(self, session, base_url, state_id, username, password):
        """Login to Naksha API and get authentication token"""
        try:
            login_url = base_url + "/NakshaPortalAPI/api/Auth/LogIn"

            payload = {
                "loginID": username,
                "password": password,
                "stateID": str(state_id),
                "userTypeID": 3,
                "source": "desktop"
            }

            headers = {'Content-Type': 'application/json'}
            json_data = json.dumps(payload)

            response = session.post(login_url, data=json_data, headers=headers, timeout=30)

            if response.status_code == 200:
                try:
                    response_data = response.json()
                except AttributeError:
                    response_data = json.loads(response.content)

                if response_data.get('status') and response_data.get('data'):
                    self.auth_token = response_data['data'].get('token')
                    self.state_id = state_id
                    if self.auth_token:
                        session.headers.update({
                            'Authorization': 'Bearer {}'.format(self.auth_token)
                        })
                        return True

            print_error("Login failed: {}".format(response.text))
            return False

        except Exception as e:
            print_error("Login error: {}".format(e))
            return False

    def is_authenticated(self):
        """Check if client is authenticated"""
        return self.auth_token is not None

    def logout(self, session):
        """Remove authentication token from session"""
        if self.auth_token and 'Authorization' in session.headers:
            del session.headers['Authorization']
        self.auth_token = None
        self.state_id = None