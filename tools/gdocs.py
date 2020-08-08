import logging
import os.path
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache
from oauth2client.service_account import ServiceAccountCredentials

# Request all these scopes, instead of letting the use pick the relevant ones
# because eventually, in this context, we'll need all
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive.metadata']


class MemoryCache(Cache):
    # https://github.com/googleapis/google-api-python-client/issues/325#issuecomment-274349841
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content


def _google_service_auth(key_filepath):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        key_filepath, scopes=SCOPES)
    return creds


def _google_user_auth(interactive, key_filepath, pickled_token):
    creds = None
    if os.path.exists(pickled_token):
        with open(pickled_token, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Create the flow using the client secrets file from the Google API
            # Console.
            flow = InstalledAppFlow.from_client_secrets_file(key_filepath, SCOPES)
            if interactive:
                creds = flow.run_local_server(port=0)
            else:
                creds = flow.run_console()
        # Save the credentials for the next run
        with open(pickled_token, 'wb') as token:
            pickle.dump(creds, token)
    return creds


class GDocsWrapper(object):

    def __init__(self, creds_cfg):
        self.creds_cfg = creds_cfg
        # init creds and services:
        self.creds = None
        self.sheets = None
        self.drive = None
        self.cache = MemoryCache()

    @staticmethod
    def google_auth(creds_cfg):
        if creds_cfg.type == "user":
            return _google_user_auth(
                creds_cfg.interactive, creds_cfg.key_filepath, creds_cfg.pickled_token)
        if creds_cfg.type == "service":
            return _google_service_auth(creds_cfg.key_filepath)

        logging.error("Unknown auth type {}".format(creds_cfg.type))
        return None

    def get_creds(self):
        if self.creds is None:
            self.creds = GDocsWrapper.google_auth(self.creds_cfg)
        return self.creds

    def get_sheets(self):
        if self.sheets is None:
            self.sheets = build('sheets', 'v4', credentials=self.get_creds(), cache=self.cache)
        return self.sheets

    def get_drive(self):
        if self.drive is None:
            self.drive = build('drive', 'v3', credentials=self.get_creds(), cache=self.cache)
        return self.drive

    def set_sheet_values(self, spreadsheet_id, sheet_id, content):
        # TODO: consider doing it here
        pass

    def get_sheet_values(self, spreadsheet_id, sheet_range):
        if not spreadsheet_id or not sheet_range:
            return None

        response = self.get_sheets().spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=sheet_range).execute()

        values = response.get('values')
        if not values:
            return None

        return values
