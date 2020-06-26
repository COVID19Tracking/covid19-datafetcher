from omegaconf import DictConfig
import csv
import hydra
import logging
import os.path
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache
from oauth2client.service_account import ServiceAccountCredentials


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


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

def _google_auth(creds_cfg):
    if creds_cfg.type == "user":
        return _google_user_auth(
            creds_cfg.interactive, creds_cfg.key_filepath, creds_cfg.pickled_token)
    elif creds_cfg.type == "service":
        return _google_service_auth(creds_cfg.key_filepath)
    else:
        logging.error("Unknown auth type {}".format(creds_cfg.type))
    return None

@hydra.main(config_name='config')
def main(cfg: DictConfig) -> None:
    creds = _google_auth(cfg.creds)
    service = build('sheets', 'v4', credentials=creds, cache=MemoryCache())

    content = ""
    with open(cfg.push.file, 'r') as csv_file:
        content = csv_file.read()

    # Build ugly request
    body = {
        'requests': [{
            'pasteData': {
                "coordinate": {
                    "sheetId": cfg.push.sheet_id,
                    "rowIndex": 0,
                    "columnIndex": 0
                },
                "data": content,
                "type": 'PASTE_NORMAL',
                "delimiter": ','
            }}
        ],
        "includeSpreadsheetInResponse": False,
        "responseRanges": [ ],
        "responseIncludeGridData": False
    }

    # Call the Sheets API
    sheet = service.spreadsheets()
    res = service.spreadsheets().batchUpdate(spreadsheetId=cfg.push.spreadsheet_id, body=body).execute()

    logging.info("Pushed to spreadsheet. Result: {}".format(res))

if __name__ == '__main__':
    main()
