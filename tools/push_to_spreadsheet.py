import hydra
from omegaconf import DictConfig
import csv
import pickle
import os.path

from googleapiclient.discovery import build
from googleapiclient.discovery_cache.base import Cache
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

#class MemoryCache(Cache):
class MemoryCache(Cache):
    # https://github.com/googleapis/google-api-python-client/issues/325#issuecomment-274349841
    _CACHE = {}
    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content

def _google_auth():
    creds = None
    if os.path.exists('creds/token.pickle'):
        with open('creds/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'creds/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('creds/token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

@hydra.main(config_name='config')
def main(cfg: DictConfig) -> None:
    creds = _google_auth()
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
    print("Result: ")
    print(res)

if __name__ == '__main__':
    main()
