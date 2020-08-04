import logging
from omegaconf import DictConfig
import hydra

from gdocs import GDocsWrapper


@hydra.main(config_name='config')
def main(cfg: DictConfig) -> None:
    gw = GDocsWrapper(cfg.creds)
    service = gw.get_sheets()

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
            }}],
        "includeSpreadsheetInResponse": False,
        "responseRanges": [],
        "responseIncludeGridData": False
    }

    # Call the Sheets API
    res = service.spreadsheets().batchUpdate(spreadsheetId=cfg.push.spreadsheet_id, body=body).execute()
    logging.info("Pushed to spreadsheet. Result: {}".format(res))


if __name__ == '__main__':
    main()
