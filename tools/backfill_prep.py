from omegaconf import DictConfig
import hydra
import pandas as pd

from gdocs import GDocsWrapper

'''
Running this file:
python tools/backfill_prep.py backfill.states=[CO] creds.type=user
python tools/backfill_prep.py creds.type=service

'''


def get_states_daily_content(gw, file_id):
    '''
    gw: google wrapper
    '''
    values = gw.get_sheet_values(file_id, "'States Daily'!A2:AK")
    if not values:
        return None

    # fix values for DF
    columns = values[0]
    first_row = values[1]
    if len(first_row) < len(columns):
        # add empty cells at the end
        first_row.extend([''] * (len(columns) - len(first_row)))
    df = pd.DataFrame(values[1:], columns=columns)
    return df


def get_timeseries_data(gw, ctp_source_file, backfill_source):
    ctp_df = get_states_daily_content(gw, ctp_source_file)
    backfill_df = pd.read_csv(backfill_source)
    return ctp_df, backfill_df


def _update_or_create_content_sheet(sheets, state_file_id, tab_name, tab_id, content):
    if not state_file_id:
        # Nothing to do
        return None

    # clean and generate the CTP sheet
    content_sheet = None
    response = sheets.spreadsheets().get(spreadsheetId=state_file_id).execute()
    for sheet in response.get('sheets'):
        if sheet.get('properties', {}).get('title') == tab_name:
            content_sheet = sheet.get('properties', {}).get('sheetId')
            break

    if not isinstance(content, str):
        content = content.to_csv(None, index=False)

    body = {
        'requests': [{
            "pasteData": {
                "coordinate": {
                    "sheetId": tab_id,
                    "rowIndex": 0,
                    "columnIndex": 0
                },
                "data": content,
                "type": 'PASTE_NORMAL',
                "delimiter": ','
            }
        }],
        "includeSpreadsheetInResponse": False,
    }
    if not content_sheet:
        body['requests'].insert(
            0,
            {"addSheet": {
                "properties": {
                    "title": tab_name,
                    "index": 2,
                    "sheetId": tab_id,
                }}})

    response = sheets.spreadsheets().batchUpdate(
        spreadsheetId=state_file_id, body=body).execute()
    print("Response for", tab_name, tab_id)
    print(response)


def update_or_create_backfill_sheet(sheets, state_file, content):
    return _update_or_create_content_sheet(sheets, state_file, 'Backfill Data', 666666, content)


def update_or_create_ctp_sheet(sheets, state_file, content):
    return _update_or_create_content_sheet(sheets, state_file, 'CTP Data', 55555, content)


@hydra.main(config_name='config')
def main(cfg: DictConfig) -> None:
    gw = GDocsWrapper(cfg.creds)

    # Load the template and its parent (parent should be "folder" from config)
    drive = gw.get_drive()
    template_file = drive.files().get(
        fileId=cfg.backfill.templateId, fields='id,kind,parents').execute()

    # Find all state files
    result = drive.files().list(
        q='"{}" in parents and not trashed'.format(cfg.backfill.folder),
        pageSize=60, fields="nextPageToken, files(id, name)").execute()
    files = result.get('files', [])
    files = {x['name']: x['id'] for x in files}

    # read comparison content
    sheets = gw.get_sheets()
    states_daily, backfill_data = get_timeseries_data(
        gw, cfg.backfill.states_daily_source, cfg.backfill.backfill_source)

    for state in cfg.backfill.states:
        print("Running ", state)
        state_file = files.get(state)
        if not state_file:
            file_res = drive.files().copy(
                fileId=template_file['id'],
                body={"name": state, "parents": template_file['parents']}).execute()
            state_file = file_res.get('id')

        update_or_create_ctp_sheet(sheets, state_file, states_daily[states_daily['State'] == state])
        update_or_create_backfill_sheet(sheets, state_file, backfill_data[backfill_data['STATE'] == state])


if __name__ == '__main__':
    main()
