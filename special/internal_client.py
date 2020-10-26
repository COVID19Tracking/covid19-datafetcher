import json
import urllib.request
import urllib.parse


def build_edit_request(data, batch_note = None, data_entry_type = None, force_state=True, username=None):
    if not data_entry_type:
        data_entry_type = "edit"

    states = set([x['state'] for x in data])
    states = sorted(list(states))
    if not batch_note:
        rows = len(data)
        batch_note = "Updating {} rows for {}".format(rows, ", ".join(states))

    request_data = {
        "context": {
            "dataEntryType": data_entry_type,
            "batchNote": batch_note,
        },
        "coreData": data
    }

    if username:
        request_data['context']['shiftLead'] = username

    if force_state:
        assert len(states) == 1
        request_data['context']['state'] = states[0]
    return request_data


def api_call(data, url, token=None, staging=True):
    if staging:
        assert 'stage' in url or 'localhost' in url

    headers = {'Content-type': 'application/json',
               'Authorization': 'Bearer {}'.format(token)}

    #data = urllib.parse.urlencode(data).encode()
    data = json.dumps(data).encode('utf-8')

    req = urllib.request.Request(url, data, headers)
    with urllib.request.urlopen(req) as response:
        resp = response.read()
        # TODO: do something with the response, like, check code and such
        print("Response:")
        print(resp)
