from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from pdfminer.high_level import extract_text

import urllib
import urllib.request
import shutil
import json
import os
import sys


BASE_URL = 'https://oklahoma.gov/content/dam/ok/en/covid19/documents/weekly-epi-report'
REPORT = '%Y.%m.%d Weekly Epi Report.pdf'
TOTAL_TESTS = 'Specimens tested, count'


def atoi(val):
    '''Raises exception on failure to parse'''
    if isinstance(val, int):
        return val
    return int(val.replace(",", ''))


def get_data(filepath):
    text = extract_text(filepath)
    lines = text.splitlines()
    lines = [x.strip() for x in lines if x.strip()]
    data = {}
    data['date'] = lines[2]
    for i, x in enumerate(lines):
        if x == TOTAL_TESTS:
            # read the next number and abort
            data[x] = atoi(lines[i+7*2])
            break

    return data


def main(base_dir):
    # download report
    # open it
    # parse it
    # store numbers, as json, somewhere

    data = {}
    day = datetime.now()

    # we won't always have a document ready, in this case, try to find a previous one
    # but don't go beyond -8 days, it's a weekly report
    for i in range(8):
        day = datetime.now() - timedelta(days=i)
        report_url = day.strftime(REPORT)
        report_url = urllib.parse.quote(report_url)
        url = BASE_URL + "/" + report_url
        print(url)
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req) as response, NamedTemporaryFile(delete=True) as tmpfile:
                shutil.copyfileobj(response, tmpfile)
                tmpfile.flush()
                data = get_data(tmpfile.name)
                print(data)

                if data:
                    json.dump(data, open(os.path.join(base_dir, "ok_pcr_specimens.json"), 'w'))

                break
        except urllib.error.HTTPError as e:
            # if it's 404 -- it's ok
            print(day, e.getcode())
            if e.getcode() != 404:
                print(str(e))


if __name__ == "__main__":
    # where to store?
    base_dir = '.'
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]

    main(base_dir)
