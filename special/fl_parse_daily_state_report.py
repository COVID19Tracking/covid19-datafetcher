from tempfile import NamedTemporaryFile
import PyPDF4
import urllib
import urllib.request
import shutil
import json
import os
import sys


URL = 'http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/cases-monitoring-and-pui-information/state-report/state_reports_latest.pdf'
SER_URL = 'http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/total-antibody-results/serology-reports/serology_latest.pdf'

def atoi(val):
    '''Raises exception on failure to parse'''
    if isinstance(val, int):
        return val
    return int(val.replace(",", ''))


def parse_totals(totals):
    '''Getting an array of tokens, satrig with the string "Total"
    returning a tagged dictionary, expecting the order to be.
    This method handles a couple of formats, depending on the length of
    the input. If there is a difference in order of values (e.g., swap betwene
    negatives and positives) this will not be cought here and will be parsed
    incorrectly.
    '''
    res = None

    if len(totals) == 6:
        # expected:
        # ["Title", Inconclusive, Negative, Positive, Percent Positive, Total]
        # Or
        # ["title", "Positive", "Negative", "Inconclusive", "Total", "Percent positivity"]
        if totals[-1].strip().endswith("%"):
            res = {
                'Inconclusive': atoi(totals[3]),
                'Negative': atoi(totals[2]),
                'Positive': atoi(totals[1]),
                'Total': atoi(totals[4])
            }
        else:
            res = {
                'Inconclusive': atoi(totals[1]),
                'Negative': atoi(totals[2]),
                'Positive': atoi(totals[3]),
                'Total': atoi(totals[5])
            }
    elif len(totals) == 5:
        # expected:
        # ["Total", Negative, Positive, Percent Positive, Total]
        res = {
            'Negative': atoi(totals[1]),
            'Positive': atoi(totals[2]),
            'Total': atoi(totals[4])
        }
    else:
        print("\tTotals have some other format?")
        print(totals)

    return res


def get_antibody_tests(pages):
    people_page = pages[0]
    lines = people_page.splitlines()
    index = lines.index("Total tested")
    interesting = lines[index:index+10]

    # quick map:
    mapping = {
        'Total tested': 'Total_people',
        'Positive': 'Positive_people',
        'Negative': 'Negative_people',
        'Inconclusive': 'Inconclusive_people'
    }

    res = {}
    for i in range(0, 10, 2):
        if interesting[i].strip() in mapping:
            res[mapping[interesting[i].strip()]] = atoi(interesting[i+1].strip())

    # Tests
    tests = pages[1].splitlines()
    # small heuristic here about the location
    index = tests.index('Total', len(tests) - 10)
    part = parse_totals(tests[index:])
    res.update(part)
    return res


def get_pcr_tests(page_text):
    totals = page_text.splitlines()

    # We need the last index for "Total"
    index = None
    date_str = None
    res = {}
    for i, row in enumerate(totals):
        if row.startswith("Data through"):
            date_str = row
        if row == "Total":
            index = i
    res = parse_totals(totals[index:])
    if date_str:
        if not res:
            res = {}
        res['Date'] = date_str
    return res

def get_data(filepath, prefix):
    pdf = PyPDF4.PdfFileReader(filepath)
    res = {}
    # people vs tests
    if prefix == 'SER':
        part = get_antibody_tests([
            pdf.getPage(0).extractText(), pdf.getPage(pdf.getNumPages() - 3).extractText()])
        res.update(part)
    else:
        testing_candidate = pdf.getPage(pdf.getNumPages() - 1)
        part = get_pcr_tests(testing_candidate.extractText())
        res.update(part)

    # apply prefix to all keys:
    res = {"{}_{}".format(prefix, k): v for k, v in res.items()}
    return res


def main(base_dir):
    # download report
    # open it
    # parse it
    # store numbers, as json, somewhere

    data = {}
    urls = {"PCR": URL, "SER": SER_URL}
    for prefix, url in urls.items():
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response, NamedTemporaryFile(delete=True) as tmpfile:
            shutil.copyfileobj(response, tmpfile)
            tmpfile.flush()
            part = get_data(tmpfile.name, prefix)
            data.update(part)

    print(data)
    json.dump(data, open(os.path.join(base_dir, "fl_pcr_specimens.json"), 'w'))


if __name__ == "__main__":
    # where to store?
    base_dir = '.'
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]

    main(base_dir)
