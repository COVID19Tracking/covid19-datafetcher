import PyPDF4
import os
import pandas as pd
import re
import sys


'''
Use it on a folder such as:
https://github.com/olivierlacan/florida-department-of-health-covid-19-report-archive/blob/trunk/state/

How to run it:
python fl_parse_state_reports.py [optional folder]

Default folder name, if not specified: 'state_reports'

'''


def date_parser(filename):
    """
    Parsing date from filenames
    This function takes in a filename usually of the format "state_reports_yyyy-mm-dd-randomnumbers.pdf" and extract
    date out in format "yyyy-mm-dd" as string
    Input:
    filename: The filename (without full directory) ending in ".pdf"
    Output:
    date: Date in format "yyyy-mm-dd" as string
    """

    'file format: state_report_yyyy-mm-dd(_whatever)'
    return filename[len("state_reports_"):len("state_reports_")+10]


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
        # ["Total", Inconclusive, Negative, Positive, Percent Positive, Total]
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


def get_data(filepath):
    """
    Get page to look for data
    This function takes in the Florida pdf file and returns the testing numbers for positive, negative and totals.
    First, it searches for the phrase "Coronavirus: testing by laboratory", which is on the top of all the pages
    relevant to testing. Then extracts the last page, split by lines, extract the last 4 lines, remove the percent
    positive and convert to integer.
    Input: filepath for pdf file as string
    Output: total tests as list of integers in order Negative, Positive, Totals
    """

    title = "Coronavirus: testing by laboratory"
    pdf = PyPDF4.PdfFileReader(filepath)

    # Find the page with the title
    testing_candidate = None
    for page in pdf.pages:
        page_text = page.extractText()
        if re.search(title, page_text):
            # yay, found the page
            testing_candidate = page
        else:
            if testing_candidate is not None:
                # if we already populated candidate, and the text does not
                # match, we're at a next section
                break

    if testing_candidate:
        # yay, we found the last page of with the title
        totals = testing_candidate.extractText().splitlines()

        # We need the last index for "Total"
        index = None
        for i, row in enumerate(totals):
            if row == "Total":
                index = i
        return parse_totals(totals[index:])
    return None


def main(base_dir):
    res = []
    misses = []
    with os.scandir(base_dir) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith('.pdf'):
                print("Handling ", entry.name)
                try:
                    # oh Python and your indentations... WTH
                    date = date_parser(entry.name)
                    data = get_data(os.path.join(base_dir, entry.name))
                    if data:
                        data['Date'] = date
                        res.append(data)
                    else:
                        misses.append(entry.name)
                except Exception as e:
                    print("Failed to parse ", entry.name, str(e))
                    misses.append(entry.name)

    df = pd.DataFrame(res, columns=['Date', 'Negative', 'Positive', 'Total'])
    df = df.set_index('Date')
    df.sort_index(inplace=True)
#    df.sort_values(by='Date', inplace=True)
    df.to_csv("specimens_tests_by_date.csv")
    print("Failed to parse: ", set(misses))


if __name__ == "__main__":
    base_dir = 'state_reports'
    print(sys.argv)
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]

    main(base_dir)
