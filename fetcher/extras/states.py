from bs4 import BeautifulSoup
from copy import copy
from datetime import datetime
from io import StringIO
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import ZipFile
import csv
import logging
import pandas as pd
import re
import shutil
import urllib, urllib.request

from fetcher.utils import request_and_parse, extract_attributes, \
   map_attributes, Fields, csv_sum, extract_arcgis_attributes


''' This file contains extra handling needed for some states
To make it work, the method must be called "handle_{state_abbreviation:lower_case}"
The parameters are:
- query result
- state mappings
'''

def atoi(val):
    if isinstance(val, int):
        return val
    return int(val.replace(",",''))

def handle_al(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state = 'AL')
        tagged.update(partial)

    widgets = res[-1].get('widgets', {})

    for widget in widgets:
        if widget.get('defaultSettings', {}) \
                    .get('description',"").find("STATEWIDE") >= 0:
            # now check that it's a numeric value
            recovered = widget['defaultSettings']['middleSection']['textInfo']['text'].strip()
            if re.match("[1-9][0-9,]*", recovered) is not None:
                tagged[Fields.RECOVERED.name] = atoi(recovered)

    return tagged


def handle_ar(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'AR')
        tagged.update(partial)

    soup = res[-1]
    tables = soup.find_all("table")
    table = tables[-1].find("tbody")
    for tr in table.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        name = cols[0].get_text(strip=True)
        value = cols[1].get_text(strip=True)
        if name in mapping:
            tagged[mapping[name]] = atoi(value)

    return tagged

def handle_fl(res, mapping):
    '''Need to add the non-FL residents to the totals:
    they separate it for death and hosp"
    '''
    res = res[0]
    mapped = extract_arcgis_attributes(res, mapping, 'FL')
    extra_hosp = 0
    extra_death = 0
    try:
        extra_hosp = res['features'][0]['attributes']['SUM_C_HospYes_NonRes']
        extra_death = res['features'][0]['attributes']['SUM_C_NonResDeaths']
    except Exception as ex:
        logging.warning("Failed Florida extra processing: ", e)
        raise

    mapped[Fields.HOSP.name] += extra_hosp
    mapped[Fields.DEATH.name] += extra_death

    return mapped

def handle_ky(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'KY')
        tagged.update(partial)

    # soup time
    soup = res[-1]
    h3 = soup.find("h3", string=re.compile("Coronavirus Monitoring"))
    if not h3:
        # quick fail
        return tagged

    datadiv = h3.find_next_sibling("div", "row")
    for item in datadiv.find_all("div", "info-card"):
        title = item.find("span", "title")
        value = item.find("span", "number")
        probable = item.find("span", "probable")
        if not value:
            continue

        hasalpha = lambda x: re.match("[a-zA-Z0-9]", x)
        pattern = "([a-zA-Z ]*): ([0-9,]*)"

        # class = title, number, probable
        title = title.get_text(strip=True)
        value = value.get_text(strip=True)
        probable = probable.get_text(strip=True) if probable else ""
        if probable:
            probable = re.findall(pattern, probable)

        if title.lower().find("total test") >= 0:
            for (k, v) in probable:
                if k.lower().find("pcr") >= 0:
                    tagged[Fields.TOTAL.name] = atoi(v)
                elif k.lower().find("serology") >= 0:
                    tagged[Fields.ANTIBODY_TOTAL.name] = atoi(v)
        elif title.lower().find("positive") >= 0:
            tagged[Fields.POSITIVE.name] = atoi(value)
            for (k, v) in probable:
                if k.lower().find("probable") >= 0:
                    tagged[Fields.PROBABLE.name] = atoi(v)
                elif k.lower().find("confirm") >= 0:
                    tagged[Fields.CONFIRMED.name] = atoi(v)
        elif title.lower().find("death") >= 0:
            tagged[Fields.DEATH.name] = atoi(value)
            for (k, v) in probable:
                if k.lower().find("probable") >= 0:
                    tagged[Fields.DEATH_PROBABLE.name] = atoi(v)
                elif k.lower().find("confirm") >= 0:
                    tagged[Fields.DEATH_CONFIRMED.name] = atoi(v)
        elif title.lower().find("recover") >= 0:
            tagged[Fields.RECOVERED.name] = atoi(value)

    updated = h3.find_next_sibling("p").get_text(strip=True)
    tagged[Fields.DATE.name] = updated

    return tagged

def handle_vt(res, mapping):
    tagged = {}
    pui = 'HOSP_PUI'
    for result in res:
        partial = extract_arcgis_attributes(result, mapping, 'VT')
        tagged.update(partial)

    tagged[Fields.CURR_HOSP.name] += tagged[pui]
    tagged.pop(pui)
    return tagged

def handle_pa(res, mapping):
    '''Need to sum ECMO and Vent number for a total count
    '''
    state = 'PA'
    tagged = {}
    updated_mapping = copy(mapping)
    ecmo = 'ecmo'
    updated_mapping.update({ecmo: ecmo})
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, updated_mapping, state)
        tagged.update(partial)

    tagged[Fields.CURR_VENT.name] += tagged[ecmo]
    tagged.pop(ecmo)

    # antibody stuff, soup time
    soup = res[-1]
    try:
        table = soup.find_all("table")[1]
        # expecting 2 rows, with 3 columns
        # need the value for "serology"
        rows = table.find_all('tr')
        titles = rows[0]
        data = rows[1].find_all('td')
        for i, title in enumerate(titles.find_all("td")):
            title = title.get_text(strip=True)
            if title.lower().find('serology') >= 0:
                value = atoi(data[i].get_text(strip=True))
                tagged[Fields.ANTIBODY_POS.name] = value

        if tagged.get(Fields.ANTIBODY_POS.name):
            tagged[Fields.POSITIVE.name] += tagged[Fields.ANTIBODY_POS.name]

    except Exception as e:
        pass
    return tagged

def handle_ne(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'NE')
        tagged.update(partial)
    stats = res[-1]
    if 'features' in stats and len(stats['features']) > 0:
        attributes = stats['features']
        for attr in attributes:
            # expecting {attributes: {lab_status: NAME, COUNT_EXPR0: VALUE}}
            name = attr['attributes']['lab_status']
            value = attr['attributes']['COUNT_EXPR0']
            if name in mapping:
                tagged[mapping[name]] = value

    return tagged

def handle_in(res, mapping):
    # ckan
    tagged = {}

    # There's pretty bad error handling now
    # I want to get errors as fast as possible -- to fix faster
    stats = res[0]['objects']['daily_statistics']
    tagged = map_attributes(stats, mapping, 'IN')

    hosp_data = res[1]['result']['records']
    for record in hosp_data:
        name = record['STATUS_TYPE']
        value = record['TOTAL']
        if name in mapping:
            tagged[mapping[name]] = value

    return tagged

def handle_la(res, mapping):
    stats = res[0]
    state_tests = 'STATE_TESTS'
    tagged = {}
    if 'features' in stats and len(stats['features']) > 0:
        attributes = stats['features']
        attributes = {attr.get('attributes', {}).get('Measure'): attr.get('attributes', {}).get('SUM_Value') for attr in attributes}
        tagged = map_attributes(attributes, mapping, 'LA')

    if state_tests in tagged:
        tests = tagged.pop(state_tests)
        tagged[Fields.TOTAL.name] += tests

    # parse the probable death and vent and hospital data
    # This is going to be fragile
    hosp_title = "Reported COVID-19 Patients in Hospitals"
    recovered_title = "Presumed Recovered"
    curr_hosp = ""
    curr_vent = ""
    recovered = ""

    widgets = res[1].get('widgets', {})
    for widget in widgets:
        if widget.get('defaultSettings', {}) \
                    .get('topSection', {}).get('textInfo', {}).get('text') == hosp_title:
            # Take the hosp value from the main number, and vent number from the small text
            curr_hosp = atoi(widget['datasets'][0]['data'])
            vent_subtext = widget['defaultSettings']['bottomSection']['textInfo']['text']
            curr_vent = atoi(vent_subtext.split()[0])
        elif widget.get('defaultSettings', {}) \
                    .get('topSection', {}).get('textInfo', {}).get('text', '').find(recovered_title) >= 0:
            recovered = atoi(widget['datasets'][0]['data'])

    tagged[Fields.CURR_HOSP.name] = curr_hosp
    tagged[Fields.CURR_VENT.name] = curr_vent
    tagged[Fields.RECOVERED.name] = recovered
    return tagged

def handle_il(res, mapping):
    state = 'IL'
    state_name = 'Illinois'
    mapped = {}
    # main dashboard
    for county in res[0]['characteristics_by_county']['values']:
        if county['County'] == state_name:
            mapped = map_attributes(county, mapping, state)

    last_update = res[0]['LastUpdateDate']
    y = last_update['year']
    m = last_update['month']
    d = last_update['day']
    timestamp = datetime(y,m,d).timestamp()
    mapped[Fields.TIMESTAMP.name] = timestamp

    # probable
    partial = map_attributes(res[0].get('probable_case_counts', {}), mapping, state)
    mapped.update(partial)

    # recovery
    partial = map_attributes(res[0].get('state_recovery_data', {}).get('values')[0], mapping, state)
    mapped.update(partial)

    # hospital data
    hosp_data = res[1]['statewideValues']
    hosp_mapped = map_attributes(hosp_data, mapping, state)
    mapped.update(hosp_mapped)

    # fill in the date
    last_update = res[1]['LastUpdateDate']
    y = last_update['year']
    m = last_update['month']
    d = last_update['day']
    updated = datetime(y,m,d, 23, 59).strftime("%m/%d/%Y %H:%M:%S")
    mapped[Fields.DATE.name] = updated

    return mapped

def handle_gu(res, mapping):
    res = res[0]
    tagged = {}
    if 'features' in res and len(res['features']) > 0:
        attributes = res['features']
        for attr in attributes:
            # expecting {attributes: {Variable: NAME, Count: VALUE}}
            name = attr['attributes']['Variable']
            value = attr['attributes']['Count']
            if name in mapping:
                tagged[mapping[name]] = value

    # sum all tests
    return tagged

def handle_hi(res, mapping):
    stats = res[0]
    # last row with values
    last_state_row = {}
    for row in stats:
        if row['Region'] == 'State' and row.get('Cases_Tot'):
            last_state_row = row

    tagged = {}
    # expecting the order be old -> new data, so last line is the newest
    for k, v in last_state_row.items():
        if k in mapping:
            tagged[mapping[k]] = v

    people = res[1]
    h2 = people.find('h2', string=re.compile("COVID-19 Cases"))
    h2par = h2.find_parent()
    for p in h2par.find_all("p"):
        text = p.get_text(strip=True)
        gr = re.search('[aA] total of ([0-9,]+) individuals', text)
        if gr:
            val = gr.group(1)
            tagged[Fields.TOTAL.name] = atoi(val)

    return tagged

def handle_ri(res, mapping):
    dict_res = {r[0]: r[1] for r in res[0]}
    mapped = map_attributes(dict_res, mapping, 'RI')
    return mapped

def handle_dc(res, mapping):
    # expecting 1 file:
    df = res[0]
    tagged = {}

    overall = df['Overall Stats']
    for tab in ['Testing', 'Hospitals']:
        subtable = overall[overall['Unnamed: 0'] == tab].T
        subtable.columns = subtable.loc['Unnamed: 1']
        subtable = subtable.iloc[-1]

        for name in subtable.index:
            if name in mapping:
                tagged[mapping[name]] = subtable[name]

    # Need hospitals tab for vent number
    hospitals = df['Hospital Data']
    # TODO: add to mapping
    vent = hospitals.iloc[-1]['Number of ventilators in use by COVID positive inpatients']
    tagged[Fields.CURR_VENT.name] = vent

    return tagged

def handle_de(res, mapping):
    df = res[0]
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    df = df[(df['Unit'] == 'people') & (df['Statistic'].isin(mapping.keys()))]
    max_date = df['Date'].max()
    df = df[df['Date'] == max_date]
    df = df.set_index('Statistic')

    mapped = map_attributes(df['Value'], mapping, 'DE')
    mapped.update({Fields.DATE.name: max_date})
    return mapped

def handle_va(res, mapping):
    '''Getting multiple CVS files from the state and parsing each for
    the specific data it contains
    '''
    tagged = {}

    # Res:
    # 0 -- cases & death, probable & confirmed
    # 1 -- testing info
    # 2 -- hospital/icu/vent
    cases = res[0]
    testing = res[1]
    hospital = res[2]

    date_format = "%m/%d/%Y"

    # Cases
    # expecting 2 rows in the following format
    # Report Date,Case Status,Number of Cases,Number of Hospitalizations,Number of Deaths
    # 5/14/2020,Probable,1344,24,28
    # 5/14/2020,Confirmed,26469,3568,927

    PROB = 'Probable'
    CONF = 'Confirmed'

    for row in cases:
        if (row['Case Status'] == CONF):
            for k, v in row.items():
                if (k in mapping):
                    tagged[mapping[k]] = atoi(v)
        elif (row['Case Status'] == PROB):
            tagged[Fields.PROBABLE.name] = atoi(row['Number of Cases'])
            tagged[Fields.DEATH_PROBABLE.name] = atoi(row['Number of Deaths'])
    tagged[Fields.POSITIVE.name] = tagged[Fields.CONFIRMED.name] + tagged[Fields.PROBABLE.name]

    # sum everything
    testing_cols = [
        'Number of PCR Testing Encounters', 'Number of Positive PCR Tests',
        'Total Number of Testing Encounters', 'Total Number of Positive Tests']
    summed_testing = csv_sum(testing, testing_cols)
    tagged[Fields.SPECIMENS.name] = summed_testing[testing_cols[0]]
    tagged[Fields.SPECIMENS_POS.name] = summed_testing[testing_cols[1]]
    tagged[Fields.ANTIBODY_TOTAL.name] = summed_testing[testing_cols[2]] - summed_testing[testing_cols[0]]
    tagged[Fields.ANTIBODY_POS.name] = summed_testing[testing_cols[3]] - summed_testing[testing_cols[1]]

    # Hospitalizations
    hospital = sorted(hospital, key=lambda x: datetime.strptime(x['Date'], date_format), reverse = True)
    mapped_hosp = map_attributes(hospital[0], mapping, 'VA')
    tagged.update(mapped_hosp)

    return tagged

def handle_nj(res, mapping):
    '''Need to parse everything the same, and add past recoveries
    to the new query, because I do not know how to add a constant
    to the ArcGIS query
    '''
    mapped = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'NJ')
        mapped.update(partial)

    # it's not a magic value, it's from an existing query, but
    # it's always the same
    mapped[Fields.RECOVERED.name] += 15642


    hosp = 'Hospitalizations'
    widgets = res[-1].get('widgets', {})

    for widget in widgets:
        if widget.get('defaultSettings', {}) \
                    .get('topSection',{}).get('textInfo', {}).get('text', "").find(hosp) >= 0:
            val = widget['defaultSettings']['middleSection']['textInfo']['text'].strip()
            if re.match("[1-9][0-9,]*", val) is not None:
                mapped[Fields.HOSP.name] = atoi(val)

    return mapped

def handle_ok(res, mapping):
    # need to sum all values
    res = res[0]

    # sum all fields
    # TODO: functools probably has something nice
    cols = ['Cases', 'Deaths', 'Recovered']
    summed = csv_sum(res, cols)
    mapped = map_attributes(summed, mapping, 'OK')
    mapped[Fields.DATE.name] = res[0].get('ReportDate')
    return mapped

def handle_or(res, mapping):
    mapped = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'NJ')
        mapped.update(partial)

    # The last item is the page that needs to be scraped
    page = res[-1]
    # main stats
    h4 = page.find('h4', string=re.compile("Overview"))
    main_table = h4.find_next_sibling('table')
    for row in main_table.find_all('tr'):
        tds = row.find_all('td')
        if len(tds) < 2:
            continue
        name = tds[0].get_text(strip=True)
        value = tds[1].get_text(strip=True)
        if name in mapping:
            mapped[mapping[name]] = atoi(value)


    tables = page.find_all('table')
    hosp = tables[4]
    curr_hosp = tables[6]

    td = hosp.find_all("td", limit=2)
    mapped[Fields.HOSP.name] = atoi(td[1].get_text(strip=True))

    # TODO: Unify this code (data tables)
    for tr in curr_hosp.find_all("tr"):
        tds = tr.find_all('td')
        if len(tds) < 2:
            continue
        name = tds[0].get_text(strip=True)
        value = tds[1].get_text(strip=True)
        if name in mapping:
            mapped[mapping[name]] = atoi(value)

    return mapped

def handle_mi(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'MI')
        tagged.update(partial)

    # TODO: Can use the reverse mapping
    cases = 'Cases'
    deaths = 'Deaths'
    probable = 'Probable'
    confirmed = 'Confirmed'
    pcr = 'Diagnostic'
    antibody = 'Serology'
    negative = 'Negative'
    positive = 'Positive'


    soup = res[-1]
    h = soup.find("h5", string=re.compile('[dD][aA][tT][aA]'))
    parent = h.find_parent("ul")
    links = parent.find_all("a")

    base_url = 'https://www.michigan.gov'
    cases_url = base_url + links[0]['href']
    tests_url = base_url + links[3]['href']
    results_url = base_url + links[4]['href']

    try:
        df = pd.read_excel(cases_url)
        filter_col = 'CASE_STATUS'
        summed = df.groupby(filter_col).sum()
        for m in [cases, deaths]:
            for t in [probable, confirmed]:
                tagged[mapping[m+t]] = summed[m][t]
    except Exception as e:
        logging.warning("Exception getting cases by status", e)

    df = pd.read_excel(tests_url)
    filter_col = 'TestType'
    summed = df.groupby(filter_col).sum()
    for m in [pcr, antibody]:
        tagged[mapping[m]] = summed['Count'][m]

    df = pd.read_excel(results_url)
    summed = df[[negative, positive]].sum()
    for x in [negative, positive]:
        tagged[mapping[x]] = summed[x]

    return tagged

def handle_nd(res, mapping):
    soup = res[-1]
    circles = soup.find_all("div", "circle")
    tagged = {}
    for c in circles:
        name = c.find('p').get_text(strip=True)
        value = atoi(c.find('h2').get_text(strip=True))
        if name in mapping:
            tagged[mapping[name]] = value

    # total tests
    tests_text = soup.find(string=re.compile("Tests Completed"))
    if tests_text:
        # take the 1st value, after stripping everything, including "-"
        tests_val = ""
        try:
            tests_text = tests_text.split()[0].split("-")[0]
            tests_val = atoi(tests_text)
        except Exception as e:
            logging.warning("Failed to parrse tests/text", e)
        if tests_val:
            tagged[Fields.SPECIMENS.name] = tests_val

    # Serology testing
    table = soup.find('table')
    rows = table.find_all('tr')
    titles = rows[0]
    data = rows[1].find_all('td')

    for i, title in enumerate(titles.find_all("td")):
        title = title.get_text(strip=True)
        if title in mapping:
            value = atoi(data[i].get_text(strip=True))
            tagged[mapping[title]] = value

    # probable death
    prob_death = [k for k, v in mapping.items() if v == Fields.DEATH_PROBABLE.name][0]
    prob = soup.find(string=re.compile(prob_death))
    td = prob.find_parent('td')
    val_td = td.find_next_sibling('td').get_text(strip=True)
    tagged[Fields.DEATH_PROBABLE.name] = atoi(val_td)

    return tagged

def handle_ma(res, mapping):
    soup = res[0]
    link = soup.find('a', string=re.compile("COVID-19 Raw Data"))
    link_part = link['href']
    url = "https://www.mass.gov{}".format(link_part)

    tagged = {}

    # download zip
    req = urllib.request.Request(url, headers = {'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response, \
         NamedTemporaryFile(delete=False) as tmpfile , TemporaryDirectory() as tmpdir:
            shutil.copyfileobj(response, tmpfile)
            tmpfile.flush()
            shutil.unpack_archive(tmpfile.name, tmpdir, format="zip")

            # Now we can read the files
            files = ['DeathsReported.csv', 'Testing2.csv',
                     'Hospitalization from Hospitals.csv', 'Cases.csv']
            for filename in files:
                with open("{}/{}".format(tmpdir, filename), 'r') as csvfile:
                    reader = csv.DictReader(csvfile, dialect = 'unix')
                    rows = list(reader)
                    last_row = rows[-1]
                    partial = map_attributes(last_row, mapping, 'MA')
                    tagged.update(partial)

            hosp_key = ""
            for k, v in mapping.items():
                if v == Fields.HOSP.name:
                    hosp_key = k
            hospfile = csv.DictReader(open(tmpdir + "/RaceEthnicity.csv", 'r'))
            hosprows = list(hospfile)
            last_row = hosprows[-1]
            hosprows = [x for x in hosprows if x['Date'] == last_row['Date']]
            summed = csv_sum(hosprows, [hosp_key])
            tagged[Fields.HOSP.name] = summed[hosp_key]

    return tagged

def handle_ut(res, mapping):
    tagged = {}
    soup_start = 1
    for result in res[:soup_start]:
        partial = extract_arcgis_attributes(result, mapping, 'NJ')
        tagged.update(partial)


    stats = res[1]
    for k, v in mapping.items():
        x = stats.find(id=k)
        if x:
            name = v
            value_item = x.find(class_='value')
            if not value_item:
                value_item = x.find(class_='value-output')
            if not value_item:
                continue
            value = atoi(value_item.get_text(strip=True))
            tagged[v] = value

    # inverse mapping
    revmap = {v: k for k, v in mapping.items()}
    hosp = res[2]
    tables = hosp.find_all('table')

    curr_hosp_table = tables[0]
    tds = curr_hosp_table.find_all('td', string=re.compile(revmap[Fields.CURR_HOSP.name]))
    curr_hosp = 0
    for td in tds:
        for x in td.next_siblings:
            if (x.name == 'td'):
                curr_hosp += atoi(x.get_text(strip=True))
    tagged[Fields.CURR_HOSP.name] = curr_hosp

    #TODO: code here can be improved, combined with top part
    td = curr_hosp_table.find('td', string=re.compile(revmap[Fields.CURR_ICU.name]))
    for x in td.next_siblings:
        if (x.name == 'td'):
            val = atoi(x.get_text(strip=True))
            tagged[Fields.CURR_ICU.name] = val

    for t in tables[1:]:
        if t.caption.get_text(strip=True) in mapping:
            td = t.find_all('td', limit=2)[1]
            tagged[mapping[t.caption.get_text(strip=True)]] = atoi(td.get_text(strip=True))

    return tagged

def handle_vi(res, mapping):
    # 0: covid page
    # 1: DoH page

    covid_page = res[0]
    container = covid_page.find('div', 'views-element-container block block-views block-views-blockcovid-19-epi-summary-block-1')

    tagged = {}

    header = container.find('div', 'view-header')
    header_text = header.get_text(strip=True)
    if header_text.startswith('Last updated'):
        tagged[Fields.DATE.name] = header_text[len('Last updated')+1:]

    divs = container.find_all('div', 'views-field')
    for x in divs:
        name = x.find('span').get_text(strip=True)
        if not x.find('div'):
            # this is the end
            break;
        value = x.find('div').get_text(strip=True)

        if name == 'Recovered':
            # need to special case it
            value = value.split("/")[0]

        if name in mapping:
            tagged[mapping[name]] = atoi(value)

    return tagged
