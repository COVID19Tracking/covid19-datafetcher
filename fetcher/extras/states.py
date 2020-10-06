""" This file contains extra handling needed for some states
To make it work, the method must be called "handle_{state_abbreviation:lower_case}"
The parameters are:
- query result
- state mappings
"""


from datetime import datetime
import csv
import logging
import math
import os
import re
import pandas as pd

from fetcher.utils import map_attributes, Fields, csv_sum, extract_arcgis_attributes
from fetcher.extras.common import MaContextManager


def atoi(val):
    if isinstance(val, int):
        return val
    return int(val.replace(",", ''))


def handle_al(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='AL')
        tagged.update(partial)

    widgets = res[-1].get('widgets', {})
    # 6 = hospitalizations
    # 29 = recoveries
    extras = [(widgets[6], Fields.HOSP.name),
              (widgets[29], Fields.RECOVERED.name)]

    for widget, field in extras:
        if widget.get('defaultSettings', {}) \
                    .get('description', "").find("STATEWIDE") >= 0:
            # now check that it's a numeric value
            val = widget['defaultSettings']['middleSection']['textInfo']['text'].strip()
            if re.match("[1-9][0-9,]*", val) is not None:
                tagged[field] = atoi(val)

    return tagged


def handle_fl(res, mapping):
    '''Need to add the non-FL residents to the totals:
    they separate it for death and hosp"
    '''
    mapped = extract_arcgis_attributes(res[1], mapping, 'FL')
    pcr = map_attributes(res[0], mapping, 'FL')
    mapped.update(pcr)

    # Current hosp csv
    hosp = res[2]
    for r in hosp:
        if r.get('County') == 'All':
            mapped[Fields.CURR_HOSP.name] = atoi(r.get('COVID Hospitalizations'))

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
        if not value:
            continue

        probable = item.find_all("span", "probable")
        pattern = "([a-zA-Z ]*): ?([0-9,]*)"

        # class = title, number, probable
        title = title.get_text(strip=True)
        value = value.get_text(strip=True)

        probable = " ".join([p.get_text(strip=True) if p else "" for p in probable])
        if probable and probable.strip():
            probable = re.findall(pattern, probable)

        if title.lower().find("total test") >= 0:
            for (k, v) in probable:
                if k.lower().find("pcr") >= 0:
                    tagged[Fields.SPECIMENS.name] = atoi(v)
                elif k.lower().find("serology") >= 0:
                    tagged[Fields.ANTIBODY_TOTAL.name] = atoi(v)
                elif k.lower().find('antigen') >= 0:
                    tagged[Fields.ANTIGEN_TOTAL.name] = atoi(v)
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
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'PA')
        tagged.update(partial)

    # soup time: recovered
    soup = res[-1]
    try:
        table = soup.find("table")
        total_cases = None
        recover_pct = None
        for td in table.find_all("td"):
            text = td.get_text(strip=True)
            text = text.strip(u'\u200b')
            if text.startswith('Total Cases'):
                total_cases = atoi(text[len('Total Cases*'):])
            elif text.startswith('Recovered'):
                recover_pct = atoi(text[len('Recovered***'):-1])
            elif text.startswith('Total PCR Tests'):
                specimens = atoi(text[len('Total PCR Tests'):])
                tagged[Fields.SPECIMENS.name] = specimens

        if total_cases and recover_pct:
            tagged[Fields.RECOVERED.name] = math.floor(total_cases * recover_pct / 100)
    except Exception:
        logging.warning("PA: failed to parse recovered", exc_info=True)
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


def handle_nh(res, mapping):
    # we love soup
    t = res[0].find('table')

    mapped = {}
    for tr in t.find_all('tr'):
        th = tr.find('th').get_text(strip=True)
        td = tr.find('td').get_text(strip=True)
        # numbers here are funny, need to clean a bit
        td = td.split()[0]
        if th in mapping:
            # yay, the faster option
            mapped[mapping[th]] = atoi(td)
            continue

    return mapped


def handle_la(res, mapping):
    stats = res[0]
    state_tests = 'STATE_TESTS'
    tagged = {}
    if 'features' in stats and len(stats['features']) > 0:
        attributes = stats['features']
        attributes = {attr.get('attributes', {}).get('Measure'):
                      attr.get('attributes', {}).get('SUM_Value') for attr in attributes}
        tagged = map_attributes(attributes, mapping, 'LA')

    if state_tests in tagged:
        tests = tagged.pop(state_tests)
        tagged[Fields.TOTAL.name] += tests

    # hospitalization
    for result in res[1:3]:
        partial = extract_arcgis_attributes(result, mapping, 'LA')
        tagged.update(partial)

    # recoveries from dashboard
    widgets = res[-1].get('widgets', {})
    for widget in widgets:
        if widget.get('name') == 'recovered':
            val = widget.get('datasets')[0].get('data')
            tagged[Fields.RECOVERED.name] = val
        if widget.get('defaultSettings', {}) \
                 .get('topSection', {}).get('textInfo', {}).get('text', '').find("Probable") >= 0:
            datasets = widget.get('datasets')
            for ds in datasets:
                if ds.get('type') == 'staticDataset':
                    tagged[Fields.PROBABLE.name] = atoi(ds.get('data'))

    return tagged


def handle_in(res, mapping):
    daily_stats = res[0]
    mapped = map_attributes(daily_stats, mapping, 'IN')

    # Base data is in "daily statistics", hosp data in "data"
    for metric_category in ['data', 'daily_statistics']:
        df = pd.DataFrame(daily_stats['metrics'][metric_category])
        df = df[df['district_type'] == 's']  # assuming "s" stands for State
        last_row = df.iloc[-1]  # assume either unique or sorted by date (that's what we know)
        for k, v in last_row.iteritems():
            if k in mapping:
                mapped[mapping[k]] = v
    return mapped


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
    timestamp = datetime(y, m, d).timestamp()
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
    updated = datetime(y, m, d, 23, 59).strftime("%m/%d/%Y %H:%M:%S")
    mapped[Fields.DATE.name] = updated

    return mapped


def handle_ga(res, mapping):
    tagged = {}
    for result in res:
        partial = extract_arcgis_attributes(result, mapping, debug_state='GA')
        tagged.update(partial)
    tagged[Fields.CURR_HOSP.name] += tagged.pop('CURR_HOSP_PUI')
    return tagged


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

    probables = res[2]
    h2 = probables.find('h3', id='probables')
    table = h2.find_next('table')
    probables_index = -1
    for i, th in enumerate(table.find('thead').find_all('th')):
        if th.get_text(strip=True).find("Total Probable Cases") >= 0:
            probables_index = i
            break

    probables_val = 0
    if probables_index >= 0:
        for tr in table.find('tbody').find_all('tr'):
            td = tr.find_all('td')[probables_index]
            probables_val += atoi(td.get_text(strip=True))

    tagged[Fields.PROBABLE.name] = probables_val

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
                    .get('topSection', {}).get('textInfo', {}).get('text', "").find(hosp) >= 0:
            val = widget['defaultSettings']['middleSection']['textInfo']['text'].strip()
            if re.match("[1-9][0-9,]*", val) is not None:
                mapped[Fields.HOSP.name] = atoi(val)

    return mapped


def handle_oh(res, mapping):
    soup = res[0]
    container = soup.find('div', 'stats-cards__container')
    tagged = {}
    for div in container.find_all('div', 'stats-cards__item'):
        name = div.find('div', 'stats-cards__label')
        val = div.find('div', 'stats-cards__number')
        if name and val and name and name.get_text(strip=True) in mapping:
            val = atoi(val.get_text(strip=True))
            tagged[mapping[name.get_text(strip=True)]] = val

    # Get last updated date
    msg = container.find_next_sibling('div', 'stats-cards__update-msg')
    if msg:
        spans = msg.find_all('span')
        tagged[Fields.DATE.name] = spans[1].get_text(strip=True)
    return tagged


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
        partial = extract_arcgis_attributes(result, mapping, 'OR')
        mapped.update(partial)

    # The last item is the page that needs to be scraped
    page = res[-1]
    # main stats
    main_table = page.find('table')
    for row in main_table.find_all('tr'):
        tds = row.find_all('td')
        if len(tds) < 2:
            continue
        name = tds[0].get_text(strip=True)
        if tds[1].find('sup') is not None:
            value = tds[1].find('b').find(text=True, recursive=False)
        else:
            value = tds[1].get_text(strip=True)
        if name in mapping:
            try:
                mapped[mapping[name]] = atoi(value)
            except Exception:
                logging.warning("OR: failed to parse {} for {}".format(value, name), exc_info=True)

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


def handle_me(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'MI')
        tagged.update(partial)

    soup = res[-1]
    th = soup.find("th", string=re.compile("Reported COVID-19 Tests"))
    table = th.find_parent('table')
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 3:
            continue
        name = tds[0].get_text(strip=True)
        if name not in ['Positive', 'Negative', 'Total']:
            continue
        antibody_val = atoi(tds[1].get_text(strip=True))
        pcr_val = atoi(tds[2].get_text(strip=True))

        if name == 'Positive':
            tagged[Fields.ANTIBODY_POS.name] = antibody_val
            tagged[Fields.SPECIMENS_POS.name] = pcr_val
        elif name == 'Negative':
            tagged[Fields.ANTIBODY_NEG.name] = antibody_val
            tagged[Fields.SPECIMENS_NEG.name] = pcr_val
        elif name == 'Total':
            tagged[Fields.ANTIBODY_TOTAL.name] = antibody_val
            tagged[Fields.SPECIMENS.name] = pcr_val

    return tagged


def handle_mi(res, mapping):
    tagged = {}
    for result in res[:2]:
        partial = extract_arcgis_attributes(result, mapping, 'MI')
        tagged.update(partial)

    # Recoveries soup
    recovered_page = res[-3]
    recover_p = recovered_page.find('div', 'fullContent')
    span = recover_p.find('span').get_text(strip=True)
    tagged[Fields.RECOVERED.name] = atoi(span)

    # Hospitalization soup
    hospitalization_page = res[-2]
    tables = hospitalization_page.find_all('table')
    vent = None
    icu = None
    hosp = None
    for t in tables:
        caption = t.find('caption').get_text(strip=True)
        if caption.startswith('COVID-19 Metrics'):
            for row in t.find_all('tr'):
                th = row.find('th')
                if th and th.get_text(strip=True).startswith('Hospitalized and Ventilated'):
                    # take last td
                    td = row.find_all('td')[-1]
                    vent = td.get_text(strip=True)
        elif caption.startswith('Patient Census'):
            # This is where we take icu and hosp data
            last_row = t.find_all('tr')[-1]
            tds = last_row.find_all('td')
            hosp = tds[1].get_text(strip=True)
            icu = tds[2].get_text(strip=True)

    if vent is not None:
        tagged[Fields.CURR_VENT.name] = atoi(vent)
    if icu is not None and hosp is not None:
        tagged[Fields.CURR_HOSP.name] = atoi(hosp)
        tagged[Fields.CURR_ICU.name] = atoi(icu)

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

    try:
        df = pd.read_excel(results_url)
        summed = df[[negative, positive]].sum()
        for x in [negative, positive]:
            tagged[mapping[x]] = summed[x]
    except Exception:
        logging.warning("[MI] Failed to fetch test results")

    return tagged


def handle_mn(res, mapping):
    mapped = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'NJ')
        mapped.update(partial)

    # testing
    soup = res[-1]
    h2 = soup.find_all(['h2', 'h3'])
    for x in h2:
        title = x.get_text(strip=True).strip()
        if title == 'Testing':
            # do the testing thing
            p = x.find_next_sibling('p').get_text(strip=True)
            li = x.find_next_sibling('ul').get_text(strip=True)
            pcr = p.split(":")[-1].strip()
            pcr_people = li.split(":")[-1].strip()
            mapped[Fields.PCR_TEST_ENCOUNTERS.name] = atoi(pcr)
            mapped[Fields.TOTAL.name] = atoi(pcr_people)
        elif title == 'Deaths':
            li = x.find_next_sibling('ul').find_all('li')[-1]
            for x in li.stripped_strings:
                if x.strip().isnumeric():
                    mapped[Fields.DEATH_PROBABLE.name] = atoi(x.strip())
        # elif title == 'Hospitalization':
        #     li = x.find_next_sibling('ul').find('li')
        #     items = list(li.stripped_strings)
        #     for i in range(0, len(items), 2):
        #         if items[i].find(":") >= 0:
        #             if 'ICU' in items[i]:
        #                 mapped[Fields.CURR_ICU.name] = atoi(items[i+1])
        #             if 'Hospitalized' in items[i]:
        #                 mapped[Fields.CURR_HOSP.name] = atoi(items[i+1])

    return mapped


def handle_ms(res, mapping):
    soup = res[0]
    mapped = {}

    tables = soup.find_all('table')
    # expecting [<some number of tables>, cases/death table, testing]

    # skip tables until we get to the cases table
    for i, t in enumerate(tables):
        header = t.find('thead').get_text(strip=True)
        if header.lower() == 'confirmedprobabletotal':
            break
    tables = tables[i:]

    status = tables[0]
    trs = status.find('tbody').find_all('tr')
    cases = trs[0]
    cases_fields = [Fields.CONFIRMED, Fields.PROBABLE, Fields.POSITIVE]
    deaths = trs[1]
    deaths_fields = [Fields.DEATH_CONFIRMED, Fields.DEATH_PROBABLE, Fields.DEATH]
    for tr, title, fields in [(cases, 'cases', cases_fields),
                              (deaths, 'deaths', deaths_fields)]:
        tds = tr.find_all('td')
        if tds[0].get_text(strip=True).lower() == title:
            # we're in the right place
            for i, field in enumerate(fields):
                mapped[field.name] = atoi(tds[i+1].get_text(strip=True))

    testing = tables[1]
    fields = [Fields.SPECIMENS, Fields.ANTIBODY_TOTAL, Fields.ANTIGEN_TOTAL]
    titles = ['pcr', 'antibody', 'antigen']
    header = [x.get_text(strip=True) for x in testing.find('tr').find_all('td')]
    totals = testing.find_all('tr')[-1].find_all('td')
    for i in range(len(titles)):
        if header[i+2].lower() == titles[i]:
            mapped[fields[i].name] = atoi(totals[i+2].get_text(strip=True))

    return mapped


def handle_nc(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='NC')
        tagged.update(partial)

    # CSV
    df = res[-2]
    df['Date'] = pd.to_datetime(df['Date'])
    df['Measure Names'] = df['Measure Names'].str.strip()
    df = df.pivot(index='Date', columns='Measure Names')

    # deaths are already cumulative, take the latest
    tagged[Fields.DEATH.name] = df['Measure Values']["NC Deaths"].iloc[-1]

    for k, v in df.sum().iteritems():
        if k[1] in mapping:
            tagged[mapping[k[1]]] = v

    df = res[-1]
    df['Date'] = pd.to_datetime(df['Date'])
    df['Measure Names'] = df['Measure Names'].str.strip()
    df = df.pivot(index='Date', columns='Measure Names')
    for k, v in df.sum().iteritems():
        if k[1] in mapping:
            tagged[mapping[k[1]]] = v + tagged.get(mapping[k[1]], 0)

    return tagged


def handle_nd(res, mapping):
    soup = res[0]
    tagged = {}

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

    # confirmed+probable death
    h2_death = soup.find("h2", string=re.compile("Deaths"))
    death_table = h2_death.find_next("table")

    for tr in death_table.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        strong = cols[0].find("strong")
        if not strong or len(strong.get_text()) < 10:
            continue
        name = strong.get_text(strip=True)
        value = cols[1].get_text(strip=True)
        if name in mapping:
            tagged[mapping[name]] = atoi(value)

    # by county testing snapshot
    county_testing = res[1]
    columns = [k for k, v in mapping.items() if v == Fields.NEGATIVE.name]
    specimens = csv_sum(county_testing, columns=columns)
    tagged[Fields.NEGATIVE.name] = specimens[columns[0]]

    # PCR encounters and other metrics
    pcr = res[2]
    columns = [k for k, v in mapping.items() if v in [
        Fields.TOTAL.name, Fields.SPECIMENS_NEG.name, Fields.PCR_TEST_ENCOUNTERS.name,
        Fields.CONFIRMED.name, Fields.SPECIMENS.name, Fields.RECOVERED.name
        ]]
    values = csv_sum(pcr, columns)
    partial = map_attributes(values, mapping)
    tagged.update(partial)

    return tagged


def handle_ma(res, mapping):
    tagged = {}

    files = ['DeathsReported.csv', 'Testing2.csv',
             'Hospitalization from Hospitals.csv', 'Cases.csv']

    with MaContextManager(res[0]) as zipdir:
        for filename in files:
            with open(os.path.join(zipdir, filename), 'r') as csvfile:
                reader = csv.DictReader(csvfile, dialect='unix')
                rows = list(reader)
                last_row = rows[-1]
                partial = map_attributes(last_row, mapping, 'MA')
                tagged.update(partial)

        inverse_mapping = {v: k for k, v in mapping.items()}
        keys = [Fields.HOSP.name, Fields.DEATH.name, Fields.POSITIVE.name]
        keys = [inverse_mapping[k] for k in keys]

        hospfile = csv.DictReader(open(os.path.join(zipdir, "RaceEthnicity.csv"), 'r'))
        hosprows = list(hospfile)
        last_row = hosprows[-1]
        hosprows = [x for x in hosprows if x['Date'] == last_row['Date']]
        summed = csv_sum(hosprows, keys)
        for k in keys:
            tagged[mapping[k]] = summed[k]

    # weekly report:
    with MaContextManager(res[0], "Weekly Public Health Report - Raw Data", file_type='xls') as tmpfile:
        df = pd.read_excel(tmpfile, sheet_name=None)

        # recovered
        rec = df['Quarantine and Isolation']
        recovered = rec[rec['Status'] == 'Total Cases Released from Isolation'].iloc[-1]['Residents']
        tagged[Fields.RECOVERED.name] = recovered

    return tagged


def handle_tx(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='TX')
        tagged.update(partial)

    # last item is the current ICU DataFrame
    df = res[-1]
    icu = df.loc[df[df.columns[0]] == 'Total'][df.columns[-1]].iloc[-1]
    tagged[Fields.CURR_ICU.name] = icu
    return tagged


def handle_ut(res, mapping):
    tagged = {}
    soup_start = 1
    for result in res[:soup_start]:
        partial = extract_arcgis_attributes(result, mapping, 'UT')
        tagged.update(partial)

    stats = res[1]
    for k, v in mapping.items():
        x = stats.find(id=k)
        if x:
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
            if x.name == 'td':
                curr_hosp += atoi(x.get_text(strip=True))
    tagged[Fields.CURR_HOSP.name] = curr_hosp

    # TODO: code here can be improved, combined with top part
    td = curr_hosp_table.find('td', string=re.compile(revmap[Fields.CURR_ICU.name]))
    for x in td.next_siblings:
        if x.name == 'td':
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
    container = covid_page.find(
        'div',
        'views-element-container block block-views block-views-blockcovid-19-epi-summary-block-1')

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
            break
        value = x.find('div').get_text(strip=True)

        if name == 'Recovered':
            # need to special case it
            value = value.split("/")[0]

        if name in mapping:
            tagged[mapping[name]] = atoi(value)

    return tagged


def handle_wi(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'WI')
        tagged.update(partial)

    # testing encounters
    testing = res[-1]
    encounters = [k for k, v in mapping.items() if v == Fields.PCR_TEST_ENCOUNTERS.name][0]
    value = testing[testing['Measure Names'] == encounters]['Totals'].sum()
    tagged[mapping[encounters]] = value
    return tagged
