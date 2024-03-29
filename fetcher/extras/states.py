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
from fetcher.extras.common import atoi, MaRawData, zipContextManager


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


def handle_az(res, mapping):
    mapped = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'AZ')
        mapped.update(partial)
    # testing
    df = res[-1]
    mapped[Fields.SPECIMENS.name] = df.sum()['value']
    return mapped


def handle_fl(res, mapping):
    '''Need to add the non-FL residents to the totals:
    they separate it for death and hosp"
    '''
    mapped = map_attributes(res[0], mapping, 'FL')

    for result in res[1:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'FL')
        mapped.update(partial)

    # pcr encounters
    result = res[-1].get('features', [{}])[0].get('attributes')
    mapped[Fields.PCR_TEST_ENCOUNTERS.name] = sum(result.values())
    return mapped


def handle_ky(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'KY')
        tagged.update(partial)

    # soup time
    soup = res[-1]
    datacards = soup.find_all('div', 'info-card')
    for item in datacards:
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

    updated = soup.find('p', string=re.compile('Current as of')).get_text(strip=True)
    tagged[Fields.DATE.name] = updated

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
    # this is where mapping and no "as" support is breaking
    stats = stats.get('features', [{}])[0].get('attributes')
    tagged[Fields.POSITIVE.name] = stats['TotalPositiveAsOfThisDate']
    tagged[Fields.NEGATIVE.name] = stats['TotalNotDetectedAsOfThisDate']
    tagged[Fields.INCONCLUSIVE.name] = stats['TotalInconclusiveAsOfThisDate']
    tagged[Fields.TOTAL.name] = stats['AllTestsAsOfThisDate']
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
            mapped[mapping[th]] = atoi(re.search("[0-9,]+", td).group(0))
            continue

    # cases + tests
    for df in res[1:]:
        mapped.update(map_attributes(df.sum(), mapping, 'NH'))

    return mapped


def handle_la(res, mapping):
    tagged = {}
    for stats in res[:2]:
        if 'features' in stats and len(stats['features']) > 0:
            attributes = stats['features']
            attributes = {attr.get('attributes', {}).get('Measure'):
                          attr.get('attributes', {}).get('SUM_Value') for attr in attributes}
            tagged.update(map_attributes(attributes, mapping, 'LA'))

    # everything else
    for result in res[2:]:
        partial = extract_arcgis_attributes(result, mapping, 'LA')
        tagged.update(partial)

    return tagged


def handle_id(res, mapping):
    # yay, tableau
    mapped = {}
    for df in res[0]:
        mapped.update(map_attributes(df.iloc[-1], mapping))
    return mapped


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

    partial = map_attributes(res[1]['result']['records'][0], mapping, 'IN')
    mapped.update(partial)

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

    # hospital data
    hosp_data = res[1]['statewideValues']
    hosp_mapped = map_attributes(hosp_data, mapping, state)
    mapped.update(hosp_mapped)

    # fill in the date
    last_update = res[1]['lastUpdatedDate']
    y = last_update['year']
    m = last_update['month']
    d = last_update['day']
    updated = datetime(y, m, d, 23, 59).strftime("%m/%d/%Y %H:%M:%S")
    mapped[Fields.DATE.name] = updated

    return mapped


def handle_ga(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='GA')
        tagged.update(partial)
    tagged[Fields.CURR_HOSP.name] += tagged.pop('CURR_HOSP_PUI')

    # last item is zip
    files = ["total_testing.csv", "summary_totals.csv"]
    with zipContextManager(res[-1]) as zipdir:
        for filename in files:
            summary = csv.DictReader(open(os.path.join(zipdir, filename), 'r'))
            summary = list(summary)
            summary = summary[-1]
            partial = map_attributes(summary, mapping, 'GA')
            tagged.update(partial)

    return tagged


def handle_hi(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='HI')
        tagged.update(partial)

    probables = res[-1]
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
        subtable = subtable.iloc[2:]
        # need to drop the non-date values at the ending
        subtable['Date'] = pd.to_datetime(subtable.index, errors='coerce')
        subtable.index = subtable['Date']
        subtable = subtable.loc[subtable.index.dropna()]

        for name in subtable.iloc[-1].index:
            if name in mapping:
                tagged[mapping[name]] = subtable.iloc[-1][name]

    # Need hospitals tab for vent number
    hospitals = df['Hospital Data'].dropna(how='all')
    # TODO: add to mapping
    vent = hospitals.iloc[-1]['Number of ventilators in use by COVID positive inpatients']
    tagged[Fields.CURR_VENT.name] = vent

    return tagged


def handle_de(res, mapping):
    df = res[0]
    df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
    people = df[(df['Unit'].isin(['people'])) & (df['Statistic'].isin(mapping.keys()))]
    max_date = df['Date'].max()
    people = people[people['Date'] == max_date]
    people = people.set_index('Statistic')

    mapped = map_attributes(people['Value'], mapping, 'DE')
    mapped.update({Fields.DATE.name: max_date})

    # Here's the funny thing, while we need to *max* date for most metrics, we need
    # a 2-day lagged testing metric
    tests = df[(df['Unit'].isin(['tests'])) & (df['Statistic'].isin(mapping.keys()))]
    tests_date = tests['Date'].sort_values().unique()[-3]
    tests = tests[tests['Date'] == tests_date]
    tests = tests.set_index('Statistic')
    partial = map_attributes(tests['Value'], mapping, 'DE')
    mapped.update(partial)

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

    # Find the magic number added to probables
    widget = res[-1]['widgets'][17]
    val = atoi(widget.get('valueConversion', {}).get('offset', 0))
    mapped[Fields.PROBABLE.name] += val

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
    mapped = {}
    for result in res[:-1]:
        partial = map_attributes(result, mapping, 'OK')
        mapped.update(partial)

    # need to sum all values
    res = res[1]

    # sum all fields
    # TODO: functools probably has something nice
    cols = ['Cases', 'Deaths', 'Recovered']
    summed = csv_sum(res, cols)
    mapped.update(map_attributes(summed, mapping, 'OK'))
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

    return mapped


def handle_md(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'MD')
        tagged.update(partial)

    # Antigen people
    attributes = [x['attributes'] for x in res[-1]['features']]

    df = pd.DataFrame(attributes).T
    df.columns = df.iloc[1]
    df['date'] = pd.to_datetime(df.index, format="d_%m_%d_%Y", errors='coerce')
    sr = df.sort_values('date', ascending=False).dropna().iloc[0]
    tagged.update(extract_arcgis_attributes(sr, mapping, 'MD'))

    return tagged


def handle_me(res, mapping):
    tagged = {}
    # summary csv from tableau
    df = res[0]
    df = df[df['Patient County'] == 'All'].set_index('Measure Names')
    partial = map_attributes(df['Measure Values'], mapping, 'ME')
    tagged.update(partial)

    # hospital capacity
    df = res[1]
    partial = map_attributes(df.iloc[0], mapping, 'ME')
    tagged.update(partial)

    soup = res[-1]
    th = soup.find("th", string=re.compile("Results from Labs Reporting Electronically"))
    table = th.find_parent('table')
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 3:
            continue
        name = tds[0].get_text(strip=True)
        if name not in ['Positive', 'Negative', 'Total']:
            continue
        antibody_val = atoi(tds[1].get_text(strip=True))
        antigen_val = atoi(tds[2].get_text(strip=True))
        pcr_val = atoi(tds[3].get_text(strip=True))

        if name == 'Positive':
            tagged[Fields.ANTIBODY_POS.name] = antibody_val
            tagged[Fields.ANTIGEN_POS.name] = antigen_val
            tagged[Fields.SPECIMENS_POS.name] = pcr_val
        elif name == 'Negative':
            tagged[Fields.ANTIBODY_NEG.name] = antibody_val
            tagged[Fields.ANTIGEN_NEG.name] = antigen_val
            tagged[Fields.SPECIMENS_NEG.name] = pcr_val
        elif name == 'Total':
            tagged[Fields.ANTIBODY_TOTAL.name] = antibody_val
            tagged[Fields.ANTIGEN_TOTAL.name] = antigen_val
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
    vent = 0
    icu = 0
    hosp = 0
    for t in tables:
        caption = t.find('caption').get_text(strip=True)
        if caption.startswith('COVID-19 Metrics'):
            for row in t.find_all('tr'):
                th = row.find('th')
                if th and th.get_text(strip=True).startswith('Total Hospitalized Adult'):
                    # take last td
                    td = row.find_all('td')[-1]
                    hosp += atoi(td.get_text(strip=True))
                elif th and th.get_text(strip=True).startswith('Hospitalized Peds'):
                    td = row.find_all('td')[-1]
                    hosp += atoi(td.get_text(strip=True))
                elif th and th.get_text(strip=True).startswith('Adult ICU Confirmed/Suspected'):
                    td = row.find_all('td')[-1]
                    icu += atoi(td.get_text(strip=True))
                elif th and th.get_text(strip=True).startswith('Hospitalized and Ventilated'):
                    td = row.find_all('td')[-1]
                    vent += atoi(td.get_text(strip=True))

    tagged[Fields.CURR_VENT.name] = atoi(vent)
    tagged[Fields.CURR_HOSP.name] = atoi(hosp)
    tagged[Fields.CURR_ICU.name] = atoi(icu)

    # TODO: Can use the reverse mapping
    soup = res[-1]
    h = soup.find("h5", string=re.compile('[dD][aA][tT][aA]'))
    parent = h.find_parent("ul")
    links = parent.find_all("a")

    base_url = 'https://www.michigan.gov'
    cases_url = base_url + links[0]['href']
    tests_url = base_url + links[3]['href']
    results_url = base_url + links[4]['href']

    try:
        df = pd.read_excel(cases_url, engine='xlrd')
        filter_col = 'CASE_STATUS'
        summed = df.groupby(filter_col).sum()
        for m in ['Cases', 'Deaths']:
            for t in ['Confirmed', 'Probable']:
                tagged[mapping[m+t]] = summed[m][t]
    except Exception as e:
        logging.warning("Exception getting cases by status", e)

    try:
        df = pd.read_excel(tests_url, engine='xlrd')
        filter_col = 'TestType'
        summed = df.groupby(filter_col).sum()
        for m in ['Diagnostic', 'Serology']:
            tagged[mapping[m]] = summed['Count'][m]
    except Exception:
        logging.warning("[MI] failed to fetch test results")

    try:
        df = pd.read_excel(results_url, engine='xlrd')
        fields = ['Negative', 'Positive']
        summed = df[fields].sum()
        for x in fields:
            tagged[mapping[x]] = summed[x]
    except Exception:
        logging.warning("[MI] Failed to fetch test results")

    return tagged


def handle_mn(res, mapping):
    mapped = {}
    for result in res[:1]:
        partial = extract_arcgis_attributes(result, mapping, 'NJ')
        mapped.update(partial)

    # testing
    soup = res[-1]
    h2 = soup.find_all(['h2', 'h3'])
    for x in h2:
        title = x.get_text(strip=True).strip().strip(":")
        if title in ['Testing', 'Deaths', 'Hospitalizations', 'Daily Update']:
            tables = x.find_next_siblings('table', limit=2)
            for t in tables:
                for tr in t.find_all('tr'):
                    title = tr.find('th').get_text(strip=True).strip()
                    value = tr.find('td').get_text(strip=True).strip()
                    if title in mapping:
                        mapped[mapping[title]] = atoi(value)

    return mapped


def handle_mo(res, mapping):
    testing = res[0]
    foo = testing[testing['Test Date'] == 'All'].groupby('Measure Names', as_index=True).sum()
    mapped = map_attributes(foo['Measure Values'], mapping, 'MO')

    county = res[1]
    county = county[county['County'] == 'All'].set_index('Measure Names')
    partial = map_attributes(county['Measure Values'].astype(int), mapping, 'MO')
    mapped.update(partial)
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
    for tr in testing.find_all('tr'):
        tds = tr.find_all('td')
        title = tds[0].get_text(strip=True).strip()
        if title in mapping:
            mapped[mapping[title]] = atoi(tds[1].get_text(strip=True))

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
    df = df.pivot(index='Date', columns='Measure Names', values='Measure Values')

    for k, v in df.sum().iteritems():
        if k in mapping:
            tagged[mapping[k]] = v

    df = res[-1]
    df['Date'] = pd.to_datetime(df['Date'])
    df['Measure Names'] = df['Measure Names'].str.strip()
    df = df.pivot(index='Date', columns='Measure Names', values='Measure Values')
    for k, v in df.sum().iteritems():
        if k in mapping:
            tagged[mapping[k]] = v + tagged.get(mapping[k], 0)

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
        value = atoi(cols[1].get_text(strip=True))
        if len(cols) > 2:
            value += atoi(cols[2].get_text(strip=True))
        if name in mapping:
            tagged[mapping[name]] = atoi(value)

    # by county testing snapshot: for negatives
    county_testing = res[1]
    columns = [k for k, v in mapping.items() if v in [
        Fields.CONFIRMED.name, Fields.NEGATIVE.name, Fields.DEATH_CONFIRMED.name
        ]]
    values = csv_sum(county_testing, columns=columns)
    tagged.update(map_attributes(values, mapping))

    # PCR encounters and other metrics
    pcr = res[2]
    partial = map_attributes(pcr.sum(), mapping)
    tagged.update(partial)

    # active hosp/icu should not be summed
    hosp = pcr.groupby('Date').sum().filter(like='Active').iloc[-1]
    tagged.update(map_attributes(hosp, mapping))

    return tagged


def handle_ma(res, mapping):
    tagged = {}

    df = MaRawData(res[0])
    tabs = ['DeathsReported (Report Date)', 'Testing2 (Report Date)',
            'Cases (Report Date)', 'Hospitalization from Hospitals']

    for tab in tabs:
        # report the last row
        partial = map_attributes(df[tab].iloc[-1], mapping, 'MA')
        tagged.update(partial)

    # positive pcr
    tests = df['TestingByDate (Test Date)'].filter(like='All Positive')
    tagged[Fields.SPECIMENS_POS.name] = tests.sum()['All Positive Molecular Tests']

    hosp = df['RaceEthnicityLast2Weeks']
    maxdate = hosp['Date'].max()
    tagged[Fields.HOSP.name] = hosp[hosp['Date'] == maxdate].sum()['Ever Hospitaltized']

    # weekly report:
    df = MaRawData(res[0], "Weekly Public Health Report - Raw Data")

    # recovered
    rec = df['Quarantine and Isolation'].sort_values('Date')
    recovered = rec[rec['Status'] == 'Total Cases Released from Isolation'].iloc[-1]['Residents']
    tagged[Fields.RECOVERED.name] = recovered
    antibody = df['Antibody'].sum()
    tagged[Fields.ANTIBODY_TOTAL_PEOPLE.name] = antibody['Total Tests']
    tagged[Fields.ANTIBODY_POS_PEOPLE.name] = antibody['Positive Tests']

    return tagged


def handle_sc(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='SC')
        tagged.update(partial)

    # testing
    df = res[-1]
    df = df.unstack()
    df.index = df.index.map("-".join)
    partial = map_attributes(df, mapping)
    tagged.update(partial)

    return tagged


def handle_tx(res, mapping):
    tagged = {}
    for result in res[:-3]:
        partial = extract_arcgis_attributes(result, mapping, debug_state='TX')
        tagged.update(partial)

    # dashboard totals
    totals = res[-3]
    tagged.update(map_attributes(
        {x['attributes']['TestType']: x['attributes']['Count_'] for x in totals['features']}, mapping, 'TX'))

    # positive pcr
    pcr_pos = res[-2]
    val = sum(pcr_pos['features'][0]['attributes'].values())
    tagged[Fields.SPECIMENS_POS.name] = val

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

    # Downloadable file
    zipurl = res[-1]
    # Sometimes there are files for multiple dates, we need the most recent
    specimens_file_prefix = 'Overview_Total Tests by'
    specimens_file_latest = specimens_file_prefix
    recovered_file = 'Overview_Cumulative COVID-19 Cases'
    recovered_file_latest = recovered_file
    people_tested_file = 'Overview_Number of People Tested by'
    people_tested_latest = people_tested_file
    test_type = ['PCR/amplification', 'Antigen by DFA/IF']
    result = ['POSITIVE', 'NEGATIVE']
    with zipContextManager(zipurl) as zipdir:
        with os.scandir(zipdir) as it:
            for entry in it:
                df = None
                fields = []
                if not entry.is_file:
                    # just in case
                    continue
                if entry.name.startswith(specimens_file_prefix):
                    if entry.name < specimens_file_latest:
                        continue
                    # specimens
                    fields = [Fields.SPECIMENS_POS, Fields.SPECIMENS_NEG,
                              Fields.ANTIGEN_POS, Fields.ANTIGEN_NEG]
                    specimens_file_latest = entry.name
                elif entry.name.startswith(people_tested_file):
                    if entry.name < people_tested_latest:
                        continue
                    # people tested
                    fields = [Fields.CONFIRMED, Fields.NEGATIVE,
                              Fields.ANTIGEN_POS_PEOPLE, Fields.ANTIGEN_NEG_PEOPLE,
                              Fields.TOTAL, Fields.ANTIGEN_TOTAL_PEOPLE]
                    people_tested_latest = entry.name
                elif entry.name.startswith(recovered_file):
                    if entry.name < recovered_file_latest:
                        continue
                    # recoveries
                    fields = [Fields.RECOVERED]
                    recovered_file_latest = entry.name
                if fields and entry.name.startswith(recovered_file):
                    df = pd.read_csv(os.path.join(zipdir, entry.name))
                    last = df['Estimated Recovered *'].iloc[-1]
                    if Fields.RECOVERED in fields:
                        tagged[Fields.RECOVERED.name] = last
                elif fields and not entry.name.startswith(recovered_file):
                    df = pd.read_csv(os.path.join(zipdir, entry.name))
                    summed = df.groupby(['Test Type', 'Result']).sum()
                    i = 0
                    for tt in test_type:
                        for rr in result:
                            tag = fields[i]
                            tag = tag if isinstance(tag, str) else tag.name
                            value = summed.loc[tt, rr]['Count']
                            tagged[tag] = value
                            i += 1
                    # handle totals
                    if Fields.CONFIRMED in fields:
                        tagged[Fields.TOTAL.name] = sum([
                            summed.loc[test_type[0], rr]['Count'] for rr in result])
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


def handle_wa(res, mapping):
    tagged = {}
    for result in res[:1]:
        tagged = extract_arcgis_attributes(result, mapping, 'WA')

    # cases, hosp and death excel
    names = {
        'Cases': Fields.POSITIVE,
        'Hospitalizations': Fields.HOSP}
    for name, field in names.items():
        df = res[1][name]
        tagged[field.name] = df.iloc[-1][name]

    # tests
    df = res[-1].rename(columns=mapping)
    df = df.groupby(df.columns.values, axis=1).sum().sum()
    # df['SPECIMENS'] = df['SPECIMENS_POS'] + df['SPECIMENS_NEG']
    tagged.update(df.filter(like='SPECIMEN').to_dict())

    return tagged


def handle_wi(res, mapping):
    tagged = {}
    for result in res[:-1]:
        partial = extract_arcgis_attributes(result, mapping, 'WI')
        tagged.update(partial)

    # testing encounters
    testing = res[-1]
    encounters = [k for k, v in mapping.items() if v == Fields.PCR_TEST_ENCOUNTERS.name][0]
    value = testing[testing['Measure Names'] == encounters]['Number of Tests'].sum()
    tagged[mapping[encounters]] = value
    return tagged


def handle_wv(res, mapping):
    return map_attributes(res[0].sum(), mapping)
