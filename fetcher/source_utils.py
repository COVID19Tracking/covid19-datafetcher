from datetime import datetime, date
import inspect
import logging
import typing

from fetcher.utils import Fields, request, request_and_parse, request_csv, request_soup, \
    request_pandas, request_tableau_scraper, extract_attributes, extract_arcgis_attributes


MS_FILTER = datetime(2020, 1, 1, 0, 0).timestamp() * 1000
# Indices
TS = 'TIMESTAMP'
STATE = Fields.STATE.name


def fetch_query(state, query):
    # TODO: make a better mapping here
    res = None
    try:
        if query.type in ['arcgis', 'json', 'ckan', 'soda']:
            res = request_and_parse(query.url, query.params, query.method)
        elif query.type in ['csv']:
            res = request_csv(
                query.url, query.params,
                header=query.header, encoding=query.encoding)
        elif query.type in ['html']:
            res = request(query.url, query.params, query.encoding)
        elif query.type in ['html:soup']:
            res = request_soup(query.url, query.params, query.encoding)
        elif query.type in ['pandas', 'xls', 'xlsx']:
            res = request_pandas(query)
        elif query.type.lower() in ['tableau']:
            # The thing I tried so hard to avoid
            res = request_tableau_scraper(query)
        else:
            # the default is to send the URL as is
            # TODO: It's used for something, but it's not great
            res = query.url
    except Exception:
        logging.error("{}: Failed to fetch {}".format(state, query.url), exc_info=True)
        raise

    return res


def fetch_source(source):
    results = []

    for query in source.queries:
        res = fetch_query(source.name, query)
        results.append(res)
    return results


def process_source_responses(source, results):
    processed_results = []
    if source.extras:
        # passing queries or not?
        if len(inspect.signature(source.extras).parameters) > 2:
            processed_results = source.extras(results, source.mapping, source.queries)
        else:
            processed_results = source.extras(results, source.mapping)
    else:
        for i, result in enumerate(results):
            query = source.queries[i]
            if query.type == 'arcgis':
                partial = extract_arcgis_attributes(result, source.mapping, source.name)
            else:
                # This is a guess; getting an unknown top level object
                partial = extract_attributes(
                    result, query.data_path, source.mapping, source.name)
            processed_results.append(partial)

    data = _aggregate_state_results(source, processed_results)
    return data


def _update_constants(data, updates):
    for k, v in updates.items():
        if isinstance(v, str) and v.startswith("$"):
            copy_key = v[1:]
            target_key = k
            data[target_key] = data.get(copy_key)
        else:
            data[k] = v


def _aggregate_state_results(source, results):
    '''
    This function handles all the results (post-processing) from
    all queries to a single state.
    Result is always a flat list of dictionary records
    '''

    # special casing here for extras handling
    if isinstance(results, typing.Dict):
        # does it ever happen??
        results = [results]

    mapping = source.mapping
    state = source.name
    data = []

    # all the stuff we need to append to the results
    timestamp = datetime.now()
    for i, x in enumerate(results):
        constants = source.queries[i].constants if not source.extras else {}
        if constants is None:
            constants = {}
        if isinstance(x, typing.Dict):
            _tag_and_timestamp(state, x, timestamp, mapping.get('__strptime'))
            _update_constants(x, constants)
            data.append(x)
        elif isinstance(x, typing.List):
            for record in x:
                _tag_and_timestamp(state, record, timestamp, mapping.get('__strptime'))
                _update_constants(record, constants)
                data.append(record)
        else:
            # should not happen
            logging.warning("Unexpected type in results: %r", x)

    return data


def _tag_and_timestamp(state, data, timestamp, dateformat=None):
    data[Fields.FETCH_TIMESTAMP.name] = timestamp
    data[STATE] = state

    # we should also make sure that the timestamp field is datetime format
    # or parse the Date field
    if TS in data and data[TS]:
        # Check whether it's s or ms and convert to datetime
        ts = data[TS]
        if not isinstance(ts, datetime) or not isinstance(ts, date):
            data[TS] = datetime.fromtimestamp(ts/1000 if ts > MS_FILTER else ts)
    elif 'DATE' in data and data['DATE'] and dateformat:
        d = data['DATE']
        data[TS] = d if isinstance(d, datetime) else datetime.strptime(d, dateformat)
    else:
        # TODO: Should I add now time?
        pass
