import logging

from fetcher.utils import extract_attributes, extract_arcgis_attributes
from fetcher.utils import request, request_and_parse, request_csv, \
    request_soup, request_pandas


def fetch_query(state, query):
    # TODO: make a better mapping here
    res = None
    try:
        if query.type in ['arcgis', 'json', 'ckan', 'soda']:
            res = request_and_parse(query.url, query.params)
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
        processed_results = source.extras(results, source.mapping)
    else:
        for i, result in enumerate(results):
            if source.queries[i].type == 'arcgis':
                partial = extract_arcgis_attributes(result, source.mapping, source.name)
            else:
                # This is a guess; getting an unknown top level object
                partial = extract_attributes(
                    result, source.queries[i].data_path, source.mapping, source.name)
            processed_results.append(partial)

    return processed_results


def parse_query_response(state, queries, results, mapping,):
    pass
