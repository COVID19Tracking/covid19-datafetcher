from fetcher.utils import map_attributes


def collect_attributes(attributes, mapping, field, value, debug_state=None):
    collected = {x['attributes'][field]: x['attributes'][value] for x in attributes}
    return map_attributes(collected, mapping, debug_state)


def handle_ak(res, mapping):
    # collect values
    res = res[0].get('features', [])
    return collect_attributes(res, mapping, 'Race', 'CountCases', 'AK')


def handle_mo(res, mapping):
    # Do a quick pass to collect everything then send it to `map_attributes`
    res = res[0].get('features', [])
    return collect_attributes(res, mapping, 'RACE', 'Frequency', 'MO')
