from fetcher.utils import map_attributes

def handle_ak(res, mapping):
    # collect values
    res = res[0].get('features', [])
    collected = {x['attributes']['Race']: x['attributes']['CountCases'] for x in res}
    return map_attributes(collected, mapping, 'AK')

def handle_mo(res, mapping):
    # Do a quick pass to collect everything then send it to `map_attributes`
    res = res[0].get('features', [])
    collected = {x['attributes']['RACE']: x['attributes']['Frequency'] for x in res}
    return map_attributes(collected, mapping, 'MO')