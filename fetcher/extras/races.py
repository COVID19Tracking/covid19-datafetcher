from fetcher.utils import map_attributes

def handle_ak(res, mapping):
    # collect values
    res = res[0].get('features', [])
    collected = {x['attributes']['Race']: x['attributes']['CountCases'] for x in res}
    return map_attributes(collected, mapping, 'AK')
