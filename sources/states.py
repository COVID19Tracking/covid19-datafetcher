'''
This file reads the data by state and returns a data structure the rest of the code should use
Any changes to underlying storage should be contained to this file.
'''

import yaml

URLS_FILE = 'sources/urls.yaml'
MAPPING_FILE = 'sources/mappings.yaml'

def read_sources(filename = URLS_FILE):
    urls = yaml.load(open(filename), Loader=yaml.SafeLoader)
    return urls

def read_mappings(filename = MAPPING_FILE):
    data = yaml.load(open(filename), Loader=yaml.SafeLoader)
    return data

class Sources(object):
    states = None
    mappings = None

    '''Should I read it here? it's nice but requires extras to be complete '''

    def __init__(self):
        pass
