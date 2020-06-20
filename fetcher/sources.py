'''
This file reads the data by state and returns a data structure the rest of the code should use
Any changes to underlying storage should be contained to this file.
'''

import importlib
import os
import yaml

def _read_yaml(parent_dir, filename):
    content = yaml.load(open(os.path.join(parent_dir, filename), 'r'), Loader=yaml.SafeLoader)
    return content

def _read_extras(extras_module_name, keys):
    extras = {}
    # Check the extras file and register all extra handling methods
    extras_module = importlib.import_module(extras_module_name)

    extra_format = "handle_{}"
    for key in keys:
        extra_name = extra_format.format(key.lower())
        if hasattr(extras_module, extra_name):
            extras[key] = getattr(extras_module, extra_name)
    return extras

class Sources(object):
    def __init__(self, url_file, mappings_file, extras_module=None):
        ''' Read sources and mappings '''
        self.sources = _read_yaml(".", url_file)
        self.mapping = _read_yaml(".", mappings_file)
        self.extras = {}
        if extras_module:
            self.extras = _read_extras(extras_module, self.keys())

    def keys(self):
        return self.sources.keys()

    def queries_for(self, state):
        return self.sources.get(state)

    def mapping_for(self, state):
        return self.mapping.get(state)

    # TODO: handle extras here too
