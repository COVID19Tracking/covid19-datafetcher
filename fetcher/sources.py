'''
This file reads the data by state and returns a data structure the rest of the code should use
Any changes to underlying storage should be contained to this file.
'''

from dataclasses import dataclass, field

import importlib
import os
from typing import List, Dict, Any
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


def build_sources(url_file, mappings_file, extras_module=None):
    sources_raw = _read_yaml(".", url_file)
    mappings = _read_yaml(".", mappings_file)
    extras = {}
    if extras_module:
        extras = _read_extras(extras_module, sources_raw.keys())

    sources = {}
    for state, queries in sources_raw.items():
        state_queries = []
        for q in queries:
            # need to rename "type" to 'query_type'
            q['query_type'] = q.pop('type')
            query = Query(**q)
            state_queries.append(query)

        extras_func = extras.get(state)
        source = Source(state, state_queries, mapping=mappings.get(state, {}), extras=extras_func)
        sources[state] = source
    return sources


@dataclass
class Query:
    url: str
    query_type: str
    params: dict = field(default_factory=list)
    method: str = None
    data_path: list = field(default_factory=list)
    constants: dict = field(default_factory=dict)
    header: bool = True  # Remove this, used only once, and maybe should be stripped
    encoding: str = None
    desc: str = ""

    @property
    def type(self):
        return self.query_type


@dataclass
class Source:
    # equivalent to "state"
    name: str
    queries: List[Query]
    mapping: Dict[str, str]
    extras: Any = None  # function
