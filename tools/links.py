''' This file generates an HTML page with links to the data sources
This is only one step over just dumping everything to a txt file, nothing fancy
'''

from jinja2 import Environment, PackageLoader, select_autoescape
import urllib.parse
import sys
import yaml
import os

FILENAME = 'index.html'
# relative path (not great, but it'll suffice for now)
SOURCES_FILE = ['../sources/urls.yaml', 'sources/urls.yaml']

def main():
    for f in SOURCES_FILE:
        if os.path.isfile(f):
            sources = yaml.load(open(f), Loader=yaml.SafeLoader)
    # massage the sources to make them human readable (for the relevant ones)
    for state in sources:
        for query in sources.get(state, []):
            # query is url, params
            url = query.get('url')
            params = query.get('params')
            type = query.get('type')

            link = url
            if params:
                if type == 'arcgis' and 'f' in params and state != 'NE':
                    params.pop('f')
                link = "{}?{}".format(url, urllib.parse.urlencode(params))

            query['url'] = link

    env = Environment(
        loader=PackageLoader('links', 'templates'),
        autoescape=select_autoescape(['html'])
    )
    template = env.get_template('links.html')
    template.stream(sources=sources).dump(open(FILENAME, 'w'))


if __name__ == "__main__":
    print("Version: ", sys.version_info)
    main()
