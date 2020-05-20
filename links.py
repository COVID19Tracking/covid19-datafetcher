''' This file generates an HTML page with links to the data sources
This is only one step over just dumping everything to a txt file, nothing fancy
'''

from jinja2 import Environment, PackageLoader, select_autoescape
from sources import states
import sys
import urllib.parse


FILENAME = 'index.html'

def main():
    sources = states.read_sources()
    # massage the sources to make them human readable (for the relevant ones)
    for state in sources:
        for query in sources[state]:
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
    #res = template.rended(sources = sources)
    template.stream(sources=sources).dump(open(FILENAME, 'w'))


if __name__ == "__main__":
    print("Version: ", sys.version_info)
    main()
