from tempfile import NamedTemporaryFile, TemporaryDirectory
import shutil
from contextlib import contextmanager
import re
import urllib
import urllib.request


@contextmanager
def MaContextManager(res, link_text='COVID-19 Raw Data', file_type='zip'):
    soup = res
    link = soup.find('a', string=re.compile(link_text))
    link_part = link['href']
    url = "https://www.mass.gov{}".format(link_part)
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    try:
        with urllib.request.urlopen(req) as response:
            tmpfile = NamedTemporaryFile(delete=True)
            tmpdir = TemporaryDirectory()

            shutil.copyfileobj(response, tmpfile)
            tmpfile.flush()
            if file_type == 'zip':
                shutil.unpack_archive(tmpfile.name, tmpdir.name, format="zip")
                yield tmpdir.name
            else:
                yield tmpfile.name
    finally:
        # close everything, it should also unlink the dirs to be deleted
        tmpfile.close()
        tmpdir.cleanup()
