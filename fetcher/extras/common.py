from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import ZipFile
import shutil
from contextlib import contextmanager
import re
import urllib
import urllib.request

@contextmanager
def MaContextManager(res):
    soup = res[0]
    link = soup.find('a', string=re.compile("COVID-19 Raw Data"))
    link_part = link['href']
    url = "https://www.mass.gov{}".format(link_part)
    print("URL = ", url)
    req = urllib.request.Request(url, headers = {'User-Agent': 'Mozilla/5.0'})

    try:
        response = urllib.request.urlopen(req)
        tmpfile = NamedTemporaryFile(delete=True)
        tmpdir = TemporaryDirectory()

        shutil.copyfileobj(response, tmpfile)
        tmpfile.flush()
        shutil.unpack_archive(tmpfile.name, tmpdir.name, format="zip")

        yield tmpdir.name
    finally:
        # close everything
        print("dir name = ", tmpdir.name, "file name = ", tmpfile.name)
        tmpfile.close()
        tmpdir.cleanup()
