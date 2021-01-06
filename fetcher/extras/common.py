from tempfile import NamedTemporaryFile, TemporaryDirectory
from io import BytesIO
import shutil
from contextlib import contextmanager
import pandas as pd
import re
import urllib
import urllib.request


@contextmanager
def zipContextManager(file_url, archive_type='zip'):
    req = urllib.request.Request(file_url, headers={'User-Agent': 'Mozilla/5.0'})

    try:
        with urllib.request.urlopen(req) as response:
            tmpfile = NamedTemporaryFile(delete=True)
            tmpdir = TemporaryDirectory()

            shutil.copyfileobj(response, tmpfile)
            tmpfile.flush()
            if archive_type:
                shutil.unpack_archive(tmpfile.name, tmpdir.name, format=archive_type)
                yield tmpdir.name
            else:
                yield tmpfile.name
    finally:
        # close everything, it should also unlink the dirs to be deleted
        tmpfile.close()
        tmpdir.cleanup()


def MaRawData(res, link_text='COVID-19 Raw Data'):
    soup = res
    link = soup.find('a', string=re.compile(link_text))
    link_part = link['href']
    url = "https://www.mass.gov{}".format(link_part)

    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    res = urllib.request.urlopen(req)
    df = pd.read_excel(BytesIO(res.read()), sheet_name=None)
    return df
