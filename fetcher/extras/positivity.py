from datetime import datetime
import re


def handle_ky(res, mapping):
    tagged = {}

    # soup time
    soup = res[-1]
    title = soup.find('span', string=re.compile("Positivity Rate"))
    number = title.find_next_sibling()
    tagged['PPR'] = float(number.get_text(strip=True))
    tagged['TIMESTAMP'] = datetime.now().timestamp()

    return tagged
