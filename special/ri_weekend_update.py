from datetime import datetime, timedelta, timezone
import hydra
import pandas as pd
import sys

# This is very ugly, I'll fix it later
sys.path.append('.')
from fetcher.sources import build_sources
from fetcher.utils import Fields, request_pandas
from special import internal_client


RI = "RI"


@hydra.main(config_name="ri")
def main(cfg):
    print(cfg.pretty(resolve=True))
    sources = build_sources(
        cfg.dataset.sources_file, cfg.dataset.mapping_file)

    ri_source = sources[RI]
    queries = ri_source.queries
    mapping = ri_source.mapping

    # need to verify the correct day + day of week
    qs = [q for q in queries if q.type == 'pandas']
    if len(qs) > 1:
        print("Don't know which query to choose", [q.get('desc') for q in qs])
        sys.exit(1)

    df = request_pandas(qs[0])
    df = df.rename(columns=mapping)
    df['DATE_INDEX'] = pd.to_datetime(df['DATE'])
    df = df.set_index('DATE_INDEX').sort_index()

    df = df[[v for k, v in mapping.items() if k != '__strptime']]

    # We need te last C days
    # and then, we need to match what would fit Sat-Sun, shifted by 1 day
    df = df.tail(cfg.backfill.skip + cfg.backfill.fill)

    # verify that the dates make sense: we're looking at the most recent day
    yesterday = datetime.now().date() - timedelta(days=1)
    assert df.index[-1].date() == yesterday, \
        "Expecting last date to be yesterday, got %r" % df.index[-1].date()
    assert df.index[-1].day_name() == cfg.backfill.DOW, \
        "Expecting backfill day to be " + cfg.backfill.DOW + ", got " + df.index[-1].day_name()

    # Prepare the request
    if 'POSITIVE' not in df.columns:
        df['POSITIVE'] = df['CONFIRMED']
    if 'STATE' not in df.columns:
        df['STATE'] = RI
    shifted = df['DATE'] = df.index.shift(periods=cfg.backfill.shift, freq='d')
    df['DATE'] = shifted.strftime(cfg.output_date_format)
    df['lastUpdateTime'] = datetime.now(tz=timezone.utc).isoformat()

    print(df)
    # rename
    columns_renames = {k: v.value for k, v in Fields.__members__.items()}
    data = df.rename(columns=columns_renames). \
        head(cfg.backfill.fill). \
        dropna(axis=1). \
        to_dict(orient='records')

    request_content = internal_client.build_edit_request(data, username=cfg.api.username)
    if cfg.api.url:
        internal_client.api_call(
            request_content, url=cfg.api.url, token=cfg.creds.token, staging=cfg.api.staging)


if __name__ == "__main__":
    main()
