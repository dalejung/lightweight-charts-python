import polars as pl


def _get_ohlc_changes(df):
    ohlcv = ['open', 'high', 'low', 'close', 'volume']

    change = {}
    for col in df.columns:
        if col.lower() in ohlcv:
            change[col] = col.lower()
    return change


def convert_pl(df: pl.DataFrame, name=None):
    ts_cols = ['time', 'date', 'timestamp']
    change = {}

    # assume this is a candle stick if no name?
    if name is None:
        change = {
            **_get_ohlc_changes(df),
            **change,
        }

    # always grab timestamp column
    for col in df.columns:
        if col.lower() in ts_cols:
            change[col] = 'time'
            break

    # TODO if we have name does this mean we only select that column?
    subset = list(change.values())
    if name and name not in subset:
        subset.append(name)

    ldf = (
        df.lazy()
        .rename(change)
        .select(subset)
    )
    pd_df = ldf.collect().to_pandas()
    return pd_df
