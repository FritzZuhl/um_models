from datetime import datetime, timedelta
import psycopg2 as pg
import pandas as pd

import logging


def get_days_data(date_id=None, this_connection=None):
    query = """
    select *
      from
        fct_streams fs
       where
        fs.date_id = %s
       and
        fs.streaming_platform_id = 2
    """
    if date_id is None:
        date_id = datetime.date.today().strftime("%Y%m%d")

    query_complete = query % date_id
    df = pd.read_sql_query(query_complete, this_connection)
    if this_connection is not None:
        this_connection.close()

    return df


def get_pastdays_data(days_ago=10, last_date=None, this_connection=None):
    query = """
    select *
      from
        fct_streams fs
       where
        fs.date_id >= %s
        and
        fs.date_id <= %s
       and
        fs.streaming_platform_id = 2
    """

    if last_date is None:
        last_date = datetime.today()

    last_date_id = last_date.strftime("%Y%m%d")
    start_date = last_date - timedelta(days=days_ago)
    start_date_id = start_date.strftime("%Y%m%d")

    query_complete = query % (start_date_id, last_date_id)

    df = pd.read_sql_query(query_complete, this_connection)
    if this_connection is not None:
        this_connection.close()

    return df


def get_superfans_global(df, portion_top=None, min_daily_rate=None):
    """
    The function provides the list of listeners that are superfans.

    :param df: a pd.DataFrame, typically from get_pastdays_data(). The data must have at least
    the listener_id, each row represents one distinct stream. The time span is calculated from the range of the
    date_id stamp.

    :param portion_top: If the top portion is desired, this param should be between 0-1. 0.1 is a good number, which
    would define a superfan as being one of the top 10% of listeners.

    :param min_daily_rate: Enter the minimum daily streaming for each listener if you want to define superfan by the
    the number of daily streams.

    :return: a pandas dataframe that contains listener_id(key), total streams(int), daily average(float),
             is_top(boolean)
    """

    # counts = df.groupby('listener_id')['id'].count()

    counts_n = df['listener_id'].value_counts()
    counts_n = counts_n.sort_values(ascending=False)

    days = df.date_id.max() - df.date_id.min()
    if days == 0:
        days = 1.0

    daily_rate = counts_n.divide(days) # the average number of streams/listener threshold to define superfan.

    counts_n_df = counts_n.to_frame()
    counts_n_df['daily_rate'] = daily_rate

    if (portion_top is None) & (min_daily_rate is None):
        logging.error("either 'portion_top' or 'min_daily_rate' must be defined")
        return

    # for min_daily_rate
    if min_daily_rate is not None:
        is_top = daily_rate >= min_daily_rate

    # for portion_top
    if portion_top is not None:
        n = int(len(counts_n) * portion_top)
        is_top = pd.Series([False] * counts_n)
        is_top.loc[0:n] = True

    counts_n_df['is_top'] = is_top.values

    counts_n_df.columns = ['counts', 'daily_rate', 'is_top']
    counts_n_df['listener_id'] = counts_n_df.index

    return counts_n_df


def get_superfans_bygroup(df, fans_df):
    """
    Returns some statistics for the number of 'superfans' in df, using the listener_id(s) listed in fans_df. This
    function is typically used for the listener base for a particular artist ('artist_id').

    :param df: a pd.Dataframe of streams that has as least listener_id. One line per stream.
    :param fans_df: a pd.Dataframe that lists listener_ids that are considered super-fans as defined by the
    function get_superfans_global().

    :return: a dict that contains KPIs from the streams in df.
    """

    # get distinct list of listener_id(s)
    listeners = df['listener_id'].drop_duplicates()
    distinct_listener_count = len(listeners)

    # get list of listeners that are is_top
    top_listeners = fans_df.loc[fans_df['is_top'] == True, 'listener_id'].copy()

    # flag listeners who are in super_fan list
    top_or_not = listeners.isin(top_listeners)
    counts = top_or_not.value_counts()
    try:
        superfan_count = counts[True]
    except KeyError:
        superfan_count = 0

    try:
        portion = float(superfan_count) / float(distinct_listener_count)
    except ZeroDivisionError:
        portion = 0

    result_dict = {'total_streams': df.__len__(),
                   'distinct_listeners': distinct_listener_count,
                   'count_of_superfans': superfan_count,
                   'portion_of_all_listeners': portion}

    return result_dict


if __name__ == '__main__':
    days_of_streams = 14  # number of days of streaming to get superfan
    streams_per_day = None  # minimum numbers of daily streams/day to define a superfans
    portion_of_all_listeners = 0.9  # portion of all listeners to consider as superfans

    conn = pg.connect(host="127.0.0.1", port=65432, database="insights", user="root", password='bates-lory-cracker')

    x1 = get_pastdays_data(days_ago=days_of_streams, this_connection=conn)
    # x2 = get_superfans_global(x1, portion_top=0.10)
    x2 = get_superfans_global(x1, portion_top=portion_of_all_listeners, min_daily_rate=streams_per_day)

    x1_g = x1.groupby('artist_id')
    artists = x1_g.groups.keys()

    N = len(artists)
    dictlist = [dict() for x in range(N)]
    for i in range(N):
        dictlist[i] = get_superfans_bygroup(x1_g.get_group(artists[i]), x2)
        dictlist[i].update({'artist_id': artists[i]})

    res = pd.DataFrame(dictlist)

    print res
