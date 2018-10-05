#!/usr/bin/env python2
'''
*** SUPERFAN ***
Retrieves restricted streaming data by listener, runs restrictions & HDBSCAN to label superfans

--- Restrictions are defined below ---
-- has listened to the artist for at least 4 weeks
-- has listened to the artist within the last 4 weeks
-- has listened to the artist at least 50% of the weeks since first listening
-- has listened to the artist for at least 14 days all time
-- has listened to the artist on average for at last 7 days per month

--- Cluster dimensions are listed below ---
-- average streams for artist per month
-- average days listened to artist per month
-- share of weeks listened to artist since first listening
-- average number of days between listening to artist
-- total streams for artist

Inserts count of superfans per artist and loads data into artist_model_score
'''

import gc
import logging
import multiprocessing

import hdbscan
import pandas as pd

import db
import util.bq as bq

# set global variables related to bq limits
max_unsigned_integer = 2147483647
page_size = 10000
max_results = max_unsigned_integer

def bq_generate_df(project_id, dataset_id, query, page_size = page_size, max_results = max_results):
    function_name = bq_generate_df.__name__
    # check if max_results value is valid
    if max_results > max_unsigned_integer:
        log_string = '{}: ("max_results": {}) too large, value must be < {}'
        logging.error(log_string.format(function_name, max_results, max_unsigned_integer))
    # check for number of rows to download, error out if invalid query
    try:
        total_rows = int(bq.query(project_id, dataset_id, query, max_results)['totalRows'])
    except:
        logging.error('{}: invalid query'.format(function_name))
        return
    log_string = '{}: total rows to download - {}'
    logging.info(log_string.format(function_name = function_name, total_rows = total_rows))
    # download data for each limit & offset combination for complete result
    df = pd.DataFrame()
    for limit, offset in bq.generate_limit_and_offset(total_rows, page_size = page_size):
        loop_query = query + ' limit {} offset {}'.format(limit, offset)
        logging.info('{}: running query - "{}"'.format(function_name, loop_query))
        # retrieve data from bigquery
        result_dictionary = bq.query(
            project_id = project_id,
            dataset_id = dataset_id,
            query = loop_query,
            max_results = max_results
        )
        if len(result_dictionary['rows']) < limit:
            log_string = '{}: incomplete result, data size over limit'
            logging.error(log_string.format(function_name = function_name))
            break
        # convert result dictionary to dataframe & assign correct column order
        loop_df = pd.DataFrame(
            data = bq.coerce_rows(
                fields = result_dictionary['schema']['fields'],
                rows = result_dictionary['rows']
            ),
            columns = [
                schema_dictionary['name']
                for schema_dictionary in result_dictionary['schema']['fields']
            ]
        )
        # combine data
        df = pd.concat(objs = [df, loop_df], axis = 0, ignore_index = True)
        row_share = round((100.0 * (limit + offset) / total_rows), 2)
        row_share_string = '{:.2f}'.format(row_share) + '%'
        logging.info('{}: {} complete'.format(function_name, row_share_string))
        # clear memory
        gc.collect()
    df.reset_index(inplace = True, drop = True)
    return df


def bq_download_df(project_id, dataset_id, table_id, page_size = page_size, max_results = max_results):
    # construct select * query
    query = 'select * from `{}.{}.{}`'.format(project_id, dataset_id, table_id)
    # return dataframe from bq_generate_df()
    return bq_generate_df(project_id, dataset_id, query, page_size, max_results)


def select_optimal_cluster(cluster_df, cluster_column_dictionary):
    # subset to cluster metric features & get avg per cluster
    mean_df = cluster_df[cluster_column_dictionary.keys() + ['cluster']].groupby('cluster').mean()
    mean_df = mean_df.reset_index()
    rank_df = pd.DataFrame(mean_df['cluster'].copy())
    # rank clusters by metric avg
    for column, info in cluster_column_dictionary.iteritems():
        rank_df[column + '_rank'] = mean_df[column].rank(
            axis = 0,
            ascending = info['ascending']
        )
    rank_df = rank_df.set_index('cluster').unstack().reset_index()
    rank_df.columns = ['metric', 'cluster', 'rank']
    # define optimal cluster as highest avg rank
    optimal_cluster = rank_df.groupby('cluster').mean()['rank'].sort_values().index[0]
    return optimal_cluster

def select_optimal_min_cluster_size(cluster_df, cluster_column_dictionary, min_cluster_size_list):
    grid_search_columns = [
        'min_cluster_size',
        'is_optimal_cluster',
        'cluster_size',
        'metric_name',
        'metric_value'
    ]
    grid_search_df = pd.DataFrame(columns = grid_search_columns)
    for min_cluster_size in min_cluster_size_list:
        # initialize and fit cluster
        hdbscan_cluster = hdbscan.HDBSCAN(
            min_cluster_size = min_cluster_size,
            core_dist_n_jobs = multiprocessing.cpu_count()
        )
        logging.info('cluster_size: {}'.format(min_cluster_size))
        hdbscan_cluster.fit_predict(cluster_df[cluster_column_dictionary.keys()])
        # add cluster to df and select optimal cluster
        cluster_df['cluster'] = hdbscan_cluster.labels_
        cluster_df['cluster'] = cluster_df['cluster'].astype(str)
        optimal_cluster = select_optimal_cluster(cluster_df, cluster_column_dictionary)
        cluster_df['is_optimal_cluster'] = [
            True if cluster_value == optimal_cluster else False
            for cluster_value in cluster_df['cluster']
        ]
        # generate metric averages for optimal cluster & remaining data points
        grid_search_insert_df = cluster_df[
            cluster_column_dictionary.keys() + ['is_optimal_cluster']
        ].groupby('is_optimal_cluster').mean().T.unstack().reset_index()
        grid_search_insert_df.columns = ['is_optimal_cluster', 'metric_name', 'metric_value']
        grid_search_insert_df['min_cluster_size'] = min_cluster_size
        cluster_size_df = cluster_df.groupby('is_optimal_cluster').count()['listener_id'].reset_index()
        cluster_size_df.columns = ['is_optimal_cluster', 'cluster_size']
        # combine metric averages & cluster size
        grid_search_insert_df = pd.merge(
            left = grid_search_insert_df,
            right = cluster_size_df,
            on = ['is_optimal_cluster']
        )
        grid_search_df = pd.concat(
            [grid_search_df, grid_search_insert_df],
            axis = 0,
            ignore_index = True
        )
    # generate min cluster size comparison from grid search
    comparison_column_list = ['cluster_size', 'metric_name', 'metric_value', 'min_cluster_size']
    optimal_grid_search_df = grid_search_df[grid_search_df['is_optimal_cluster'] is True].copy()
    optimal_grid_search_df = optimal_grid_search_df[comparison_column_list]
    optimal_grid_search_df.columns = [
        'optimal_cluster_size',
        'metric_name',
        'optimal_metric_value',
        'min_cluster_size'
    ]
    nonoptimal_grid_search_df = grid_search_df[grid_search_df['is_optimal_cluster'] is False].copy()
    nonoptimal_grid_search_df = nonoptimal_grid_search_df[comparison_column_list]
    nonoptimal_grid_search_df.columns = [
        'nonoptimal_cluster_size',
        'metric_name',
        'nonoptimal_metric_value',
        'min_cluster_size'
    ]
    # join optimal & nonoptimal metric averages
    comparison_df = pd.merge(
        left = optimal_grid_search_df,
        right = nonoptimal_grid_search_df,
        on = ['metric_name', 'min_cluster_size']
    )
    comparison_column_list = [
        'min_cluster_size',
        'optimal_cluster_size',
        'nonoptimal_cluster_size',
        'metric_name',
        'optimal_metric_value',
        'nonoptimal_metric_value'
    ]
    comparison_df = comparison_df[comparison_column_list]
    # restrict clusters based on share of listeners
    comparison_df = comparison_df[
        comparison_df['min_cluster_size'].isin(
            comparison_df[comparison_df['optimal_cluster_size'].between(
                len(cluster_df) * 0.20,
                len(cluster_df) * 0.80
            )]['min_cluster_size'].drop_duplicates().tolist()
        )
    ]
    # calculate metric growth & enforce polarity
    comparison_df['metric_growth'] = (
        1.0 * (comparison_df['optimal_metric_value'] - comparison_df['nonoptimal_metric_value']) /
        comparison_df['nonoptimal_metric_value']
    )
    negative_polarity_metric_list = [
        column for column, info in cluster_column_dictionary.iteritems()
        if info['polarity'] == -1
    ]
    comparison_df['metric_polarity'] = [
        -1 if metric_name in negative_polarity_metric_list else 1
        for metric_name in comparison_df['metric_name']
    ]
    comparison_df['metric_growth'] = (
        1.0 * comparison_df['metric_growth'] *
        comparison_df['metric_polarity']
    )
    # generate statistics for min/max scaling
    scaling_df = pd.concat(
        data = [
            comparison_df.groupby('metric_name').min()['metric_growth'],
            comparison_df.groupby('metric_name').max()['metric_growth']
        ],
        axis = 1
    )
    scaling_df.columns = ['min_metric_growth', 'max_metric_growth']
    scaling_df.reset_index(inplace = True)
    # add min/max statistics for min/max scaling
    evaluation_df = pd.merge(
        left = comparison_df,
        right = scaling_df,
        on = ['metric_name']
    )
    evaluation_df['metric_index'] = (
        1.0 * (evaluation_df['metric_growth'] - evaluation_df['min_metric_growth']) /
        (evaluation_df['max_metric_growth'] - evaluation_df['min_metric_growth'])
    )
    # calculate score
    evaluation_df['metric_score'] = (
        1.0 * evaluation_df['optimal_cluster_size'] *
        evaluation_df['metric_index']
    ).astype(float)
    try:
        optimal_min_cluster_size = int(
            evaluation_df.groupby('min_cluster_size').sum().sort_values(
                'metric_score', ascending = False
            ).index[0]
        )
    except IndexError:
        optimal_min_cluster_size = 10
    return comparison_df, evaluation_df, optimal_min_cluster_size


def main():
    # set bq parameters
    project_id = 'tommy-boy'
    dataset_id = 'insights'
    data_dictionary = {
        'sfs_month_statistics': pd.DataFrame(),
        'sfs_week_statistics': pd.DataFrame(),
        'sfs_day_statistics': pd.DataFrame(),
        'sfs_week_elapsed': pd.DataFrame()
    }
    #  download from bq
    for table_name, info in data_dictionary.iteritems():
            data_dictionary[table_name] = bq_download_df(project_id, dataset_id, table_name)
    # construct superfan score dataset
    month_statistics_df = data_dictionary['sfs_month_statistics']
    day_statistics_df = data_dictionary['sfs_day_statistics']
    week_elapsed_df = data_dictionary['sfs_week_elapsed']
    # monthly statistics
    month_df = month_statistics_df.copy()
    month_columns = [
        column for column in month_df.columns
        if ('min' not in column) &
           ('max' not in column) &
           ('stddev' not in column) &
           ('tracks' not in column)
    ]
    month_df = month_df[month_columns]
    month_df.columns = [
        column + '_month' if column not in ['artist_id', 'artist_name', 'listener_id'] else column
        for column in month_df.columns
    ]
    # weeks listened share
    elapsed_column_list = ['artist_id', 'artist_name', 'listener_id', 'weeks_listened_share']
    elapsed_df = week_elapsed_df[elapsed_column_list]
    # day statistics
    day_column_list = [
        'artist_id',
        'artist_name',
        'listener_id',
        'stream_window_avg',
        'stream_window_sum'
    ]
    day_df = day_statistics_df[day_column_list]
    # merge dataframes into one
    base_df = pd.merge(
        left = month_df,
        right = elapsed_df,
        on = ['artist_id', 'artist_name', 'listener_id'],
        how = 'outer'
    )
    base_df = pd.merge(
        left = base_df,
        right = day_df,
        on = ['artist_id', 'artist_name', 'listener_id'],
        how = 'outer'
    )
    # add duration at each time level
    cluster_column_dictionary = {
        'streams_avg_month': {
            'ascending': False,
            'polarity': 1
        },
        'days_avg_month': {
            'ascending': False,
            'polarity': 1
        },
        'weeks_listened_share': {
            'ascending': False,
            'polarity': 1
        },
        'stream_window_avg': {
            'ascending': True,
            'polarity': -1
        },
        'streams_sum_month': {
            'ascending': False,
            'polarity': 1
        }
    }
    min_cluster_size_list = xrange(10, 210, 5)
    artist_list = list(base_df['artist_name'].drop_duplicates())
    artist_cluster_dictionary = {
        artist_name: {
            'comparison_df': pd.DataFrame(),
            'evaluation_df': pd.DataFrame(),
            'optimal_min_cluster_size': int(),
            'superfan_count': int()
        }
        for artist_name in artist_list
    }
    for artist_name in artist_list:
        cluster_df = base_df[base_df['artist_name'] == artist_name]
        # enforce metric engagement restriction
        cluster_df = cluster_df[
            (cluster_df['weeks_listened_share'] >= 0.50) &
            (cluster_df['days_sum_month'] >= 14) &
            (cluster_df['days_avg_month'] >= 7)
        ]
        if cluster_df.shape[0] <= 205:
            logging.info('{}: {} has too few listeners'.format(__name__, artist_name))
            artist_cluster_dictionary[artist_name]['superfan_count'] = cluster_df.shape[0]
            pass
        else:
            comparison_df, evaluation_df, optimal_min_cluster_size = select_optimal_min_cluster_size(
                cluster_df,
                cluster_column_dictionary,
                min_cluster_size_list
            )
            artist_cluster_dictionary[artist_name]['comparison_df'] = comparison_df
            artist_cluster_dictionary[artist_name]['evaluation_df'] = evaluation_df
            artist_cluster_dictionary[artist_name]['optimal_min_cluster_size'] = optimal_min_cluster_size
            hdbscan_cluster = hdbscan.HDBSCAN(
                min_cluster_size = optimal_min_cluster_size,
                core_dist_n_jobs = multiprocessing.cpu_count()
            )
            hdbscan_cluster.fit_predict(cluster_df[cluster_column_dictionary.keys()])
            cluster_df['cluster'] = hdbscan_cluster.labels_
            cluster_df['cluster'] = cluster_df['cluster'].astype(str)
            artist_cluster_dictionary[artist_name]['superfan_count'] = cluster_df[
                cluster_df['cluster'] == select_optimal_cluster(cluster_df, cluster_column_dictionary)
            ].shape[0]
    # retrieve superfan count & prepare for loading
    superfan_record_list = [
        (artist_name, info['superfan_count'])
        for artist_name, info in artist_cluster_dictionary.iteritems()
    ]
    superfan_df = pd.DataFrame(
        data = superfan_record_list,
        columns = ['artist_name', 'superfan_count']
    )
    superfan_df = pd.merge(
        left = superfan_df,
        right = base_df[['artist_name', 'artist_id']].drop_duplicates(),
        on = ['artist_name']
    )
    superfan_df = superfan_df[['artist_id', 'superfan_count']]
    connection = db.get_insights_connection()
    cursor = connection.cursor()
    cursor.execute('select id, um_id from dim_artists')
    artist_mapping_df = pd.DataFrame(cursor.fetchall())
    superfan_df = pd.merge(
        left = superfan_df,
        right = artist_mapping_df,
        left_on = 'artist_id',
        right_on = 'um_id'
    )
    superfan_df = superfan_df[['id', 'superfan_count']]
    superfan_df.columns = ['artist_id', 'superfan_count']
    for index, row in superfan_df.iterrows():
        cursor.execute(
            '''
            insert into artist_model_score (artist_id, model_name, model_score)
            values (%s, %s, %s)
            '''
            (row['artist_id'], 'superfan_count', row['superfan_count'])
        )
    # commit inserts & close connection
    connection.commit()
    cursor.close()
    connection.close()


main()
