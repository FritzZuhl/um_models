import itertools
import random

import numpy as np
import pandas as pd

import util
import util.bq as bq


def main():
    enfore_date = True
    date_id = '2018-10-11'

    content_query = "select * from `tommy-boy.applemusic_analytics.am_contentdemographics` "

    stream_query = "select * from `tommy-boy.applemusic_analytics.am_streams`  "

    date_restriction = " where ingest_datestamp = '%s' "

    if enfore_date:
        stream_query = (stream_query + date_restriction) % date_id
        content_query = (content_query + date_restriction) % date_id

    today_streams = get_bq_data(stream_query)
    today_content = get_bq_data(content_query)

    res = infer_am_demographics(today_streams, today_content)
    return res


def infer_am_demographics(streams, content):
# def infer_am_demographics(self, datestamp):
    """

    :param streams: this is streaming data as found in bq apple_analytics.am_streams
    :param content: this is content data as found in bq apple_analytics.am_contentdemographics
    :return: a dataframe similar to am_streams, but with age_band and gender indicators
    """

    # The gross_gender and gross_ageBands have the sum the streams by age/gender groups
    # There are lots of redundancies with this stream count. Apple makes it very difficult
    # to make an exact assignment (if not impossible). Instead, for solely the purpose of
    # daily aggregations, the relative portions of age/demo counts will be statistically inferred.

    date = datestamp.strftime('%Y-%m-%d')
    query_streams = """
        select *
        from am_streams
        where 
        am_streams.ds = :stream_ds
     """
    streams = self.hv_session.execute(query_streams, {'stream_ds': date})
    query_content = """
        select *
        from am_content_demographics
        where 
        am_content_demographics = :content_ds
    """
    content = self.hv_session.execute(query_content, {'content_ds': date})

    try:
        anonymized_person_id_unique = list(set(streams.anonymized_person_id))
    except AttributeError:
        print("stream data needs anonymized_person_id")

    try:
        gross_gender = content.groupby('gender').sum().streams
        gross_ageBands = content.groupby('age_band').sum().streams
    except KeyError:
        print("content data must have the fields \"gender\" and \"age_band\" ")

    # Used to reduce the portion counts back to number of distinct listeners.
    scale_factor = float(len(anonymized_person_id_unique)) / float(sum(content['streams']))

    scaled_gender   = np.round(gross_gender * scale_factor).astype(int)
    scaled_ageBands = np.round(gross_ageBands * scale_factor).astype(int)

    age_bands = [[scaled_ageBands.index[t] for i in range(scaled_ageBands.values[t])] for t in
                 xrange(len(scaled_ageBands))]
    age_bands2 = list(itertools.chain(*age_bands))
    age_bands3 = random.sample(age_bands2, len(age_bands2))

    genders = [[scaled_gender.index[t] for i in range(scaled_gender.values[t])] for t in xrange(len(scaled_gender))]
    genders2 = list(itertools.chain(*genders))
    genders3 = random.sample(genders2, len(genders2))

    # due to rounding, it may be necessary to add an item to age_bands3 and genders3
    # repeat last item to list
    while (len(anonymized_person_id_unique) > len(age_bands3)):
        age_bands3.append(age_bands3[len(age_bands3) - 1])

    while (len(anonymized_person_id_unique) > len(genders3)):
        genders3.append(genders3[len(genders3) - 1])

    age_bands3a = age_bands3[0:len(anonymized_person_id_unique)]
    genders3a   = genders3[0:len(anonymized_person_id_unique)]

    # Make table to merge & assign to listeners.
    assignments = pd.DataFrame(
        {'anonymized_person_id': anonymized_person_id_unique,
         'gender': genders3a,
         'age_band': age_bands3a
         }
    )

    result = pd.merge(streams, assignments, on='anonymized_person_id', how="left")
    return result

def get_bq_data(query):
    res = util.bq.query('tommy-boy', 'applemusic_analytics', query, max_results=1000000)
    d = pd.DataFrame(
        data=bq.coerce_rows(
            fields=res['schema']['fields'],
            rows=res['rows']
        ),
        columns=[
            schema_dictionary['name']
            for schema_dictionary in res['schema']['fields']
        ]
    )
    return d


if __name__ == '__main__':
    res = main()
    print(res)
