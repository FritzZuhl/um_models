
import util
import util.bq as bq

import numpy as np
import pandas as pd
import cPickle as pickle
import itertools
import random
import pandas_gbq as gbq



project_id = 'tommy-boy'
# df = gbq.read_gbq("select count(*) from FritzZuhl.engagement", project_id=project_id, dialect='legacy')


date_id = '2018-9-29'

# Get the data from Big Query
query_get_daily_data = """
select
s.anonymized_person_id
,s.apple_identifier
,s.action_type
,cd.age_band
,cd.gender
,cd.streams
from
`applemusic_analytics.am_streams` s
join
`applemusic_analytics.am_contentdemographics` cd
on
s.apple_identifier = cd.apple_identifier
and
s.membership_mode = cd.membership_mode
and
s.membership_type = cd.membership_type
and
s.ingest_datestamp = cd.ingest_datestamp
and
s.storefront_name = cd.storefront_name
and
s.action_type = cd.action_type
and
s.datestamp = cd.datestamp
where
s.ingest_datestamp = '%s'
order by 1,2;
"""

query_get_daily_data = query_get_daily_data % date_id
res = util.bq.query('tommy-boy', 'applemusic_analytics',  query_get_daily_data, max_results=1000000)
d = pd.DataFrame(
    data = bq.coerce_rows(
        fields = res['schema']['fields'],
        rows = res['rows']
    ),
    columns = [
        schema_dictionary['name']
        for schema_dictionary in res['schema']['fields']
    ]
)


# input_file = 'data/BQ_data.plk'
# try:
#     with open(input_file, 'rb') as input:
#     d = pickle.load(input)
# except IOError:
#     print("cannot open %s" % (input_file))


buf = """select * from `tommy-boy.applemusic_analytics.am_streams` 
                        where ingest_datestamp = '%s';"""

get_streaming_data = buf % date_id
res = bq.query('tommy-boy', 'applemusic_analytics', get_streaming_data, max_results=1000000)
streams = pd.DataFrame(
    data = bq.coerce_rows(
        fields = res['schema']['fields'],
        rows = res['rows']
    ),
    columns = [
        schema_dictionary['name']
        for schema_dictionary in res['schema']['fields']
    ]
)



# extract list of distinct listeners
try:
    anonymized_person_id_unique = list(set(d.anonymized_person_id))
except AttributeError:
    print("data needs anonymized_person_id")



try:
    gross_gender = d.groupby('gender').sum().streams
    gross_ageBands = d.groupby('age_band').sum().streams
except KeyError:
    print('data file "%s" must have the fields "gender" and "age_band" ' % input_file)

# The gross_gender and gross_ageBands have the sum the streams by age/gender groups
# There are lots of redundancies with this stream count. Apple makes it very difficult
# to make an exact assignment (if not impossible). Instead, for solely the purpose of
# daily aggregations, the relative portions of age/demo counts will be statistically inferred.
#


# Used to reduce the portion counts back to number of distinct listeners.
scale_factor = float(len(anonymized_person_id_unique )) / float(sum(d['streams']))

#
scaled_gender = np.round(gross_gender * scale_factor).astype(int)
scaled_ageBands = np.round(gross_ageBands * scale_factor).astype(int)

age_bands = [[scaled_ageBands.index[t] for i in range(scaled_ageBands.values[t])] for t in xrange(len(scaled_ageBands))]
age_bands2 = list(itertools.chain(*age_bands))
age_bands3 = random.sample(age_bands2, len(age_bands2))

genders = [[scaled_gender.index[t] for i in range(scaled_gender.values[t])] for t in xrange(len(scaled_gender))]
genders2 = list(itertools.chain(*genders))
genders3 = random.sample(genders2, len(genders2))

# due to rounding, it may be necessary to add an item to age_bands3 and genders3
# repeat last item to list
while ( len(anonymized_person_id_unique) > len(age_bands3)):
    age_bands3.append(age_bands3[len(age_bands3)- 1])

while ( len(anonymized_person_id_unique) > len(genders3)):
    genders3.append(genders3[len(genders3)      - 1])

# Make table to merge & assign to listeners.
assignments = pd.DataFrame(
            {'anonymized_person_id': anonymized_person_id_unique,
             'gender'              : genders3,
             'age_band'            : age_bands3
            }
            )

result = pd.merge(streams, assignments, on='anonymized_person_id', how="left")




# check result
grp = result.groupby('anonymized_person_id')



