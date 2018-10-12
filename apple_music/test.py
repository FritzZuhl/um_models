# Get the data from Big Query
query = """
select
s.anonymized_person_id
,s.apple_identifier
,s.action_type
,cd.age_band
,cd.gender
,cd.streams
from
applemusic_analytics.am_streams s
join
applemusic_analytics.am_contentdemographics cd
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
cast(s.ingest_datestamp as string) = '2018-10-5'
order by 1,2;
"""

import pandas_gbq as gbq

query = " select  anonymized_person_id, ingest_datestamp from applemusic_analytics.am_streams LIMIT 100"

x = gbq.read_gbq(query, project_id='tommy-boy', dialect= 'legacy', verbose=True)

x = gbq.read_gbq(query, project_id='tommy-boy', dialect= 'standard')


