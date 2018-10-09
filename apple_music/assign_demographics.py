#
#
#
import os
import numpy as np
import pandas as pd
import cPickle as pickle
import sys
import itertools
import random
import collections


# d = pa.read_csv("BQ_data.csv", sep=';')

# with open('BQ_data.plk', 'wb') as output:
#    pickle.dump(d, output, pickle.HIGHEST_PROTOCOL)

with open('data/BQ_data.plk', 'rb') as input:
    d = pickle.load(input)

anonymized_person_id_unique = list(set(d.anonymized_person_id))

# date_id = '2018-9-29'  # type: str
# table_name = 'tommy-boy.FritzZuhl.out_apple_music'
gross_gender = d.groupby('gender').sum().streams
gross_ageBands = d.groupby('age_band').sum().streams

scale_factor = float(len(anonymized_person_id_unique )) / float(sum(d['streams']))

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


assignments = pd.DataFrame(
    {'anonymized_person_id': anonymized_person_id_unique,
     'gender'              : genders3,
     'age_band'            : age_bands3
     }
    )



