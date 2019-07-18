import pandas as pd
import time
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb, RCOT
from tigramite.pcmci import PCMCI
import random
import pickle
import numpy as np
from typing import List, Dict, Set
from causal_analysis.data_preparation import load_data, subset, localize, input_na, create_tigramite_dataframe
import matplotlib.pyplot as plt

GPDC_15  = pickle.load(open("GPDC_15.pickle", "rb"))
RCOT_15  = pickle.load(open("RCOT_15.pickle", "rb"))

count = 0
shared = 0
comparison_list = list(zip(GPDC_15['link_matrix'].flatten().tolist(),RCOT_15['link_matrix'].flatten().tolist()))

for i,j in comparison_list:
    if i or j:
        count += 1
        if i and j:
            shared += 1
        else:
            pass

print('The percentage of shared links between GPDC and RCOT at complexity 15 is: ' + str(round(shared/count,2)*100) + ' percent')

both_agree = 0
both_disagree = 0
only_GPDC = 0
only_RCOT = 0
count_links = 0
for i,j in comparison_list:
    count_links +=1
    if i and j:
        both_agree += 1
    if i and not j:
        only_GPDC += 1
    if j and not i:
        only_RCOT += 1
    if not i and not j:
        both_disagree +=1
    else:
        pass

my_array= np.asarray([both_agree,both_disagree,only_RCOT,only_GPDC])
my_labels= ['Both agree','Both disagree', 'Only RCOT', 'Only GPDC']

plt.pie(x=my_array,labels=my_labels,autopct='%1.1f%%')
plt.title("GPDC and RCOT conditional independece tests discrepancies")

plt.show()

print('Both agree: ' + str(round(both_agree/count_links,2)*100) + ' Both disagree: '
      + str(round(both_disagree/count_links,2)*100) + ' Only GPDC: ' + str(round(only_GPDC/count_links,2)*100) + ' Only RCOT: '
      + str(round(only_RCOT/count_links,2)*100))



