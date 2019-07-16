import pandas as pd
import time
from tigramite.independence_tests import ParCorr, GPDC, CMIknn, CMIsymb, RCOT
from tigramite.pcmci import PCMCI
import random
import pickle
import numpy as np
from typing import List, Dict, Set
from causal_analysis.data_preparation import load_data, subset, localize, input_na, create_tigramite_dataframe

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



