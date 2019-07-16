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
for i in GPDC_15['link_matrix'].flatten().tolist():
    for j in RCOT_15['link_matrix'].flatten().tolist():
        count += 1
        if i == j:
            shared +=1
        else:
            pass

print(str(round(shared/count)*100))



