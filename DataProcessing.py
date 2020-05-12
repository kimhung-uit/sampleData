import pandas as pd
import numpy as np
from pathlib import Path
import logging
import itertools
from tqdm import tqdm
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(name)s : %(message)s')
import re
import os
import multiprocessing as mp
from matplotlib import style


def addNullValues(nullNumber, leftInstance):
    totallNull = 0
    nullMappingCreated = []
    print("---------- Processing at left Instance ------------")
    indexName = leftInstance.columns.values
    for i in range(nullNumber):

        randomColumns = np.random.choice(indexName)
        randomValues = np.random.choice(leftInstance[randomColumns])
        try:
            if len(leftInstance.query(randomColumns + ' == "' + str(randomValues) + '"').index) > 0:
                leftInstance.loc[leftInstance.query(randomColumns + ' == "' + str(randomValues) + '"').index[
                                     0], randomColumns] = "_N" + str(i)
        except:
            print("Error Adding")
            continue;

        print("Replace {} value {} at column {} by {}".format(1, randomValues, randomColumns,
                                                              "_N" + str(i)))
        nullMappingCreated.append(("_N" + str(i), randomValues))
        totallNull += 1

    print("---------- Total add {} null values ------------".format(totallNull))
    return leftInstance, nullMappingCreated


DATA_PATH = Path('./data/example')
debugMode = 1

leftFilePath = Path.joinpath(DATA_PATH, "20/left/") / "l.csv"
rightFilePath = Path.joinpath(DATA_PATH, "20/right/") / "r.csv"

try:
    leftInstance = pd.read_csv(leftFilePath).dropna()
    rightInstance = pd.read_csv(rightFilePath).dropna()
except:
    logging.critical("Can not read data at {} and {} ".format(leftFilePath, rightFilePath))

if leftInstance.empty or rightInstance.empty:
    logging.critical("The data files are empty at {} and {} ".format(leftFilePath, rightFilePath))

leftInstance = leftInstance.sample(frac=0.1).reset_index(drop=True)
rightInstance = rightInstance.sample(frac=1).reset_index(drop=True)

leftInstance, nullMappingCreated = addNullValues(100, leftInstance)

print(leftInstance.shape)
print(rightInstance.shape)
leftInstance.to_csv(leftFilePath, index=False)
rightInstance.to_csv(rightFilePath, index=False)
pd.DataFrame(nullMappingCreated, columns=['Null', 'value']).to_csv(Path.joinpath(DATA_PATH, "20/") / "ground.csv")
