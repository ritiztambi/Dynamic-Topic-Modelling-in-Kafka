import random
import numpy as np
import pandas as pd

target = "./test_dataset.csv"

random.seed(9001)
df = pd.DataFrame(np.random.randint(0,2,size=(100, 4)), columns=list('ABCD'))
df.to_csv(target, index=False)
