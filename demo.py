import pandas as pd
train = pd.read_parquet('filename.parquet')
train.to_csv('filename.csv')