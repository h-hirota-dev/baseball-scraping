# test_get_baseball_data.py
import pandas as pd

data = pd.read_html('https://baseball-data.com/19/player/yb/')
print(f"{len(data)} tables found.")
print(data[0].head())
