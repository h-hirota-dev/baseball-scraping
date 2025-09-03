# test_get_baseball_data.py
import pandas as pd

data = pd.read_html('https://baseball-data.com/stats/hitter-f/')
print(f"{len(data)} tables found.")
print(data[0].head())
