import pandas as pd

url = "https://nf3.sakura.ne.jp/Pacific/F/t/fp_all_data_vsS.htm"

tables = pd.read_html(url)
df = tables[0]

print("== columns ==", list(df.columns))
print(df.head(3).to_string())
