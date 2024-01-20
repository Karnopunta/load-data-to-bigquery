import pandas as pd

# pd.set_option('display.max_columns', None)

df = pd.read_json('data-source/course.json')

# print(df.head(1))
print(df)

