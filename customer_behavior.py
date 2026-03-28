import pandas as pd


df = pd.read_csv(r"D:\Data Engineer\Console data\customer_shopping_behavior.csv")

df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))

#print(df.isnull().values.any())

df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ','_')
df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})
#print(df.columns)

#creating new column
labels = ['Young_adult','Adult','Middle_aged','Senior']
df['age_group'] = pd.qcut(df['age'],q=4,labels=labels)
#print(df[['age','age_group']].head())

#creating new column
frequency_mapping = {
    'Fortnightly':14,
    'Weekly':7,
    'Monthly':30,
    'Quarterly':90,
    'Bi-Weekly':14,
    'Annualy':365,
    'Every 3 months':90,
}
df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)
#print(df[['purchase_frequency_days','frequency_of_purchases']].head())

#print(df[['discount_applied','promo_code_used']].head())
df  = df.drop('promo_code_used',axis=1)
#print(df.columns)

import urllib
from sqlalchemy import create_engine

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=PERFECT_7\\SQLEXPRESS;"
    "DATABASE=customer_behavior;"
    "Trusted_Connection=yes;"
)

params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

#Load the dataframe into the SQL Database
# 'if_exists' can be: 'replace' (drops & recreates table) or 'append' (adds to existing)
df.to_sql(
    name='customer_data',
    con=engine,
    if_exists='replace',
    index=False
)

print(f"Success! Data successfully loaded into the customer_data table.")

