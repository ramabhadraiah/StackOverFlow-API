import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, date

current_month = datetime.today().strftime('%Y-%m')

api_url = "https://api.stackexchange.com/2.3/questions"

params = {
    "site": "stackoverflow",
    "fromdate": f"{current_month}-01",
    "todate": f"{current_month}-{date.today().day}",
    "order": "desc",
    "sort": "votes",
    "tagged": "",
    "pagesize": "100"
}

response = requests.get(api_url, params=params)
data = response.json()["items"]

df = pd.DataFrame(data)

# Get the top 10 tags and their frequency counts
tag_counts = df["tags"].explode().value_counts().head(10)

# Create a dictionary with the tag names and their counts
tag_dict = {"tag_name": tag_counts.index, "tag_count": tag_counts.values}
print(tag_dict)

# Create a DataFrame from the tag dictionary
df_tags = pd.DataFrame(tag_dict)
print(df_tags)

engine = create_engine("postgresql://postgresram:postgres@localhost:5432/demo")

df_tags.to_sql("stackoverflow_tags", engine, if_exists="replace", index=False)
