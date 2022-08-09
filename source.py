from pyspark.sql import SparkSession
import mysql.connector
import pandas as pd
import plotly.express as px

conn = mysql.connector.connect(
  user='root',
  database='sample',
  password='',
  host="localhost",
  port=3306
)
# spark = SparkSession.builder \
  # .master("local[*]") \
  # .appName("Learning_Spark") \
  # .getOrCreate()
# 
# data = spark.read.csv('countries-aggregated.csv', inferSchema=True, header=True)


def deNormalize():
  query = "Select Country, MAX(Confirmed) as Confirmed, MAX(Recovered) as Recovered, MAX(Deaths) as Deaths from Cases GROUP BY Country;"
  pdf = pd.read_sql(query, con=conn)
  return pdf

pdf = deNormalize()
# sdf = spark.createDataFrame(pdf)

fig = px.scatter_geo(pdf, 
  locations='Country',
  locationmode='country names',
  color='Country',
  hover_name='Country',
  hover_data=[pdf['Confirmed'], pdf['Recovered'], pdf['Deaths']],
  size=pdf['Confirmed']*10,
  projection='natural earth',
)
fig.show()
# print(pdf)
# sdf.show()

conn.close()
                                