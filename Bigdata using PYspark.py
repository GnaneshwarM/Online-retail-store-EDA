# Databricks notebook source
file_location = "/FileStore/tables/6M_0K_99K_users_dataset_public.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option('nanValue', ' ')\
  .option('nullValue', ' ')\
  .load(file_location)

display(df)

# COMMAND ----------

spark

# COMMAND ----------

df.count() ##### count for rest 3 tables also

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Create a view or table

temp_table_name = "c2c"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select distinct seniorityAsMonths from c2c

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct seniority,seniorityAsMonths,seniorityAsYears from c2c

# COMMAND ----------

df.stat.cov('productsListed','productsSold')

# COMMAND ----------

df.stat.corr('productsListed','productsSold')

# COMMAND ----------

df.stat.corr('productsWished','productsBought')

# COMMAND ----------

df.stat.corr('socialProductsLiked','productsBought')

# COMMAND ----------

df.stat.corr('socialProductsLiked','productsSold')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select corr(socialProductsLiked,productsSold) as correlationbetweenlikedandsold from c2c

# COMMAND ----------

df_dropped  = df.drop('')

# COMMAND ----------



# COMMAND ----------

df.stat.crosstab('productsListed','productsBought').show(10)

# COMMAND ----------

ferq_bought = df.stat.freqItems(['identifierHash','daysSinceLastLogin'])

# COMMAND ----------

display(ferq_bought.collect())

# COMMAND ----------

df_users = df.select('country','language','socialNbFollowers','socialNbFollows','socialProductsLiked','productsListed','productsSold','productsWished','productsBought','gender','hasAnyApp','seniorityAsYears','countryCode')

# COMMAND ----------

df_users.groupby('language').count().show()

# COMMAND ----------

df_users.groupby('country').count().show()

# COMMAND ----------

from pyspark.sql.functions import count,col,mean,stddev_pop,min,max,avg
df.groupby('country','productsListed').count().orderBy(col('count').desc()).show()

# COMMAND ----------

quantileprobs = [0.25,0.5,0.75,0.9]
relerror = 0.4
df_users.stat.approxQuantile('productsListed',quantileprobs,relerror)

# COMMAND ----------

quantileprobs = [0.25,0.5,0.75,0.9]
relerror = 0.4
df_users.stat.approxQuantile('productsSold',quantileprobs,relerror)

# COMMAND ----------

quantileprobs = [0.25,0.5,0.75,0.9]
relerror = 0.4
df_users.stat.approxQuantile('productsWished',quantileprobs,relerror)

# COMMAND ----------

quantileprobs = [0.25,0.5,0.75,0.9]
relerror = 0.4
df_users.stat.approxQuantile('productsBought',quantileprobs,relerror)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select productsListed, count(*) from c2c group by productsListed order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select productsSold, count(*) from c2c group by productsSold order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select productsWished, count(*) from c2c group by productsWished order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select productsBought, count(*) from c2c group by productsBought order by 2 desc

# COMMAND ----------

df_users.stat.crosstab('productsListed','productsSold').show()

# COMMAND ----------

df_users.stat.crosstab('productsWished','productsBought').show()

# COMMAND ----------

file_location = "/FileStore/tables/comparison_of_sellers_by_gender_and_country.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df2 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option('nanValue', ' ')\
  .option('nullValue', ' ')\
  .load(file_location)

display(df2)

# COMMAND ----------



# COMMAND ----------

df2.groupby('country').count().show()

# COMMAND ----------


file_location = "/FileStore/tables/countries_with_top_sellers_fashion_c2c.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df3 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option('nanValue', ' ')\
  .option('nullValue', ' ')\
  .load(file_location)

display(df3)


# COMMAND ----------

temp_table_name = "topsellers"

df3.createOrReplaceTempView(temp_table_name)

temp_table_name = "sellers"

df3.createOrReplaceTempView(temp_table_name)



# COMMAND ----------

df_country = df3.select(['country'])
df_country.show()

# COMMAND ----------

# Import packages
import matplotlib.pyplot as plt
def plot_cloud(wordcloud):
    # Set figure size
    plt.figure(figsize=(40, 30))
    # Display image
    plt.imshow(wordcloud) 
    # No axis details
    plt.axis("off");

# COMMAND ----------

df_country=df_country.apply(str)

# COMMAND ----------

!pip install wordcloud

# COMMAND ----------

df_country = df3.select('country')

# COMMAND ----------

!pip install wordcloud

# COMMAND ----------

from wordcloud import WordCloud, STOPWORDS 
import matplotlib.pyplot as plt 
import pandas as pd 

comment_words = '' 
stopwords = set(STOPWORDS) 
# iterate through the csv file 
for val in df_country: 
      
    # typecaste each val to string 
    val = str(val) 
  
    # split the value 
    tokens = val.split() 
      
    # Converts each token into lowercase 
    for i in range(len(tokens)): 
        tokens[i] = tokens[i].lower() 
      
    comment_words += " ".join(tokens)+" "
  
wordcloud = WordCloud(width = 800, height = 800, 
                background_color ='white', 
                stopwords = stopwords, 
                min_font_size = 10).generate(comment_words) 
  
# plot the WordCloud image                        
plot_cloud(wordcloud)

# COMMAND ----------

pd_df=df.toPandas()

# COMMAND ----------

type(pd_df)

# COMMAND ----------

pd_df.describe()

# COMMAND ----------



# Shape of Dataframe
print(pd_df.count(), len(pd_df.columns))


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from c2c_online_store_data 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count (*) from c2c_online_store_data

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct country from c2c limit 25 #### error

# COMMAND ----------

df_sel=df.select('socialNbFollowers','socialProductsLiked','seniorityAsMonths','productsSold','productsWished','daysSinceLastLogin')

# COMMAND ----------

df_sel.describe().show()

# COMMAND ----------

df_sel.cache()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct * from c2c_online_store_data limit 25

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

df.stat.cov('productsWished','productsBought')

# COMMAND ----------

df.stat.corr('socialProductsLiked','productsBought')

# COMMAND ----------

df.stat.corr('seniorityAsMonths','seniorityAsYears')

# COMMAND ----------

df.stat.corr('socialNbFollowers','socialNbFollows')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select corr(socialNbFollowers,socialNbFollows) as correlation from c2c_online_store_data

# COMMAND ----------

df.stat.crosstab('country','productsbought').show()

# COMMAND ----------

df.groupby('country').count().show()

# COMMAND ----------

df.groupby('country').count().orderBy(col('count').desc()).show()

# COMMAND ----------

from pyspark.sql.functions import count,mean,stddev_pop,min,max,avg

# COMMAND ----------

quantileprobs = [0.25,0.5,0.75,0.9]
relerror = 0.05
df.stat.approxQuantile('socialNbFollowers',quantileprobs,relerror)

# COMMAND ----------

quantileprobs = [0.25,0.5,0.75,1.0]
relerror = 0.0
df.stat.approxQuantile('socialNbFollows',quantileprobs,relerror)

# COMMAND ----------

#Null values
from pyspark.sql.functions import isnan, when, count, col
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select country , count(*) from c2c_online_store_data group by country order by 2 desc ## error

# COMMAND ----------

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext



# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(
    inputCols=["gender_numeric"],  
    outputCols=["gender_vector"]
)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer,VectorIndexer,OneHotEncoder,VectorAssembler

from pyspark.sql import SparkSession

sqlContext = SQLContext(sc)


stringIndexer = StringIndexer(inputCol="gender", outputCol="genderIndex")
model = stringIndexer.fit(df)
indexed = model.transform(df)
#encoder = OneHotEncoder(dropLast=False, inputCol="genderIndex", outputCol="genderVec")
#encoded = encoder.transform(indexed)
#encoded.select("genderVec").show()

# COMMAND ----------



# COMMAND ----------

display(indexed
       )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(hasAnyApp), gender
# MAGIC FROM c2c_online_store_data
# MAGIC GROUP BY gender;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(hasAnyApp), countryCode
# MAGIC FROM c2c_online_store_data
# MAGIC GROUP BY countryCode;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(productsBought), Country
# MAGIC FROM c2c_online_store_data
# MAGIC GROUP BY Country
# MAGIC HAVING COUNT(productsBought) > 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(productsBought), Country
# MAGIC FROM c2c_online_store_data
# MAGIC GROUP BY Country
# MAGIC HAVING COUNT(productsBought) < 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Country 

# COMMAND ----------

df.groupby('gender').count().show()

# COMMAND ----------

df.groupby('civilityTitle').count().show()

# COMMAND ----------


df.where(df.language.isin(['de','fr','it'])).show()

# COMMAND ----------

df.filter(df.hasAnyApp=='True').show()

# COMMAND ----------

df.stat.crosstab('hasAnyApp','gender').show()

# COMMAND ----------

df.filter(df.daysSinceLastLogin > 700).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from c2c_online_store_data

# COMMAND ----------

raw_data = spark.table('c2c')
display(raw_data)

# COMMAND ----------

display(raw_data.describe())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select country,sum(productsSold) from c2c group by country

# COMMAND ----------

# MAGIC %sql
# MAGIC select country,sum(socialNbFollowers) from c2c group by country

# COMMAND ----------

from pyspark.sql.functions import isnan,when,count,col,log
display(df.groupby('country').agg((count(col('productsBought'))).alias('count')))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select country,count(productsBought) from c2c where hasAnyApp='False' group by country

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select country_en,count(productsBought) from c2c where hasAnyApp='False' group by country_en

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select civilityTitle,sum(socialProductsLiked) as productsLiked from c2c group by civilityTitle

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(sellers), country
# MAGIC FROM topsellers
# MAGIC GROUP BY country;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(topsellers), country
# MAGIC FROM topsellers
# MAGIC GROUP BY country;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC df3.stat.crosstab('femalesellers','malesellers','country').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select femalesellers, malesellers , sum(totalproductssold) as tot_amt_sellers from sellers group by femalesellers, malesellers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select country, sex , sum(totalproductssold) as tot_amt_sellers from sellers group by country, sex 

# COMMAND ----------


display(df3.select([count(when(isnan(c)|col(c).isNull(),c)).alias(c) for c in df3.columns]))

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select totalProductsSold, sex from sellers 

# COMMAND ----------

from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# for beautiful plots and some types of graphs
import seaborn as sns


# COMMAND ----------

# Import packages
import matplotlib.pyplot as plt
def plot_cloud(wordcloud):
    # Set figure size
    plt.figure(figsize=(40, 30))
    # Display image
    plt.imshow(wordcloud) 
    # No axis details
    plt.axis("off");

# COMMAND ----------

!pip install wordcloud

# COMMAND ----------

import pyspark.mllib

# COMMAND ----------

import seaborn as sns
plt.figure(figsize=(20,10))
plt.subplot(2, 2, 1)
x = df3.totalproductssold
y = df3.femalesellers
ax = sns.lineplot(x, y)

plt.subplot(2, 2, 3)
x= df3.totalproductssold
y= df3.malesellers
ax2 = sns.lineplot(x, y, data=df)

# COMMAND ----------

db1 = df.drop(df.columns[[0,1,2,3,14]], axis = 1)

# COMMAND ----------

cols = ('identifierHash', 'type', 'country', 'language','civilityTITLE')
db1  =df.drop(*cols)
display(db1)

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import lit
from pyspark.sql.functions import exp

indexer = StringIndexer(inputCol="gender", outputCol="gender_new")
indexer2 = StringIndexer(inputCol="countryCode", outputCol="countrycode_new")

pipeline = Pipeline(stages=[indexer,indexer2])
df_r = pipeline.fit(df).transform(df)

df_r.show()


# COMMAND ----------

cols = ('identifierHash','type','country','civilityTitle','language','gender','countryCode','hasAnyApp','hasAndroidApp', 'hasIosApp','hasProfilePicture')
db2  =df_r.drop(*cols)
display(db2)

# COMMAND ----------

db_final=db2.sample(False,0.3)
print((db_final.count(), len(db_final.columns)))



# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, NumericType
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans

features =   ('socialNbFollowers', 'socialNbFollows', 'socialProductsLiked',
       'productsListed', 'productsSold', 'productsPassRate', 'productsWished',
       'productsBought', 'civilityGenderId', 'daysSinceLastLogin', 'seniority',
       'seniorityAsMonths', 'seniorityAsYears', 'gender_new',
       'countrycode_new') 
      
 
 

# COMMAND ----------

assembler = VectorAssembler(inputCols=features,outputCol="features")

dataset=assembler.transform(db_final)
dataset.select("features").show(truncate=False)

# COMMAND ----------


from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))



# COMMAND ----------



# Shows the result.
print("Cluster Centers: ")
ctr=[]
centers = model.clusterCenters()
for center in centers:
    ctr.append(center)
    print(center)

# COMMAND ----------

pandasDF=predictions.toPandas()
centers = pd.DataFrame(ctr,columns=features)
pandasDF

# COMMAND ----------


