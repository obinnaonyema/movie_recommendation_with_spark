# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Getting the Data

# COMMAND ----------

ratings_filename = "dbfs:/mnt/Files/Validated/ratings.csv"
movies_filename = "dbfs:/mnt/Files/Validated/movies.csv"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Files/Validated

# COMMAND ----------

# MAGIC %md
# MAGIC # Brief Analysis
# MAGIC 
# MAGIC We will create 2 dataframes for our analysis which will make the visualization with Databricks display function pretty straightforward.
# MAGIC 1. movies_based_on_time: we will drop the genres here. The final schema will be (movie_id, name, year)
# MAGIC 2. movies_based_on_genres: final schema will look like (movie_id, name_with_year, one_genre)

# COMMAND ----------

from pyspark.sql.types import *

movies_with_genres_df_schema = StructType([StructField('ID', IntegerType()),
                                          StructField('title',StringType()),
                                          StructField('genres',StringType())]
                                         )

movies_df_schema = StructType([StructField('ID',IntegerType()),
                              StructField('title',StringType())]
                             )


# COMMAND ----------

#creating the dataframes
movies_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferSchema=False)\
    .schema(movies_df_schema).load(movies_filename)

movies_with_genres_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferSchema=False)\
    .schema(movies_with_genres_df_schema).load(movies_filename)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Inspecting dataframes before transformation

# COMMAND ----------

movies_df.show(4, truncate=False) # will be used for collaborative filtering
movies_with_genres_df.show(4,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming the dataframes

# COMMAND ----------

from pyspark.sql.functions import split, regexp_extract

movies_with_year_df = movies_df.select('ID', 'title',regexp_extract('title',r'\((\d+)\)',1).alias('year'))

# COMMAND ----------

#view changes
movies_with_year_df.show(4,truncate=False)

# COMMAND ----------

#count of movies produced per year
display(movies_with_year_df.groupBy('year').count().orderBy('count',ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare Ratings

# COMMAND ----------

# prepare ratings schema

ratings_df_schema = StructType(
[StructField('userId',IntegerType()),
StructField('movieId',IntegerType()),
StructField('rating',DoubleType())]
) #time stamp column is getting dropped

# COMMAND ----------

# create ratings df
ratings_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferSchema=False).schema(ratings_df_schema).load(ratings_filename)
ratings_df.show(4)

# COMMAND ----------

#cache dataframes
ratings_df.cache()
movies_df.cache()

# COMMAND ----------

from pyspark.sql import functions as F

#let's create an average ratings dataframe

movie_ids_with_avg_ratings_df = ratings_df.groupBy('movieId').agg(F.count(ratings_df.rating).alias("count"), F.avg(ratings_df.rating).alias("average"))

print('movie_ids_with_avg_ratings_df:')

movie_ids_with_avg_ratings_df.show(4,truncate=False)

# COMMAND ----------

movie_names_with_avg_ratings_df = movie_ids_with_avg_ratings_df.join(movies_df,F.col('movieId') == F.col('ID')).drop('ID')
movie_names_with_avg_ratings_df.show(4, truncate=False)

# COMMAND ----------

#check global popularity
movies_with_500_ratings_or_more = movie_names_with_avg_ratings_df.filter(movie_names_with_avg_ratings_df['count'] >=500).orderBy('average',ascending=False)

movies_with_500_ratings_or_more.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Train, Test and Validation dataset

# COMMAND ----------

# We'll hold out 60% for training, 20% of our data for validation, and leave 20% for testing
seed = 4
(split_60_df, split_a_20_df, split_b_20_df) = ratings_df.randomSplit([6.0, 2.0, 2.0], seed)

# Let's cache these datasets for performance
training_df = split_60_df.cache()
validation_df = split_a_20_df.cache()
test_df = split_b_20_df.cache()

print('Training: {0}, validation: {1}, test: {2}\n'.format(
  training_df.count(), validation_df.count(), test_df.count())
)
training_df.show(3)
validation_df.show(3)
test_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Alternating Least Squares

# COMMAND ----------

from pyspark.ml.recommendation import ALS

# Let's initialize our ALS learner
als = ALS()

# Now we set the parameters for the method
(als.setMaxIter(5)
   .setSeed(seed)
   .setRegParam(0.1)
   .setUserCol("userId")
   .setItemCol("movieId")
   .setRatingCol("rating"))

# create the model with these parameters
my_ratings_model = als.fit(training_df)

# COMMAND ----------

# evaluate model
from pyspark.ml.evaluation import RegressionEvaluator

# Create an RMSE evaluator using the label and predicted columns
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse")

my_predict_df = my_ratings_model.transform(test_df)

# Remove NaN values from prediction
predicted_test_my_ratings_df = my_predict_df.filter(my_predict_df.prediction != float('nan'))

# Run the previously created RMSE evaluator, reg_eval, on the predicted_test_df DataFrame
test_RMSE_my_ratings = reg_eval.evaluate(predicted_test_my_ratings_df)

print('The model had a RMSE on the test set of {0}'.format(test_RMSE_my_ratings))

dbutils.widgets.text("input","5","")
ins = dbutils.widgets.get("input")
uid=int(ins)
ll=predicted_test_my_ratings_df.filter(F.col("userId")==uid)

# COMMAND ----------

MovieRec = ll.join(movies_df,F.col('movieID')==F.col('ID')).drop('ID').select('title').take(10)
l=dbutils.notebook.exit(MovieRec)
