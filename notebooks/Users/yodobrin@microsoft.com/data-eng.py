# Databricks notebook source

containers = ["cloud-rentals", "cloud-sales","cloud-streaming","movies","cloud-vanarse","southride-lake"]
storage_account = "ohstoragelake"
configs = {"fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

actors_schema = "ActorID string,ActorName string,Gender string"
movies_actors_schema = "MovieActorID string,MovieID string,ActorID string"
movies_schema = "MovieID string,MovieTitle string,Category string,Rating string,RunTimeMin integer,ReleaseDate string"
movies_online_schema =  "MovieID string,  OnlineMovieID string"
trx_schema = "TransactionID string, CustomerID string, MovieID string, RentalDate integer, ReturnDate integer, RentalCost decimal(19,4), LateFee decimal(19,4), RewindFlag integer, CreatedDate date, UpdatedDate date"

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# DBTITLE 1,Movies
# movies data from these dbs:
# movies
# vanarse
# cloud rentals

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in movies

# COMMAND ----------

# MAGIC %sql
# MAGIC create database Movies location '/mnt/ohstoragelake/southride-lake/databases/movies'

# COMMAND ----------

# MAGIC %sql drop table movies.movies

# COMMAND ----------

# MAGIC %sql create table if not exists movies.movies (
# MAGIC   SourceSystem string,
# MAGIC   MovieID string,
# MAGIC   MovieTitle string,
# MAGIC   Category string,
# MAGIC   Rating string,
# MAGIC   RunTimeMin integer,
# MAGIC   ReleaseDate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies.movies

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), SourceSystem 
# MAGIC from movies.movies
# MAGIC group by SourceSystem

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.movies
# MAGIC select * from movies.va_movies

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies.tmp_movies

# COMMAND ----------

#DONE
%sql
create table movies.tmp_movies
select 'Movies-DB' as SourceSystem, id as MovieID, title as MovieTitle, genre as Category, rating as Rating, runtime as RunTimeMin, to_date(array_join(array(releaseYear, '01','01'), '-')) as
ReleaseDate
from tmp_movies

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies.cr_movies

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cr_movies

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC create table movies.cr_movies
# MAGIC select 'cloud-rentals' as SourceSystem, MovieID,MovieTitle, Category, Rating, RunTimeMin, to_date(ReleaseDate,'MM-dd-yyyy') as
# MAGIC ReleaseDate
# MAGIC from cr_movies

# COMMAND ----------

# MAGIC %sql select * from va_movies

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC create table movies.va_movies
# MAGIC select 'vanarse' as SourceSystem, MovieID,MovieTitle, Category, Rating, RunTimeMin, to_date(ReleaseDate,'MM-dd-yyyy') as
# MAGIC ReleaseDate
# MAGIC from va_movies

# COMMAND ----------

# DBTITLE 1,Actors
# MAGIC %sql
# MAGIC create table movies.va_actors 
# MAGIC select 'vanarse' as SourceSystem,ActorID,ActorName,Gender
# MAGIC from va_actors
# MAGIC 
# MAGIC create table movies.cr_actors 
# MAGIC select 'cloud-rentals' as SourceSystem,ActorID,ActorName,Gender
# MAGIC from cr_actors

# COMMAND ----------

# MAGIC %sql create table if not exists movies.actors (
# MAGIC   SourceSystem string,
# MAGIC   ActorID string,
# MAGIC   ActorName string,
# MAGIC   Gender string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.actors
# MAGIC select * from movies.va_actors

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), SourceSystem 
# MAGIC from movies.actors
# MAGIC group by SourceSystem

# COMMAND ----------

# DBTITLE 1,Movies-Actors


# COMMAND ----------

# MAGIC %sql
# MAGIC create table movies.movies_actors
# MAGIC (
# MAGIC   SourceSystem string,
# MAGIC   MovieActorID string,
# MAGIC   MovieID string,
# MAGIC   ActorID string
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table movies.cr_movie_actors
# MAGIC select 'cloud-rentals' as SourceSystem, MovieActorID, MovieID, ActorID
# MAGIC from cr_movies_actors

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.movies_actors
# MAGIC select *
# MAGIC from movies.cr_movie_actors

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cr_movies_actors

# COMMAND ----------

# DBTITLE 1,Movies-online


# COMMAND ----------

# MAGIC %sql
# MAGIC create table movies.movies_online_map
# MAGIC (
# MAGIC   SourceSystem string,
# MAGIC   MovieID string,
# MAGIC   OnlineMovieID string
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table movies.va_movies_online_map
# MAGIC select 'vanarse' as SourceSystem, MovieID, OnlineMovieID
# MAGIC from va_movies_online_map

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.movies_online_map
# MAGIC select *
# MAGIC from movies.va_movies_online_map

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from va_movies_online_map

# COMMAND ----------

# MAGIC %sql
# MAGIC create table movies.cr_movies_online_map
# MAGIC select 'cloud-rentals' as SourceSystem, MovieID, OnlineMovieID
# MAGIC from cr_movies_online_map

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.movies_online_map
# MAGIC select *
# MAGIC from movies.cr_movies_online_map

# COMMAND ----------

# DBTITLE 1,Transactions
#vanarse, rentals

# COMMAND ----------

# MAGIC %sql create table movies.transactions (
# MAGIC   SourceSystem string,
# MAGIC   TransactionID string,
# MAGIC   CustomerID string,
# MAGIC   MovieID string,
# MAGIC   RentalDate date,
# MAGIC   ReturnDate date,
# MAGIC   RentalCost decimal(19, 4),
# MAGIC   LateFee decimal(19, 4),
# MAGIC   RewindFlag boolean,
# MAGIC   CreatedDate date,
# MAGIC   UpdatedDate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'cloud-rentals' as SourceSystem, TransactionID, CustomerID, MovieID, to_date(string(RentalDate),'yyyymmdd') as RentalDate, to_date(string(ReturnDate),'yyyymmdd') as ReturnDate,RentalCost,LateFee,boolean(RewindFlag) as RewindFlag, to_date(CreatedDate,'yyyy-mm-dd') as CreatedDate,to_date(UpdatedDate,'yyyy-mm-dd') as UpdateDated
# MAGIC from cr_trx

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.transactions
# MAGIC select 'cloud-rentals' as SourceSystem, TransactionID, CustomerID, MovieID, to_date(string(RentalDate),'yyyymmdd') as RentalDate, to_date(string(ReturnDate),'yyyymmdd') as ReturnDate,RentalCost,LateFee,boolean(RewindFlag) as RewindFlag, to_date(CreatedDate,'yyyy-mm-dd') as CreatedDate,to_date(UpdatedDate,'yyyy-mm-dd') as UpdateDated
# MAGIC from cr_trx

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into movies.transactions
# MAGIC select 'vanarse' as SourceSystem, TransactionID, CustomerID, MovieID, to_date(string(RentalDate),'yyyymmdd') as RentalDate, to_date(string(ReturnDate),'yyyymmdd') as ReturnDate,double(RentalCost),double(LateFee),boolean(RewindFlag) as RewindFlag, to_date(CreatedDate,'yyyy-mm-dd') as CreatedDate,to_date(UpdatedDate,'yyyy-mm-dd') as UpdateDated
# MAGIC from va_trx

# COMMAND ----------

cr_trx_df.createOrReplaceTempView('cr_trx')
va_trx_df.createOrReplaceTempView('va_trx')

# COMMAND ----------

file_2_read = "Transactions.csv"
directory = "cloud-rentals"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cr_trx_df = spark.read.csv(path,header = True, nullValue = 'NULL',schema=trx_schema) 
display(cr_trx_df)

# COMMAND ----------


file_2_read = "dboTransactionsundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
va_trx_df = spark.read.parquet(path) 
display(va_trx_df)

# COMMAND ----------

# movies_df.createOrReplaceTempView('tmp_movies')
cr_actors_df.createOrReplaceTempView('cr_actors')
cr_movies_ac_df.createOrReplaceTempView('cr_movies_actors')
cr_movies_df.createOrReplaceTempView('cr_movies')
va_actors_df.createOrReplaceTempView('va_actors')
va_mv_online_map_df.createOrReplaceTempView('va_movies_online_map')
va_movies_df.createOrReplaceTempView('va_movies')
cr_movies_online_df.createOrReplaceTempView('cr_movies_online_map')

# COMMAND ----------

cr_movies_online_df.createOrReplaceTempView('cr_movies_online_map')
file_2_read = "data_fdd5c08b-21cc-4d70-bd3b-31cef2ddb767_139277a7-8913-4301-b756-558c76ad1d47.parquet"
directory = "movies"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
movies_df = spark.read.parquet(path) 
# display(movies_df)

# COMMAND ----------

file_2_read = "Actors.csv"
directory = "cloud-rentals"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cr_actors_df = spark.read.csv(path,header = True, nullValue = 'NULL',schema=actors_schema) 
display(cr_actors_df)

# COMMAND ----------

file_2_read = "MovieActors.csv"
directory = "cloud-rentals"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cr_movies_ac_df = spark.read.csv(path,header = True, nullValue = 'NULL',schema=movies_actors_schema) 
display(cr_movies_ac_df)

# COMMAND ----------

file_2_read = "Movies.csv"
directory = "cloud-rentals"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cr_movies_df = spark.read.csv(path,header = True, nullValue = 'NULL',schema=movies_schema) 
display(cr_movies_df)

# COMMAND ----------

file_2_read = "OnlineMovieMappings.csv"
directory = "cloud-rentals"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cr_movies_online_df = spark.read.csv(path,header = True, nullValue = 'NULL',schema=movies_online_schema) 
display(cr_movies_online_df)

# COMMAND ----------

''' rentals
https://ohstoragelake.blob.core.windows.net/cloud-rentals/Actors.csv
https://ohstoragelake.blob.core.windows.net/cloud-rentals/MovieActors.csv
https://ohstoragelake.blob.core.windows.net/cloud-rentals/Movies.csv  
'''

''' vanrase
https://ohstoragelake.blob.core.windows.net/cloud-vanarse/dboActorsundefined
https://ohstoragelake.blob.core.windows.net/cloud-vanarse/dboMovieActorsundefined
https://ohstoragelake.blob.core.windows.net/cloud-vanarse/dboMoviesundefined
https://ohstoragelake.blob.core.windows.net/cloud-vanarse/dboOnlineMovieMappingsundefined
'''

# COMMAND ----------

file_2_read = "dboActorsundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
va_actors_df = spark.read.parquet(path) 
display(va_actors_df)

# COMMAND ----------

file_2_read = "dboOnlineMovieMappingsundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
va_mv_online_map_df = spark.read.parquet(path) 
display(va_mv_online_map_df)

# COMMAND ----------

file_2_read = "dboMovieActorsundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
va_mv_ac_map_df = spark.read.parquet(path) 
display(va_mv_ac_map_df)

# COMMAND ----------

file_2_read = "dboMoviesundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
va_movies_df = spark.read.parquet(path) 
display(va_movies_df)

# COMMAND ----------

