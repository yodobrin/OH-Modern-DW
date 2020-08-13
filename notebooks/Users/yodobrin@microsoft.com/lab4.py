# Databricks notebook source


# COMMAND ----------

containers = ["cloud-rentals", "cloud-sales","cloud-streaming","movies","cloud-vanarse","southride-lake"]
storage_account = "ohstoragelake"


# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# Creating all mounts for all sources and lake
for container in containers:
  mount_point = f"/mnt/{storage_account}/{container}"
  if mount_point not in [m.mountPoint for m in dbutils.fs.mounts()]:
    dbutils.fs.mount(source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",mount_point = mount_point, extra_configs = configs)
    print('*' * 20 + mount_point)

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# Customers Data exisit in:
# cloud-rentals: Customers.csv
# cloud-vanarse: dboCustomersundefined
# cloud-streaming: dbo.Customers
# cloud-sales : dbo.Customers
import pandas as pd
# Load each source table, ensure they all have the same schema
# add a source_system colum to each and populate it accordigly
# join all reformated sources into a single table

# COMMAND ----------

# DBTITLE 1,Orders and details
# MAGIC %sql
# MAGIC create database Orders location '/mnt/ohstoragelake/southride-lake/databases/orders'

# COMMAND ----------

# MAGIC %sql create table if not exists Orders.orders (
# MAGIC SourceSystem string,
# MAGIC OrderID string,
# MAGIC CustomerID string,
# MAGIC OrderDate date,
# MAGIC ShipDate date,
# MAGIC TotalCost decimal(19,4),
# MAGIC CreatedDate date,
# MAGIC UpdatedDate date,
# MAGIC MovieID string,
# MAGIC Quantity integer,
# MAGIC UnitCost decimal(19,4),
# MAGIC LineNumber integer
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders.orders
# MAGIC select * 
# MAGIC from orders.stream_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in Orders

# COMMAND ----------

cs_orders_df.createOrReplaceTempView('cs_orders')
cs_orders_details_df.createOrReplaceTempView('cs_details')

stream_orders_df.createOrReplaceTempView('st_orders')
stream_orders_details_df.createOrReplaceTempView('st_details')

# COMMAND ----------

# MAGIC %sql create table orders.sales_orders as
# MAGIC select
# MAGIC   'cloud-sales' as SourceSystem,
# MAGIC   cs_orders.OrderID,
# MAGIC   cs_orders.CustomerID,
# MAGIC   cs_orders.OrderDate,
# MAGIC   cs_orders.ShipDate,
# MAGIC   cs_orders.TotalCost,
# MAGIC   cs_orders.CreatedDate,
# MAGIC   cs_orders.UpdatedDate,
# MAGIC   cs_details.MovieID,
# MAGIC   cs_details.Quantity,
# MAGIC   cs_details.UnitCost,
# MAGIC   cs_details.LineNumber
# MAGIC from
# MAGIC   cs_orders
# MAGIC   join cs_details using (OrderID)

# COMMAND ----------

# MAGIC %sql create table orders.stream_orders as
# MAGIC select
# MAGIC   'cloud-sales' as SourceSystem,
# MAGIC   st_orders.OrderID,
# MAGIC   st_orders.CustomerID,
# MAGIC   st_orders.OrderDate,
# MAGIC   st_orders.ShipDate,
# MAGIC   st_orders.TotalCost,
# MAGIC   st_orders.CreatedDate,
# MAGIC   st_orders.UpdatedDate,
# MAGIC   st_details.MovieID,
# MAGIC   st_details.Quantity,
# MAGIC   st_details.UnitCost,
# MAGIC   st_details.LineNumber
# MAGIC from
# MAGIC   st_orders
# MAGIC   join st_details using (OrderID)

# COMMAND ----------


file_2_read = "dbo.Orders/data_82ef4b53-709f-4393-9009-6881098b8ad9_e91cf006-6c2a-4dd9-ada8-96887f2cf4ee.parquet"
directory = "cloud-sales"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cs_orders_df = spark.read.parquet(path,header = True, nullValue = 'NULL',schema=customer_schema) 
display(cs_orders_df)

# COMMAND ----------




# COMMAND ----------

# DBTITLE 0,Orders and details

file_2_read = "dbo.Orders/data_6078c372-38a7-4913-b25d-298ca3796be0_c80a3fcb-0aa4-4401-9b20-bc0d0a18a23b.parquet"
directory = "cloud-streaming"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
stream_orders_df = spark.read.parquet(path,header = True, nullValue = 'NULL',schema=customer_schema) 
display(stream_orders_df)

# COMMAND ----------

file_2_read = "dbo.OrderDetails/data_3ff38692-3046-4d6e-9276-3c4b40c925db_f2b2039f-a9c0-419c-993c-74e65f1c0cad.parquet"
directory = "cloud-sales"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
cs_orders_details_df = spark.read.parquet(path,header = True, nullValue = 'NULL',schema=customer_schema) 
display(cs_orders_details_df)

# COMMAND ----------

file_2_read = "dbo.OrderDetails/data_073d7634-4ee9-4472-a76d-72c08be26889_90ad1f1a-95fc-43c8-a61b-0be99a5daee4.parquet"
directory = "cloud-streaming"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
stream_orders_details_df = spark.read.parquet(path,header = True, nullValue = 'NULL',schema=customer_schema) 
display(stream_orders_details_df)

# COMMAND ----------

file_2_read = "dbo.OrderDetails/data_073d7634-4ee9-4472-a76d-72c08be26889_90ad1f1a-95fc-43c8-a61b-0be99a5daee4.parquet"
directory = "cloud-streaming"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
stream_orders_details_df = spark.read.parquet(path,header = True, nullValue = 'NULL',schema=customer_schema) 
display(stream_orders_details_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database Customers location '/mnt/ohstoragelake/southride-lake/databases/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from
# MAGIC customers.customers

# COMMAND ----------

customer_schema = "CustomerID string,LastName string,FirstName string,AddressLine1 string,AddressLine2 string,City string,State string,ZipCode string,PhoneNumber string,CreatedDate date,UpdatedDate date"

# COMMAND ----------

# DBTITLE 1,Populate customers.customers table from all sources
# MAGIC %sql
# MAGIC insert into customers.customers
# MAGIC select * 
# MAGIC from customers.rentals_customers
# MAGIC -- union
# MAGIC -- customers.cs_customers
# MAGIC -- union 
# MAGIC -- customers.sales_customers
# MAGIC -- union
# MAGIC -- customers.va_customers

# COMMAND ----------

# DBTITLE 1,Create the initial table from the csv file (in rentals)
# MAGIC %sql drop table customers.customers

# COMMAND ----------

# MAGIC %sql create table if not exists customers.customers (
# MAGIC   SourceSystem string,
# MAGIC   CustomerID string,
# MAGIC   LastName string,
# MAGIC   FirstName string,
# MAGIC   AddressLine1 string,
# MAGIC   AddressLine2 string,
# MAGIC   City string,
# MAGIC   State string,
# MAGIC   ZipCode string,
# MAGIC   PhoneNumber string,
# MAGIC   CreatedDate date,
# MAGIC   UpdatedDate date
# MAGIC ) 

# COMMAND ----------

file_2_read = "Customers.csv"
directory = "cloud-rentals"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
rentals_customer_df = spark.read.csv(path,header = True, nullValue = 'NULL',schema=customer_schema) 
display(rentals_customer_df)
# using csv options (header = True, nullValue = 'NULL') location '/mnt/ohstoragelake/cloud-rentals/Customers.csv'

# COMMAND ----------

rentals_customer_df.createOrReplaceTempView('rent_cust')

# COMMAND ----------

# MAGIC %sql drop table customers.rentals_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers.rentals_customers as
# MAGIC select
# MAGIC   'cloud-rentals' as SourceSystem,
# MAGIC   rent_cust.CustomerID,
# MAGIC   rent_cust.FirstName,
# MAGIC   rent_cust.LastName,
# MAGIC   rent_cust.AddressLine1,
# MAGIC   rent_cust.AddressLine2,
# MAGIC   rent_cust.City,
# MAGIC   rent_cust.State,
# MAGIC   rent_cust.ZipCode,
# MAGIC   rent_cust.PhoneNumber,
# MAGIC   rent_cust.UpdatedDate,
# MAGIC   rent_cust.CreatedDate
# MAGIC from
# MAGIC   rent_cust

# COMMAND ----------

# DBTITLE 1,Cloud-Sales - Combine Customers & Address


# COMMAND ----------

# load addresses from cloud-streaming
file_2_read = "data_211412a2-6fbb-4f0b-a297-7114df39f652_f4405f74-f92f-4be5-91db-d5a4fea8caf2.parquet"
directory = "cloud-sales"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/dbo.Addresses/{file_name}".format(directory = directory, file_name = file_2_read)
sales_address_df = spark.read.parquet(path) 
# display(sales_address_df)

# COMMAND ----------

# load customers from cloud-sales

file_2_read = "data_4d6d0ddb-0556-4f2a-a0a0-9a036cee934d_c06e84b3-0d88-4a92-aeab-00b385bb7796.parquet"
directory = "cloud-sales"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/dbo.Customers/{file_name}".format(directory = directory, file_name = file_2_read)
sales_customer_df = spark.read.parquet(path) 
display(sales_customer_df)

# COMMAND ----------

sales_customer_df.createOrReplaceTempView('s_cust')
sales_address_df.createOrReplaceTempView('s_add')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql create table customers.sales_customers as
# MAGIC select
# MAGIC   'cloud-sales' as SourceSystem,
# MAGIC   s_cust.CustomerID,
# MAGIC   s_cust.FirstName,
# MAGIC   s_cust.LastName,
# MAGIC   s_add.AddressLine1,
# MAGIC   s_add.AddressLine2,
# MAGIC   s_add.City,
# MAGIC   s_add.State,
# MAGIC   s_add.ZipCode,
# MAGIC   s_cust.PhoneNumber,
# MAGIC   s_cust.UpdatedDate,
# MAGIC   s_cust.CreatedDate
# MAGIC from
# MAGIC   s_cust
# MAGIC   join s_add using (CustomerID)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers.sales_customers

# COMMAND ----------

# DBTITLE 1,Cloud Streaming - combine Customers and Address
# The following section, reads the two parquet files into data frames
# Then creates two temp views of the data 
# creates a new table in the customer db, with a join of the two tables.

# COMMAND ----------

# load addresses from cloud-streaming
file_2_read = "data_67b98389-2735-45f2-9792-0def3f3b57dc_696fb90d-eda3-4297-9039-6d00bc7491cc.parquet"
directory = "cloud-streaming"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/dbo.Addresses/{file_name}".format(directory = directory, file_name = file_2_read)
cs_address_df = spark.read.parquet(path) 
display(cs_address_df)

# COMMAND ----------

# load customers from cloud-streaming
file_2_read = "data_9a6d5b71-36d4-4a63-af1c-aed91021a2e0_459e1b20-5942-4a1d-95c5-c1c9e4c80797.parquet"
directory = "cloud-streaming"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/dbo.Customers/{file_name}".format(directory = directory, file_name = file_2_read)
cs_customer_df = spark.read.parquet(path) 
display(cs_customer_df)

# COMMAND ----------

# sql version
cs_customer_df.createOrReplaceTempView('cs_cust')
cs_address_df.createOrReplaceTempView('cs_add')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql create table customers.cs_customers as
# MAGIC select
# MAGIC   'cloud-streaming' as SourceSystem,
# MAGIC   cs_cust.CustomerID,
# MAGIC   cs_cust.FirstName,
# MAGIC   cs_cust.LastName,
# MAGIC   cs_add.AddressLine1,
# MAGIC   cs_add.AddressLine2,
# MAGIC   cs_add.City,
# MAGIC   cs_add.State,
# MAGIC   cs_add.ZipCode,
# MAGIC   cs_cust.PhoneNumber,
# MAGIC   cs_cust.UpdatedDate,
# MAGIC   cs_cust.CreatedDate
# MAGIC from
# MAGIC   cs_cust
# MAGIC   join cs_add using (CustomerID)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from customers.cs_customers

# COMMAND ----------

# DBTITLE 1,VanArse - combine customer and address


# COMMAND ----------

# load customers from cloud-vanarse
file_2_read = "dboCustomersundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
va_customer_df = spark.read.parquet(path) 
va_customer_df.createOrReplaceTempView('va_cust')
display(va_customer_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers.va_customers as
# MAGIC select
# MAGIC   'cloud-streaming' as SourceSystem,
# MAGIC   va_cust.CustomerID,
# MAGIC   va_cust.FirstName,
# MAGIC   va_cust.LastName,
# MAGIC   va_cust.AddressLine1,
# MAGIC   va_cust.AddressLine2,
# MAGIC   va_cust.City,
# MAGIC   va_cust.State,
# MAGIC   va_cust.ZipCode,
# MAGIC   va_cust.PhoneNumber,
# MAGIC   va_cust.UpdatedDate,
# MAGIC   va_cust.CreatedDate
# MAGIC from
# MAGIC   va_cust

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers.customers

# COMMAND ----------

print(','.join(['{} {}'.format(f['name'], f['type']) for f in customer_df.schema.jsonValue()['fields']]))

# COMMAND ----------

https://ohstoragelake.blob.core.windows.net/cloud-sales/dbo.Customers/
df = spark.read.parquet("abfss://cloud-rentals@ohstoragelake.dfs.core.windows.net/Customers.parquet")
display(df)

# COMMAND ----------

file_2_read = "dbo.Customers/data_4d6d0ddb-0556-4f2a-a0a0-9a036cee934d_c06e84b3-0d88-4a92-aeab-00b385bb7796.parquet"
directory = "cloud-sales"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
ddf = spark.read.parquet(path)
display(ddf)

# COMMAND ----------


file_2_read = "dboMovieActorsundefined"
directory = "cloud-vanarse"
path = "abfss://{directory}@ohstoragelake.dfs.core.windows.net/{file_name}".format(directory = directory, file_name = file_2_read)
ddf = spark.read.parquet(path)
display(ddf)

# COMMAND ----------

path = "{mount_point}/Movies.parquet".format(mount_point = cloud_rentals_mp)
# fc_movies_pd = pd.reead(fc_movies_filepath)
df = spark.read.parquet(path)
display(df)

# COMMAND ----------

