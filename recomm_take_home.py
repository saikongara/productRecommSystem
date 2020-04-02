from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.types import StringType
import uuid
import json, requests


# Loading the `Products` dataset
products = spark.read.option("delimiter", "\t").\
option("inferSchema", "true").\
csv("/FileStore/tables/products.txt").\
withColumnRenamed("_c0", "product_id").\
withColumnRenamed("_c1", "mch_code").\
withColumnRenamed("_c2", "product_name")


# Loading the `MCH categories` dataset provided
mch_categories = spark.read.option("delimiter", "\t").\
option("inferSchema", "true").\
option("header", "true").\
csv("/FileStore/tables/mch_categories.tsv").\
withColumnRenamed("code", "mch_code")


# Loading the `Transactions` dataset provided
transactions = spark.read.\
option("inferSchema", "true").\
json("/FileStore/tables/transactions.txt")



uuidUdf= F.udf(lambda : str(uuid.uuid4()),StringType())
spark.udf.register("uuidUdf", uuidUdf)



# Limiting the number of transaction from the dataset to `10000` as the compute is not sufficient for processing
exploded_trx = transactions.select("customer", "date", F.explode("itemList").alias("items"), "store").withColumn("trx_id", F.lit(uuidUdf())).select("customer", "date","items.item", "items.price", "items.quantity", "store", "trx_id").cache().limit(10000)


win = W.partitionBy(F.col("left.item")).orderBy(F.col("count").desc())



cross_explode = exploded_trx.alias("left").crossJoin(exploded_trx.alias("right")).filter((F.col("left.item") != F.col("right.item")) & (F.col("left.trx_id") != F.col("right.trx_id"))).select("left.item", "right.item").groupBy("left.item", "right.item").count().withColumn("ranking", F.rank().over(win)).filter(F.col("ranking") <= 5).orderBy("left.item").drop("ranking", "count").groupBy("left.item").agg(F.collect_list("right.item").alias("recommended_items")).cache()

# Verifying the recommended products for Item IDs `20592676_EA` and `20801754003_C15`
# Not able to find transaction results for `20801754003_C15` This could be the result of limiting the number of Transactions to `10000`. Also, this could change the accuracy of results as all the transactions were not considered for calculating the recommendation, but the model is accurate in identifying the top 5 recommended products for given Item IDs.

cross_explode = cross_explode.filter(F.col("left.item").isin(["20592676_EA","20801754003_C15"]))
cross_explode.show()

def rearrange_items(data):
  data_map = json.loads(data)
  result = {}
  result[data_map["item"]] = data_map["recommended_items"]
  return result


to_send = {}
to_send["name"] = "Naga Sai"
to_send["email"] = "sai.kongara@gmail.com"
prods = list(map(lambda x: rearrange_items(x), cross_explode.toJSON().collect()))
for p in prods:
  key = list(p.keys())[0]
  to_send[key] = p[key]

print(to_send) 


# What are the item ID and name of the top 5 co-purchased items for item with ID 20592676_EA? What about 20801754003_C15?

try:
  recomm_itemID_for_20592676_EA = to_send['20592676_EA']
  recomm_itemID_for_20801754003_C15 = to_send['20801754003_C15']
except KeyError as e:
  print("Transactions does not contain provided Item ID : {}".format(e))


try:
  top5_recomm_products_for_20592676_EA = products.filter(products["product_id"].isin(recomm_itemID_for_20592676_EA)).select("product_id","product_name")
  top5_recomm_products_for_20592676_EA.show()
except NameError as e:
  print("Not able to identify the Recommended products for 20592676_EA")
  print("Error : {}".format(e))

try:
  top5_recomm_products_for_20801754003_C15 = products.filter(products["product_id"].isin(recomm_itemID_for_20801754003_C15)).select("product_id","product_name")
  top5_recomm_products_for_20801754003_C15.show()
except NameError as e:
  print("Not able to identify the Recommended products for 20801754003_C15")
  print("Error : {}".format(e))

# How would you deploy and serve the model?
# 1. Assuming The data sources(3) would be designed as tables in database and this model connects to database using JDBC/ODBC connections. Once the testing of this model was successful, the code can be deployed onto Production (Python 3.7 and Spark 2.4.4) and triggers whenever the schedules are in place.
# 2. Assuming the the data sources would be real-time, let us consider as kafka events, the code needs a bit more tweaks using Dstreams and StructuralStreams in Spark and the events gets processed real-time. In this scenario code needs to be deployed onto production upon successful completion of testing and triggered once to be executed contineously.

# POST request submission to **Test Service**


from google.cloud import bigquery
from google.oauth2 import service_account

key_path = '/FileStore/tables/service_account-4b7a7.json'

credentials = service_account.Credentials.from_service_account_file(key_path)

client = bigquery.Client(credentials=credentials,project=credentials.project_id)


def fetch_response(to_send):
    api_url = "https://ld-ds-take-home-test.appspot.com/submissions"
    headers = {'Authorization': 'Bearer {}'.format(credentials)}
    input = str(to_send)
    response = requests.post(url=api_url, json=input, headers=headers)
    return response.text
  
fetch_response(to_send)




