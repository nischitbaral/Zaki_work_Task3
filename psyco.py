
from pyspark.sql import SparkSession
import psycopg2

spark =  SparkSession.builder.appName('psyco').getOrCreate()

df = spark.read.parquet('in_prov.parquet')

print(df)
df.show()
conn = psycopg2.connect(
    dbname="my_pgdb",
    user="postgres",
    password="admin",
    host="localhost",
    port=5432
)
conn.autocommit = True

cur = conn.cursor()

# # create_table_query = """
# # CREATE TABLE IF NOT EXISTS provider_data (
# #     id SERIAL PRIMARY KEY,
# #     provider_group_id BIGINT,
# #     npi BIGINT[],
# #     tin_type TEXT,
# #     tin_value TEXT
# # );
# # """
# # cur.execute(create_table_query)
# # conn.commit()

# # insert_query = """
# # INSERT INTO provider_data (provider_group_id, npi, tin_type, tin_value)
# # VALUES (%s, %s, %s, %s)
# # """

# # for item in data:
# #     provider_group_id = item.get('provider_group_id')
# #     npi_list = item.get('npi', [])
# #     tin_type = item.get('tin_type', 'default_type')
# #     tin_value = item.get('tin_value', 'default_value')
# #     cur.execute(insert_query, (provider_group_id, npi_list, tin_type, tin_value))

# conn.commit()
# cur.close()
# conn.close()


