

from pyspark.sql import SparkSession
import psycopg2


def load_file(scrub_nets,scrub_prov):

    spark = SparkSession.builder.appName('load').getOrCreate()



    spark




    df_read = spark.read.parquet(scrub_prov)
    # df_read.printSchema()
    df_read2= spark.read.parquet(scrub_nets)
    df_read2.printSchema()




    conn = psycopg2.connect(
        dbname="my_pgdb",
        user="postgres",
        password="admin",
        host="localhost",
        port=5432
    )
    # conn.autocommit = True

    jdbc_url = "jdbc:postgresql://localhost:5432/my_pgdb"
    connection_properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }




    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS provider_new (
        provider_group_id BIGINT,
        npi BIGINT,
        tin_type SMALLINT,
        tin TEXT
    );
    """
    cur.execute(create_table_query)

    conn.commit()

    df_read.write.jdbc(url=jdbc_url,table="provider_new",mode="append", properties=connection_properties)

















    cur = conn.cursor()

    create_table_query = """

    CREATE TABLE IF NOT EXISTS network_new (
        billing_code TEXT,
        billing_code_type TEXT,
        negotiation_arrangement TEXT,
        provider_group_id BIGINT,
        billing_class TEXT,
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,
        negotiated_type TEXT,
        service_code INTEGER[]
    );
    """
    cur.execute(create_table_query)

    conn.commit()




    df_read2.write.jdbc(url=jdbc_url,table="network_new",mode="append", properties=connection_properties)




    cur.close()
    conn.close()








