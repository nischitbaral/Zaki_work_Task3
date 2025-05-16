
import psycopg2


def load_file(scrub_nets,scrub_prov,etl):

    spark =etl.spark

    port = etl.port
    dbname = etl.database
    user = etl.user
    password = etl.password
    host = etl.host
    spark




    df_read = spark.read.parquet(scrub_prov)
    # df_read.printSchema()
    df_read2= spark.read.parquet(scrub_nets)
    df_read2.printSchema()




    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    # conn.autocommit = True

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    connection_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }




    cur = conn.cursor()
    create_table_query = """
    DROP TABLE IF EXISTS provider_new;
   
    CREATE TABLE provider_new (
    provider_group_id BIGINT,
    npi BIGINT,
    tin_type SMALLINT,
    tin VARCHAR,
    prv_city VARCHAR,
    prv_phone VARCHAR,
    prv_state VARCHAR,
    prv_street_1 VARCHAR,
    prv_type_code INTEGER,
    prv_zip VARCHAR,
    full_name VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    taxonomy TEXT[],
    prv_specialty TEXT[]
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








