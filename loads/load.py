
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
   
    CREATE TABLE provider_data (
    provider_group_id INT,
    npi BIGINT,
    tin_type SMALLINT,
    tin VARCHAR(15),
    prv_city VARCHAR(255),
    prv_phone VARCHAR(15),
    prv_state CHAR(2),
    prv_street_1 VARCHAR(255),
    prv_type_code SMALLINT,
    prv_zip VARCHAR(10),
    full_name VARCHAR(255),
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
        billing_code VARCHAR(10),
        billing_code_type VARCHAR(10),
        negotiation_arrangement VARCHAR(5),
        provider_group_id INT,
        billing_class VARCHAR(15),
        billing_code_modifier TEXT[],
        negotiated_rate DOUBLE PRECISION,
        negotiated_type VARCHAR(12),
        service_code INTEGER[]
    );
    """
    cur.execute(create_table_query)

    conn.commit()




    df_read2.write.jdbc(url=jdbc_url,table="network_new",mode="append", properties=connection_properties)




    cur.close()
    conn.close()








