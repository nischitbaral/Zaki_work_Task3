


from pyspark.sql import SparkSession 
from pyspark.sql.functions import explode,col,hash,expr,split,array,when
from pyspark.sql.types import ArrayType, IntegerType,ShortType


def scrub_file(net_file,prov_path):

    spark = SparkSession.builder.appName('new_network').config("spark.driver.memory", "4g").getOrCreate()





    spark




    df_pyspark = spark.read.json(net_file)



    df_pyspark.printSchema()



    network_group = df_pyspark.withColumn('new_network',explode('negotiated_rates'))
    providers_n = network_group.withColumn("provider_group_id", explode("new_network.provider_references"))
    network_agn = providers_n.withColumn('newest_network',explode('new_network.negotiated_prices'))

    network_again = network_agn.select(
        "billing_code","billing_code_type","negotiation_arrangement",
        col('newest_network.billing_class').alias('billing_class'),
        col('newest_network.negotiated_rate').alias('negotiated_rate'),
        
        col('newest_network.billing_code_modifier').alias('billing_code_modifier'),
        col('newest_network.negotiated_type').alias('negotiated_type'),
        col('newest_network.service_code').alias('service_code'),
        col('provider_group_id')
        
    )




    network_again.show()



    removena_bc = network_again.dropna(subset=['billing_code'])



    removena_bc.printSchema()




    df_cast = removena_bc.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))
    df_cast2 = df_cast.withColumn("provider_group_id",(col("provider_group_id").cast(IntegerType())))



    df_cast2.printSchema()


    scrub_nets = 'out_files/scrub_net.parquet'

    df_cast2.coalesce(1).write.option("compression", "snappy").mode('overwrite').parquet(scrub_nets)




    df_pyspark2 = spark.read.json(prov_path)



    df_pyspark2.printSchema()



    provider_group = df_pyspark2.withColumn("new_provider", explode("provider_groups"))
    providers_n = provider_group.withColumn("npi_exp", explode("new_provider.npi"))


    provider_again = providers_n.select(
        "provider_group_id",
        col("npi_exp").alias("npi"),
        col("new_provider.tin.type").alias("tin_type"),
        col("new_provider.tin.value").alias("tin")
    )         

    provider_again.show()




    rem_hyp = provider_again.withColumn('tin', expr("replace(tin,'-','')"))
    rem_hyp.show()




    df = rem_hyp.withColumn('tin_type',
        when((col('tin_type')== 'ein'), 1).when((col('tin_type')== 'npi'), 2)

                        )




    df.show()




    rename_col = df.withColumnRenamed('tin_value','tin')
    rename_col.show()



    rename_col.printSchema()




    rate_cast = rename_col.withColumn("tin_type",col("tin_type").cast(ShortType()))




    rate_cast.printSchema()



    scrub_prov ='out_files/scrub_prov.parquet'
    rate_cast.write.parquet(scrub_prov)
    
    return scrub_nets,scrub_prov





