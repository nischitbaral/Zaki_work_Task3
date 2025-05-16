
from pyspark.sql.functions import explode,col,hash,expr,split,array,when,concat, lit
from pyspark.sql.types import ArrayType, IntegerType,ShortType

def scrub_file(net_file,prov_path,provider_dtl_path,etl):
    spark = etl.spark

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



####provider data#####
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

 

###new provider_datail data#######

    df_pyspark3 = spark.read.json(provider_dtl_path)
    df_new1 = df_pyspark3.drop('prv_fax','provider_name_prefix_text','prv_type_desc')
    df_chng = df_new1.withColumn('prv_type_code',
     when((col('prv_type_code')== 'P'), 1).when((col('prv_type_code')== 'F'), 2)

                       )

    df_castt = df_chng.withColumn("prv_type_code",col("prv_type_code").cast(IntegerType()))
    df_merge = df_castt.withColumn("full_name",concat(col('provider_first_name'),lit(" "),col('provider_last_name'),lit(" "),col('provider_middle_name')))
    df_merge1=df_merge.drop("provider_first_name","provider_last_name","provider_middle_name")


    df_newws = df_merge1.select(
        "*",  
        col("loc.lat").alias("latitude"),
        col("loc.lon").alias("longitude")
    )

    df_new2 = df_newws.drop('loc')

    df_arr = df_new2.withColumn("taxonomy",array(col("prv_taxonomy_1_code"),col("prv_taxonomy_2_code"),col("prv_taxonomy_3_code")))
    df_arr1=df_arr.drop("prv_taxonomy_1_code","prv_taxonomy_2_code","prv_taxonomy_3_code")

    df_arr3 = df_arr1.withColumn("prv_specialty",array(col("prv_specialty_1_desc"),col("prv_specialty_2_desc"),col("prv_specialty_3_desc")))
    df_arr4=df_arr3.drop("prv_specialty_1_desc","prv_specialty_2_desc","prv_specialty_3_desc")

    df_arr4.printSchema()

    df_join = df.alias("old").join(
        df_arr4.alias("new"),
        how="inner",
        on=col("old.npi") == col("new.npi")
    )

    df_res = df_join.select(
        col("provider_group_id"),
        col("old.npi"),
        col("tin_type"),
        col("tin"),
        col("prv_city"),
        col("prv_phone"),
        col("prv_state"),
        col("prv_street_1"),
        col("prv_type_code"),
        col("prv_zip"),
        col("full_name"),
        col("latitude"),
        col("longitude"),
        col("taxonomy"),
        col("prv_specialty")
    )


    df_res.printSchema()



    scrub_prov ='out_files/scrub_prov.parquet'
    df_res.write.option("compression", "snappy").mode('overwrite').parquet(scrub_prov)

    return scrub_nets,scrub_prov





