from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,hash,expr,array,concat_ws,concat,when,array_except,lit,lpad,regexp_replace
from pyspark.sql.types import ArrayType, IntegerType, ShortType

def scr_nrpr(combined_file,hlt_prv,etl):
    spark = etl.spark
    df_pr = spark.read.option('multiline','true').json(combined_file)
    df_pr = (
        df_pr
        .selectExpr("*", "explode(in_network) as net").drop("in_network")
        .select("*", "net.*").drop("net")
        .selectExpr("*", "explode(negotiated_rates) as rates").drop("negotiated_rates")
        .selectExpr("*", "explode(rates.negotiated_prices) as prices").drop("negotiated_prices")
        .selectExpr("*", "explode(rates.provider_groups) as provider").drop("rates")
        .selectExpr("*", "provider.tin.type as tin_type", "provider.tin.value as tin").drop("provider_groups")
        .selectExpr("*", "explode(provider.npi) as npi").drop("provider")
        .select("*", "prices.*").drop("prices")
        )
    

    df_merge = df_pr.withColumn("provider_group_id",hash(concat("npi", "tin")))
  
    df_select_nr = df_merge.select(
        "billing_code",
        "billing_code_type",
        "negotiation_arrangement",
        "billing_class",
        "billing_code_modifier",
        "negotiated_rate",
        "negotiated_type",
        "service_code",
        "provider_group_id"
        )



    rate_tbl =  df_select_nr.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType())))
  #rate_data


    net_nrpr = 'out_folder/net_nrpr.parquet'

    rate_tbl.write.mode('overwrite').parquet(net_nrpr)
    
#billing_code_tax#######################
    df_bc = spark.read.option('header','True').csv('/home/nischit-baral/Desktop/Zaki_work_Task3/ignore/billing_taxonomy_list.csv')
    df_lpad = df_bc.withColumn("billing_code", lpad(col("billing_code"), 5, "0"))
    df_drop = df_lpad.drop('_c4','_c5','_c6') \
        .withColumn("taxonomy_list",array(regexp_replace(col("taxonomy_list"), r"^\{|\}$", "")))

    df_drop.printSchema()
    df_select = df_drop.select(
        "billing_code",
        "taxonomy_list"
        )




    provider_tbl = df_merge.select( 'npi',
            "tin_type",
            "tin",
            "provider_group_id")

  
    rem_hyp = provider_tbl.withColumn('tin', expr("replace(tin,'-','')"))
    df = rem_hyp.withColumn('tin_type',
            when((col('tin_type')== 'ein'), 1).when((col('tin_type')== 'npi'), 2)

                            )
    main_provider = df.withColumn("tin_type", col("tin_type").cast(ShortType()))
  ##provider_data
  


    #highlight_prv
    df_new = spark.read.parquet(hlt_prv)
    df_new1 = df_new.drop('prv_fax','provider_name_prefix_text','prv_type_desc')
    df_chng = df_new1.withColumn('prv_type_code',
        when((col('prv_type_code')== 'P'), 1).when((col('prv_type_code')== 'F'), 2)

                        )

    df_castt = df_chng.withColumn("prv_type_code",col("prv_type_code").cast(IntegerType()))
    df_merge = df_castt.withColumn("full_name",concat_ws(" ","provider_first_name", "provider_middle_name","provider_last_name"))
    df_merge1=df_merge.drop("provider_first_name","provider_last_name","provider_middle_name")
    

    df_newws = df_merge1.select(
        "*",  
        col("loc.lat").alias("latitude").cast("double"),
        col("loc.lon").alias("longitude").cast("double")
    )

    df_new2 = df_newws.drop('loc')

    df_arr = df_new2.withColumn("taxonomy",array(col("prv_taxonomy_1_code"),col("prv_taxonomy_2_code"),col("prv_taxonomy_3_code")))
    df_arr1=df_arr.drop("prv_taxonomy_1_code","prv_taxonomy_2_code","prv_taxonomy_3_code")

    df_arr3 = df_arr1.withColumn("prv_specialty",array(col("prv_specialty_1_desc"),col("prv_specialty_2_desc"),col("prv_specialty_3_desc")))
    df_arr4=df_arr3.drop("prv_specialty_1_desc","prv_specialty_2_desc","prv_specialty_3_desc")

    df_arr4.printSchema()
    df_arr4 = df_arr4.withColumn(
        "taxonomy",array_except(col("taxonomy"),array(lit(None), lit("")))) \
            .withColumn(
        "prv_specialty",array_except(col("prv_specialty"),array(lit(None), lit(""))))
    df_join = main_provider.join(
        df_arr4,
        how="inner",
        on=["npi","tin"]

    )
    
    

    prv_nrpr = 'out_folder/prv_nrpr.parquet'
    df_join.write.mode('overwrite').parquet(prv_nrpr)


    return main_provider,df_arr4,rate_tbl,df_drop,df_select
    

