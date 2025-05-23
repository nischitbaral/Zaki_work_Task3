
from pyspark.sql.functions import size,array_intersect,col

def process_file(main_provider,df_arr4,rate_tbl,df_select):

    df_join = main_provider.join(df_arr4,how="inner",on=["npi","tin"])
    df_newjin = rate_tbl.join(df_select,how="inner",on="billing_code" )
    df_newjin.printSchema()


    df_joined = df_newjin.join( df_join,how="inner",on='provider_group_id')

    filter_spec = df_joined.filter(size(array_intersect(col("prv_taxonomy"), col("taxonomy_list"))) > 0)

    filter_spec.show()

    net_nrpr = 'out_folder/net_nrpr.parquet'
    prv_nrpr = 'out_folder/prv_nrpr.parquet'

    df_newjin.write.mode('overwrite').parquet(net_nrpr)
    df_join.write.mode('overwrite').parquet(prv_nrpr)   

    return net_nrpr,prv_nrpr