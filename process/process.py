
def process_file(main_provider,df_arr4,rate_tbl,df_select,etl,logger):

    df_join = main_provider.join(df_arr4,how="inner",on=["npi","tin"])
    df_newjin = rate_tbl.join(df_select,how="inner",on=["billing_code"] )
    df_newjin.printSchema()


    df_joined = df_newjin.join( df_join,how="inner",on='provider_group_id')


    net_nrpr = 'out_folder/net_nrpr.parquet'
    prv_nrpr = 'out_folder/net_nrpr.parquet'

    df_newjin.write.mode('overwrite').parquet(net_nrpr)
    df_join.write.mode('overwrite').parquet(prv_nrpr)

    return net_nrpr,prv_nrpr