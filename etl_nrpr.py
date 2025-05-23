
import yaml
from extract import extract_nrpr
from pyspark.sql import SparkSession
from transforms import scrub_nrpr
from process import process
from loads import load_nrpr




class ETL:

          
      def __init__(self,logger):
          with open('new_file.yml', 'r') as file:
            new_yml=  yaml.safe_load(file)  
          
          self.logger = logger
          self.logger.info("Initializing ETL") 

          self.cores = new_yml['SPARK']['EXECUTOR']
          self.dri_mem = new_yml['SPARK']['DRIVER']['MEMORY']
          self.exe_mem = new_yml['SPARK']['EXECUTOR']['MEMORY']
          self.exe_core = new_yml['SPARK']['EXECUTOR']['CORES']
          self.exe_instance = new_yml['SPARK']['EXECUTOR']['INSTANCES']


          self.database = new_yml['POSTGRES']['DATABASE']
          self.host = new_yml['POSTGRES']['HOST']
          self.user = new_yml['POSTGRES']['USER']
          self.password = new_yml['POSTGRES']['PASSWORD']
          self.port = new_yml['POSTGRES']['PORT']



      def execute(self,zip_filess,hlt_prv):
          
        
          self.logger.info("Extract")
          combined_file = extract_nrpr.ext_nrpr(zip_filess)

          self.spark = SparkSession.builder.appName("ETL_nrpr").config("spark.driver.memory", self.dri_mem ).getOrCreate()
      
          self.logger.info("Scrub")
          main_provider,df_arr4,rate_tbl,df_drop,df_select = scrub_nrpr.scr_nrpr(combined_file,hlt_prv,self )

          net_nrpr,prv_nrpr = process.process_file(main_provider,df_arr4,rate_tbl,df_select)
          
          
          self.logger.info("load")
          load_nrpr.loads_file(net_nrpr,prv_nrpr,df_drop,self)
