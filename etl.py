
import yaml
from pyspark.sql import SparkSession
from extract import extract_ano
from transforms import scrub
from loads import load



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



    def execute(self,zip_file,provider_dtl_path):
        
       
        self.logger.info("Extract")
        net_file, prov_path = extract_ano.ext_file(zip_file)

        self.spark = SparkSession.builder \
            .appName("ETL") \
            .config("spark.driver.memory", self.dri_mem )\
            .getOrCreate()
       

        self.logger.info("Scrub")
        scrub_nets, scrub_prov = scrub.scrub_file(net_file, prov_path,provider_dtl_path,self)

        self.logger.info("Load")
        load.load_file(scrub_nets, scrub_prov, self)


           

          


