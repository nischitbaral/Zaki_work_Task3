import logging
import sys
import argparse
from etl_nrpr import ETL


def main():
    # zip_file = sys.argv[1]

    parser = argparse.ArgumentParser(description="ETL pipeline for processing ZIP files containing rate and provider data.")
    # parser.add_argument("--zip_file", help="Path to the ZIP file to process")
    # parser.add_argument("--provider_dtl_path", help="Path to tht provider_detail.json")
    parser.add_argument("--zip_filess", help="Path to the ZIP file to process")
    parser.add_argument("--hlt_prv", help="Path to the ZIP file to process")


  


    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO,filename='etl.log',datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger("ETL")

 
        
   
    etl = ETL(logger)
    # etl.execute(args.zip_file,args.provider_dtl_path)
    etl.execute(args.zip_filess,args.hlt_prv)

  
    
    # net_file,prov_path= extract_ano.ext_file(zip_file) 


if __name__ == "__main__":
    main()