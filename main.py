import logging
import sys
import argparse
from etl import ETL

def main():
    # zip_file = sys.argv[1]

    parser = argparse.ArgumentParser(description="ETL pipeline for processing ZIP files containing rate and provider data.")
    parser.add_argument("--zip_file",required=True , help="Path to the ZIP file to process")

    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger("ETL")
    etl = ETL(logger)
    etl.execute(args.zip_file)

    # net_file,prov_path= extract_ano.ext_file(zip_file)


if __name__ == "__main__":
    main()