import sys
from extract import extract_ano
from transforms import scrub
from loads import load

def main():
    zip_file = sys.argv[1]
    net_file,prov_path= extract_ano.ext_file(zip_file)
    scrub_nets,scrub_prov = scrub.scrub_file(net_file,prov_path)
    load.load_file(scrub_nets,scrub_prov )

if __name__ == "__main__":
    main()