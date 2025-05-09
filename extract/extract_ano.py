import ijson    
import zipfile
import json
from decimal import Decimal 
import os

def ext_file(zip_file):
    def con_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return obj 
     
    out_file = os.path.join(os.getcwd(),'out_files')
    os.makedirs(out_file, exist_ok=True)

    net_file = os.path.join(out_file, 'net_data.json')
    prov_path = os.path.join(out_file, 'provider.json')

    with zipfile.ZipFile(zip_file, 'r') as zf:
        for name in zf.namelist():
            with zf.open(name, 'r') as f:
                print(name)
                

                with open('prov_path', 'w') as file_new:
                    for item in ijson.items(f, 'provider_references.item'):
                        file_new.write(json.dumps(item,default=con_decimal) + '\n')
                        # out_file.write('\n')
                

                f.seek(0)

                with open('net_file', 'w') as file_agn:
                    for item in ijson.items(f, 'in_network.item'):
                    
                        file_agn.write(json.dumps(item,default=con_decimal) + '\n')

    return net_file,prov_path                    