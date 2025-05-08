import ijson
import zipfile
import json
from decimal import Decimal 


prov_data = []
net_data =[]
                             
                    
def con_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)



with zipfile.ZipFile('/home/nischit-baral/Desktop/Zaki_work_Task3/MagnaCarePPO_In-Network.zip', 'r') as zf:
        for name in zf.namelist():
            with zf.open(name, 'r') as f:
                print(name)
                for item in ijson.items(f, 'provider_references.item'):
                    prov_data.append(item)
                
                f.seek(0)

                for item in ijson.items(f, 'in_network.item'):
                    net_data.append(item)


with open('prov_tab.json','w') as out_file:
    json.dump(prov_data, out_file, indent=4, default=con_decimal)
    out_file.write('\n')

with open('in_net.json','w') as out_file:
    json.dump(net_data,out_file,indent=4,default=con_decimal)
    out_file.write('\n')





# with zipfile.ZipFile('/home/nischit-baral/Desktop/Zaki_work_Task3/MagnaCarePPO_In-Network.zip', 'r') as zf:
#         for name in zf.namelist():
#             with zf.open(name, 'r') as f:
#                 print(name)
#                 for item in ijson.items(f, 'in_network.item'):
#                     records.append(item)

# with open('exampleeee.json','w') as out_file:
#     json.dump(records,out_file,indent=4,default=con_decimal) + '\n'



 