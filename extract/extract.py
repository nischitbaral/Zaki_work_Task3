import ijson
import zipfile
import json
import decimal as Decimal 

records = []
 
                    
                    
                    
def con_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)


# with open('example.json','w') as out_file:
#     with zipfile.ZipFile('/home/nischit-baral/Desktop/Zaki_work_Task3/MagnaCarePPO_In-Network.zip', 'r') as zf:
#         for name in zf.namelist():
#             with zf.open(name, 'r') as f:
#                 print(name)
#                 for item in ijson.items(f, 'provider_references.item'):
#                     records.append(item)

# with open('example.json','w') as out_file:
#     out_file.write(jsonZaki_work_Task3.dumps(records) + '\n')





with zipfile.ZipFile('/home/nischit-baral/Desktop/Zaki_work_Task3/MagnaCarePPO_In-Network.zip', 'r') as zf:
        for name in zf.namelist():
            with zf.open(name, 'r') as f:
                print(name)
                for item in ijson.items(f, 'in_network.item'):
                    records.append(item)

with open('example.json','w') as out_file:
    out_file.write(json.dumps(records,default=con_decimal) + '\n')



 