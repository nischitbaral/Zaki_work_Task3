import ijson
import zipfile
import pandas as pd


records = []

with zipfile.ZipFile("ignore/MagnaCarePPO_In-Network.zip", 'r') as zf:
    print('open file:')
    for name in zf.namelist():
        print(name)
        with zf.open(name, 'r') as f:
            for item in ijson.items(f, 'in_network.item'):
                records.append(item)
# with zipfile.ZipFile("/home/nischit-baral/Desktop/work/MagnaCarePPO_In-Network.zip", 'r') as zf:
#     print('open file:')
#     for name in zf.namelist():
#         print(name)
#         with zf.open(name, 'r') as f:
#             for item in ijson.items(f, 'provider_references.item'):
#                 records.append(item)

print(records)


rate_df = pd.DataFrame(records)

out_new = '/home/nischit-baral/Desktop/work/rate.json'
rate_df.to_json(out_new,orient='records',indent=3)
# print(f'Wrote {len(rate_df)} rows to {out_new}')
# print(rate_df)