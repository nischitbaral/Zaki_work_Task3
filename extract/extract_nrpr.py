import zipfile
import gzip
import json
import os

def ext_nrpr(zip_filess):
    output_folder = os.path.join(os.getcwd(), 'out_folder')
    os.makedirs(output_folder, exist_ok=True)

    combined_file = os.path.join(output_folder, 'combined_output.json')
    all_data = []

    for filename in os.listdir(zip_filess):
        if filename.endswith('.gz'):
            gz_path = os.path.join(zip_filess, filename)

            with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                all_data.append(data)

    with open(combined_file, 'w', encoding='utf-8') as out_file:
        json.dump(all_data, out_file, indent=2)
        out_file.write('\n')

    return combined_file


