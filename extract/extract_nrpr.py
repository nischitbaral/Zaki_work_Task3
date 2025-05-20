import zipfile
import gzip
import json
import os


def ext_nrpr(zip_filess):
    
    output_folder = os.path.join(os.getcwd(), 'out_folder')
    os.makedirs(output_folder, exist_ok=True)

    combined_file = os.path.join(output_folder, 'combined_output.json')

   
    all_data = []

   

    print("Extracting main ZIP file...")
    with zipfile.ZipFile(zip_filess, 'r') as zip_ref:
        zip_ref.extractall(output_folder)

    for root, dirs, files in os.walk(output_folder):
        for file in files:
            if file.endswith('.gz'):
                gz_path = os.path.join(root, file)
                print("Reading:", gz_path)

                with gzip.open(gz_path, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.append(data)

    with open(combined_file, 'w', encoding='utf-8') as out_file:
        json.dump(all_data, out_file, indent=2)

    return combined_file





