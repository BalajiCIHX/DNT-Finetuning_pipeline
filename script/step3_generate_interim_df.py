import os
import pandas as pd
import json
from tqdm import tqdm

dataframe = pd.read_csv('/home/balajic/workdir/donut_training_pipeline/donut_finetuning/database/one_pager_cd_r_marandapr.csv')
dataframe2 = pd.read_csv('/home/balajic/workdir/donut_training_pipeline/donut_finetuning/database/two_pager_cd_r_apr.csv')

def move_function(claim):
    print(claim)
    root = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/images/'
    ocr_root = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/ocr/'
    page_names = os.listdir(root + claim)
    for page_name in page_names:
        
        page = page_name.split('-')[-1]
        txn_id = page_name.replace(f'-{page}', '')
        try:
            ocr_path = ocr_root + claim + '/' + txn_id + '/ocr_out.json'
            if os.path.isfile(ocr_path):
                pass
            else:
                try:
                    cd_id = dataframe[dataframe['claim_id']==claim]['id'].tolist()[0]
                except:
                    cd_id = dataframe2[dataframe2['claim_id']==claim]['id'].tolist()[0]
                txn_id = cd_id
                ocr_path = ocr_root + claim + '/' + txn_id + '/ocr_out.json'
            pocr = json.load(open(ocr_path))
        except:
            print('ocr not found for txn_id', txn_id, 'with claim_id', claim, page_name)
            continue
        
        try:
            keys = {}
            for key in pocr:
                keys[list(key.keys())[0].split('/')[-1]] = list(key.keys())[0]
            if page in keys:
                for idx in range(len(pocr)):
                    if keys[page] in pocr[idx].keys():
                        page_ocr = pocr[idx][keys[page]]
                        break
            # op, rule_dict, ocr_response = bill_digit_module_([page], 12345, claim, page_ocr, root, txn_id)
            bill_digit_module_([page], 12345, claim, page_ocr, root, txn_id)
        except Exception as e:
            print(e, "for txn_id", txn_id, "of claim_id", claim, page_name)
            pass
    return None


if __name__ == '__main__':

    os.chdir('/home/balajic/workdir/donut_training_pipeline/script/')
    from bill_modules.main_dontcallocr import bill_digit_module_
    os.environ["AZURE_KEY"] = "d4379cb807734cf0ad2bac054f0f5834"
    # os.environ["AZURE_ENDPOINT"] = "https://bm-form.cognitiveservices.azure.com/"
    os.environ["CONFIG_DIR"]="/home/balajic/"
    # os.environ['OCR_MATCH']='http://20.193.157.32:8888/OMM/v1/predict-ocr'
    os.environ["PYTHONWARNINGS"]="ignore"
    # os.environ['OCRT_URL']='http://20.193.157.32:8881/OCRT/v1/image'
    import warnings
    warnings.filterwarnings("ignore")
    import logging
    logging.basicConfig(
        filename='/home/balajic/workdir/donut_training_pipeline/script/bill_ex_finetuning1.out',
        level=logging.INFO, 
        format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    root = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/images/'
    ocr_root = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/ocr/'

    save_path = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/interim_df/'
    claims = os.listdir(root)
    # claims = ['22625411']

    # introduce multi-processing here
    from multiprocessing import Pool
    p = Pool(16)
    p.map(move_function, claims)
    p.close()
    p.join()

    print("Successfully generated all the intermediate data frames.")