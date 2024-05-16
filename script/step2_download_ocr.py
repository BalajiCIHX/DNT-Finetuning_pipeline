from cloudpathlib import CloudPath
import os
import pandas as pd
from tqdm import tqdm

import boto3
import botocore

client = boto3.client('s3')
def checkPath(file_path):
  result = client.list_objects(Bucket="maartifact", Prefix=file_path )
  exists=False
  if 'Contents' in result:
      exists=True
  return exists

if __name__ == '__main__':
    df = pd.read_csv('/home/balajic/workdir/donut_training_pipeline/donut_finetuning/database/two_pager_cd_r_apr.csv')
    claims = df.claim_id.unique().tolist()
    save_path = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/ocr/'

    for claim in tqdm(claims):
        claim_df = df[df['claim_id']==claim]
        claim_df.reset_index(inplace=True)
        txns = claim_df['txn_id'].unique().tolist()
        if checkPath(f'raw_ocr_records/{txns[0]}'):
            pass
        else:
            txns = claim_df['id'].unique().tolist()
            if checkPath(f'raw_ocr_records/{txns[0]}'):
                pass
            else:
                continue
        for txn in txns:

            try:
                os.mkdir(f'{save_path}/{claim}')
            except:
                pass
            try:
                os.mkdir(f'{save_path}/{claim}/{txn}')
            except:
                pass
            
            cp = CloudPath(f's3://maartifact/raw_ocr_records/{txn}')

            cp.download_to(f'{save_path}/{claim}/{txn}')
        # break