import os
import pandas as pd
import requests
from cloudpathlib import CloudPath
import json
from tqdm import tqdm

def t(ClaimID):
    
    
    ClaimID=str(ClaimID)
    
    #activate live or stagging eve
    url = "https://kedc-api.buddhimed.in/bill-api/v1/bills/get-s3-paths-internal" # link for live : if the claim ids is taken from live eve or is inlive
    
    # url = "https://kedc-staging.ihx.in/ihx/bill-api/v1/bills/get-s3-paths-internal" #staging : if the claim ids is taken from stagging eve or is in stagging
    
    payload="{\"claim_id_list\": [\"" + ClaimID + "\"]}"
    headers = {
      'Content-Type': 'application/json'
    }
    # try:
    tid={}
    response = requests.request("GET", url, headers=headers, data=payload)
    lst=json.loads(response.text)["claim_page_data"][ClaimID]
    # print(lst)
    x=lst[-1]['s3_path'].rsplit('/',1)[0]
    for i in lst:
        tid[i['s3_path'].rsplit('/',1)[0].rsplit('/',1)[1]]=i["s3_path"].split("page_")[0]
    # except:
    #     tid=''
    return lst

if __name__ == "__main__":
    # read the csv file which has claim ids
    one_page = pd.read_csv('/home/balajic/workdir/donut_training_pipeline/donut_finetuning/database/two_pager_cd_r_apr.csv')
    one_page_claims = one_page["claim_id"].unique().tolist()
    print(len(one_page_claims))

    save_path = '/home/balajic/workdir/donut_training_pipeline/donut_finetuning/images/'
    cnt=0
    for j in tqdm(one_page_claims):
        os.chdir(save_path)  #give the path to save the bill images
        try:
            try:
                os.mkdir(str(j))
            except:
                pass
            rs=t(j)
        
            txnids=[]
            for e in rs:
                txnids.append(e["s3_path"].split("/")[-2])
            txn=[]
            for i in txnids:
                if i not in txn:
                    txn.append(i)
            txnids=txn
            dict1={}
            for el in range(len(txnids)):
                dict1[txnids[el]]=el
            cnt+=1
            for i in rs:
                if i['user_input_label']=='LABEL_SUMMARY_BILL' or i['user_input_label']=='LABEL_DETAIL_BILL':
                    cp=CloudPath(i['s3_path'])
                    cp.download_to(save_path + str(j) + '/' +
                                    str(i["s3_path"].split("/")[-2])+"-"+i["s3_path"].split("/")[-1]) #live

        except Exception as e:
            print(j,e)
            pass