from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import FormRecognizerClient
import os
import boto3
import numpy as np   
import sys
import ast
from numpy import isin
import pandas as pd
import json
from PIL import Image
from bill_modules.ocr_utils import ocr_utils
from bill_modules.utils import remove_duplicate_column_names,call_lm
import time
import requests
class ocr:
    def __init__(self, image_path, ocr_type, ocr_response):
        """Loading and reading images"""
        self.image_path = image_path
        self.image=Image.open(image_path)
        self.size=self.image.size
        self.ocr_type=ocr_type
        self.ocr_response=ocr_response

    def azure_layout(self):
        """Calling azure layout"""

        # endpoint = os.environ['AZURE_ENDPOINT'] #Required for accessing sdk
        # key = os.environ['AZURE_KEY'] #Required for accessing sdk
        # form_recognizer_client = FormRecognizerClient(endpoint=endpoint, credential=AzureKeyCredential(key))
        # with open(self.image_path, "rb") as f:
        #     poller = form_recognizer_client.begin_recognize_content(form=f)
        # form_pages = poller.result() #azure_layout results

        form_pages = self.ocr_response #azure_layout results

        # raw_output=form_pages.to_dict() #get raw output
        raw_output = form_pages
        main_dfas=[]
        # print(form_pages)


        for table in form_pages['tables']:
            #creating 3 tables for conf score, bbox and text
            table_=pd.DataFrame()
            table_bb_=pd.DataFrame()
            table_conf_=pd.DataFrame()
            for cell in table['cells']:
                table_.loc[cell['row_index'], cell['column_index']]=cell['text'] #populate text
                if isinstance(cell['field_elements'], list):
                    # print([word['confidence'] for word in cell['field_elements']])
                    try:
                        table_conf_.loc[cell['row_index'], cell['column_index']]=round(sum([word['confidence'] for word in cell['field_elements']])/len([word['confidence'] for word in cell['field_elements']]),3)
                    except:
                        table_conf_.loc[cell['row_index'], cell['column_index']]=""
                else:
                    table_conf_.loc[cell['row_index'], cell['column_index']]=""

                table_bb_.loc[cell['row_index'], cell['column_index']]=str(cell['bounding_box']) #populate bounding box
            table_conf_.fillna("", inplace=True)
            table_=table_[sorted(table_.columns)]
            table_bb_=table_bb_[sorted(table_bb_.columns)] #sorting based on column names
    
            table_conf_ = table_conf_[sorted(table_conf_.columns)]

            for col in table_bb_.columns:
                #convert table to std format
                table_bb_.rename(columns={col:'bb_'+str(col)},inplace=True)
                table_bb_['bb_'+str(col)]=table_bb_['bb_'+str(col)].apply(lambda x:ocr_utils.bb_convert_layout(x))
                table_conf_.rename(columns={col:'conf_'+str(col)},inplace=True)
            table_bb_=ocr_utils.row_wise_bb(table_bb_)
            for col in table_bb_.columns:
                table_bb_[col]=table_bb_[col].apply(lambda x:ocr_utils.dict_bb(x))
            
            table_=pd.concat([table_,table_bb_,table_conf_],axis=1)
            table_.fillna("",inplace=True)
            table_.columns=remove_duplicate_column_names(table_.columns)
            main_dfas.append(table_)
        if form_pages['lines']!=None:
            lines=[{'text':i['text'],'bounding_box':ocr_utils.dict_bb(ocr_utils.bb_convert_layout(i['bounding_box']))} for i in form_pages['lines']]
        else:
            lines=[]
        #creating dict
        final_report={}
        final_report['ocr_type']='azure_layout'
        final_report['image_path']=self.image_path
        final_report['tables']=main_dfas
        final_report['lines']=lines
        return raw_output,final_report

    
    def aws_textract(self):
            width,height=self.size
            # with open(self.image_path, 'rb') as document:
            #     imageBytes = bytearray(document.read())
            # textract = boto3.client('textract',region_name='ap-south-1')
            # response = textract.analyze_document(Document={'Bytes': imageBytes},FeatureTypes=["TABLES"]) #get textract output json
            # response=json.load(open(f"/mnt/ihxaidata01/balajic/evdata/nov4/ocr/aws/{self.image_path.split('/')[-1].replace('jpeg','json')}"))
            response=self.ocr_response
            def get_rows_columns_map(table_result, blocks_map):
                
                """
                mapping the rows based on the json format
                """
                rows,rows_bb,rows_cf = {},{},{}
                for relationship in table_result['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            cell = blocks_map[child_id]
                            if cell['BlockType'] == 'CELL':
                                row_index = cell['RowIndex']
                                col_index = cell['ColumnIndex']
                                if row_index not in rows:
                                    # create new row
                                    rows[row_index] = {}
                                    rows_bb[row_index] = {}
                                    rows_cf[row_index] = {}

                                # get the text value
                                rows[row_index][col_index] = get_text(cell, blocks_map)
                                rows_bb[row_index][col_index] = get_bb(cell, blocks_map)
                                rows_cf[row_index][col_index] = get_cf(cell, blocks_map)


                return rows,rows_bb,rows_cf
            def get_text(cell, blocks_map):
                """Mapping text"""
                text= " "
                if 'Relationships' in cell: #result
                    for relationship in cell['Relationships']:
                        if relationship['Type'] == 'CHILD':
                            for child_id in relationship['Ids']:
                                word = blocks_map[child_id]
                                if word['BlockType'] == 'WORD':
                                    text += word['Text'] + ' '
                                if word['BlockType'] == 'SELECTION_ELEMENT':
                                        if word['SelectionStatus'] =='SELECTED':
                                            text +=  'X ' 
                return text


            def get_bb(cell, blocks_map):
                """Mapping bbox"""
                text= " "
                if 'Relationships' in cell: #result
                    for relationship in cell['Relationships']:
                        if relationship['Type'] == 'CHILD':
                            for child_id in relationship['Ids']:
                                word = blocks_map[child_id]
                                if word['BlockType'] == 'WORD':
                                    text = word['Geometry']["BoundingBox"]
                                if word['BlockType'] == 'SELECTION_ELEMENT':
                                        if word['SelectionStatus'] =='SELECTED':
                                            text +=  'X ' 
                return text


            def get_cf(cell, blocks_map):
                """Mapping conf score"""

                text= " "
                conf_sc = 100
                if 'Relationships' in cell: #result
                    for relationship in cell['Relationships']:
                        if relationship['Type'] == 'CHILD':
                            for child_id in relationship['Ids']:
                                word = blocks_map[child_id]
                                if word['BlockType'] == 'WORD':
                                    if  conf_sc > word['Confidence']:
                                        conf_sc = word['Confidence']
                                        text = word['Confidence']
                                if word['BlockType'] == 'SELECTION_ELEMENT':
                                        if word['SelectionStatus'] =='SELECTED':
                                            text +=  'X ' 
                return text
            def generate_table_csv(table_result, blocks_map):
                """Generate tables"""
                rows,rows_bb,rows_cf = get_rows_columns_map(table_result, blocks_map)
                def get_table(dic):
                    for i in dic.keys():
                        col_num = len(dic[i])

                    df_rows=pd.DataFrame(columns=list(range(1,col_num+1)))

                    for i in dic.keys():
                        df_rows = df_rows.append(dic[i],ignore_index=True)
                    col_list = df_rows.iloc[0,:].tolist()
                    df_rows.columns = col_list
                    return df_rows
                def get_table_cb(dic):
                    for i in dic.keys():
                        col_num = len(dic[i])

                    df_rows=pd.DataFrame(columns=list(range(1,col_num+1)))

                    for i in dic.keys():
                        df_rows = df_rows.append(dic[i],ignore_index=True)
                    return df_rows

                df_rows=get_table(rows)
                df_bb_rows=get_table_cb(rows_bb)
                df_bb_rows.columns=[f'bb_{str(idx+1)}' for idx in range(len(df_bb_rows.columns))]
                
                df_cf_rows=get_table_cb(rows_cf)
                df_cf_rows.columns=[f'conf_{str(idx+1)}' for idx in range(len(df_cf_rows.columns))]


                return df_rows,df_bb_rows,df_cf_rows
            block = response["Blocks"]
            blocks_map = {}
            table_blocks,line_blocks = [],[]


            for i in block:
                blocks_map[i['Id']] = i
                if i['BlockType'] == "TABLE":
                    table_blocks.append(i)
                if i['BlockType'] == "LINE":
                    line_blocks.append(i)

            if len(table_blocks) <= 0:
                return {},{'ocr_type':'aws_textract','image_path':self.image_path,'tables':[],'lines':[]}
            if len(line_blocks) <= 0:
                return {},{'ocr_type':'aws_textract','image_path':self.image_path,'tables':[],'lines':[]}
            #Get text
            lines=[{'text':line['Text'],'bounding_box':ocr_utils.dict_bb(ocr_utils.bb_convert_aws(line["Geometry"]['BoundingBox'],height,width))} for line in line_blocks]
            
            lis_df = []
            for table in table_blocks:
                """Converting into std format"""
                df_rows,df_bb_rows,df_cf_rows=generate_table_csv(table, blocks_map)

                for col in df_bb_rows.columns:
                    df_bb_rows[col]=df_bb_rows[col].apply(lambda x:ocr_utils.bb_convert_aws(x,height,width))
                df_bb_rows=ocr_utils.row_wise_bb(df_bb_rows)
                if 'bb_rowwise' not in df_bb_rows.columns:
                    df_bb_rows['bb_rowwise']=""
                for col in df_bb_rows.columns:
                    df_bb_rows[col]=df_bb_rows[col].apply(lambda x:ocr_utils.dict_bb(x))
                table_=pd.concat([df_rows,df_bb_rows,df_cf_rows],axis=1)
                table_.columns = remove_duplicate_column_names(table_.columns)
                lis_df.append(table_)

            final_report={}
            final_report['ocr_type']='aws_textract'
            final_report['image_path']=self.image_path
            final_report['tables']=lis_df
            final_report['lines']=lines
            return response,final_report
    def azure_invoice(self):
        def analyzeInvoice(filename):
            invoiceResultsFilename = filename + ".invoice.json"

            # do not run analyze if .invoice.json file is present on disk
            if os.path.isfile(invoiceResultsFilename):
                with open(invoiceResultsFilename) as json_file:
                    return json.load(json_file)

            # Endpoint URL
            endpoint = os.environ['AZURE_ENDPOINT']
            key= os.environ['AZURE_KEY']

            #The URL is called directly
            post_url = endpoint + "/formrecognizer/v2.1/prebuilt/invoice/analyze?includeTextDetails=true"
            headers = {
                # Request headers
                'Content-Type': 'application/octet-stream',
                'Ocp-Apim-Subscription-Key': key,
            }

            params = {
                "includeTextDetails": True
            }

            with open(filename, "rb") as f:
                data_bytes = f.read()

            try:
                resp = requests.post(url = post_url, data = data_bytes, headers = headers, params = params)
                if resp.status_code != 202:
                    return None
                get_url = resp.headers["operation-location"]
            except Exception as e:
                return None

            n_tries = 50
            n_try = 0
            wait_sec = 6

            while n_try < n_tries:
                try:
                    resp = requests.get(url = get_url, headers = {"Ocp-Apim-Subscription-Key": key})
                    resp_json = json.loads(resp.text)
                    if resp.status_code != 200:
                        return None
                    status = resp_json["status"]
                    if status == "succeeded":
                        return resp_json
                    if status == "failed":
                        return None
                    time.sleep(wait_sec)
                    n_try += 1     
                except Exception as e:
                    msg = "GET analyze results failed:\n%s" % str(e)
                    return None

            return resp_json
        # file_path=self.image_path
        # raw_output = analyzeInvoice(file_path) #Getting json output
        raw_output=self.ocr_response
        table_=pd.DataFrame()
        table_conf=pd.DataFrame()
        table_bb=pd.DataFrame()
        try:
            for idx,item in enumerate(raw_output['analyzeResult']['documentResults'][0]['fields']['Items']['valueArray']):
                if 'valueObject' in item.keys():
                    for col in item['valueObject'].keys():
                        table_.loc[idx,col]=item['valueObject'][col]['text']
                        table_conf.loc[idx,col]=item['valueObject'][col]['confidence']
                        table_bb.loc[idx,col]=str(item['valueObject'][col]['boundingBox'])
            columns=ocr_utils.table_sorter(table_.columns)
            table_=table_[columns]
            table_conf=table_conf[columns]
            table_conf.columns=["conf_"+str(col) for col in columns]
            table_bb=table_bb[columns]
            table_bb.columns=["bb_"+str(col) for col in columns]
            table_bb=ocr_utils.row_wise_bb(table_bb)
            for bb in table_bb.columns:
                table_bb[bb]=table_bb[bb].apply(lambda x:ocr_utils.dict_bb(x))
            table_=pd.concat([table_,table_bb,table_conf],axis=1)
            table_.fillna("",inplace=True)
        except Exception as e:
            print(e)
            pass
        final_report={}
        final_report['ocr_type']='azure_invoice'
        final_report['image_path']=self.image_path
        final_report['tables']=[table_] #only one table per image / file
        final_report['lines']=[] #text is not returned in invoice
        return raw_output,final_report
    
    def blm(self):
        d=call_lm(self.image_path)
        main_dfas=[]
        if d!={}:
            for page in d:
                for table in page['tables']:
                    table_=pd.DataFrame()
                    table_bb_=pd.DataFrame()
                    table_conf_=pd.DataFrame()
                    for cell in table['cells']:
                        table_.loc[cell['row_index'],cell['column_index']]=cell['text']
                        table_bb_.loc[cell['row_index'],cell['column_index']]=str(cell['bounding_box'])
                        table_conf_.loc[cell['row_index'],cell['column_index']]=cell['confidence']
                    table_conf_.fillna("",inplace=True)
                    table_=table_[sorted(table_.columns)]
                    table_bb_=table_bb_[sorted(table_bb_.columns)]
                    table_conf_=table_conf_[sorted(table_conf_.columns)]
                    for b in table_bb_.columns:
                        table_bb_.rename(columns={b:'bb_'+str(b)},inplace=True)
                        table_bb_['bb_'+str(b)]=table_bb_['bb_'+str(b)].apply(lambda x:ocr_utils.bb_convert_layout(x))
                        table_conf_.rename(columns={b:'conf_'+str(b)},inplace=True)
                    table_bb_=ocr_utils.row_wise_bb(table_bb_)
                    if 'bb_rowwise' in table_bb_.columns:
                        table_bb_["bb_rowwise"]=table_bb_["bb_rowwise"].apply(lambda x:ocr_utils.dict_bb(x))
                    dfa=pd.concat([table_,table_bb_,table_conf_],axis=1)
                    main_dfas.append(dfa)
            if isinstance(d,list) and len(d)>0:
                raw_output=d[0]
            else:
                raw_output=d
            final_report={}
            final_report['ocr_type']='blm_layout'
            final_report['image_path']=self.image_path
            final_report['tables']=main_dfas
            final_report['lines']=[]

        else:
            raw_output={}
            final_report={'ocr_type':'blm_layout','image_path':self.image_path,'tables':[],'lines':[]}
        return raw_output,final_report

        
