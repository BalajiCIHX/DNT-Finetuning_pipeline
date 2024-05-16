import re
from datetime import datetime
import requests
import logging
alpha_pattern=r'[A-Za-z]'
# import environ
from bill_modules.get_supreme_id import get_id

def get_hospital_id(hospital_id,hospital_df):
    hospital_id=str(hospital_id).split('.')[0]
    if hospital_id in ['nan',""]:
        return 0,'azure_layout','NotSupported'
    else:
        hospital_df["Hospital_ID"]=hospital_df["Hospital_ID"].apply(lambda x:str(x).split('.')[0])
        hospital_df["iciciId"]=hospital_df["iciciId"].apply(lambda x:str(x).split('.')[0])
        
        hosp_row=hospital_df[(hospital_df['Hospital_ID']==hospital_id) | (hospital_df['iciciId']==hospital_id) ]
        if len(hosp_row)>0:
            hospital_id=hosp_row['SupremeId'].tolist()[0]
            ocr_type=hosp_row['Level_0 (Default) (Layout/ Invoice/ Textract)'].tolist()[0]
            hospital_name=hosp_row['Hospital_name'].tolist()[0]
        else:
            hospital_id,ocr_type,hospital_name=0,'azure_layout','NotSupported'
        if str(hospital_id) in ['nan','']:
            hospital_id=0
        hospital_id=str(hospital_id).split('.')[0]
        return hospital_id,ocr_type,hospital_name

def remove_duplicate_column_names(columns):
    duplicated_idx = columns.duplicated()
    duplicated = columns[duplicated_idx].unique()
    rename_cols = []
    i = 1
    for col in columns:
        
        if col in duplicated:
            rename_cols.extend([str(col) + '_' + str(i)])
            i+=1
        else:
            rename_cols.extend([col])
    return rename_cols

def particulars_finding(merged_df):
    sum_cols={}
    for i in merged_df.columns:
        merged_df[i]=merged_df[i].astype('str')
        sum_cols[i]=sum(merged_df[i].str.count(alpha_pattern))
    p_col = max(sum_cols, key= lambda x:sum_cols[x])
    if sum_cols[p_col]==0:
        for i in merged_df.columns:
            merged_df[i]=merged_df[i].astype('str')
            sum_cols[i]=sum(merged_df[i].str.count(r'[A-Za-z0-9 ]'))
    p_col = max(sum_cols, key= lambda x:sum_cols[x])
    return p_col

def particular_validation(merged_df,p_col):
    float_count=0
    flag=False
    floatpattern=r'^(\d+(?:\.\d+)?)$'

    def alpha_cnt(x):
        if len(re.findall(alpha_pattern,str(x).strip()))<3:
            return True
        else:
            return False
    
    float_count=merged_df[p_col].apply(lambda x:bool(re.match(floatpattern,str(x)))).sum()
    alpha_count=merged_df[p_col].apply(lambda x:alpha_cnt(x)).sum()
    di_chem=len(merged_df[merged_df[p_col].str.contains("dial|hemo|pharma|medic")])
    if len(merged_df)>0:
        pert=(len(merged_df)-len(set(merged_df[p_col].tolist())))/len(merged_df)
        if pert>0.75 and len(merged_df)>5 and di_chem<3:
            flag=True
    return float_count,flag,alpha_count
def remove_unnecessary_columns(df):
    for col in df.columns:
        if (str(col).startswith("bb_") or str(col).startswith('conf_')) and str(col) not in ["bb_rowwise","con_rowwise"]:
            df.drop(columns=col,inplace=True)
    return df
def rounding_off(df):
    columns=[i for i in df.columns if i not in ["bb_rowwise","con_rowwise"]]
    for col in columns:
        for ind in df[col].index:
            df.at[ind,col]=str(df.at[ind,col]).lower().strip().rstrip("c").strip()
            if re.match(r'^\d+(\.)\d{3,}$|(^\d+\,\d{3}\.\d{2,})$|^\-\d+(\.)\d{3,}$|(^\-\d+\,\d{3}\.\d{2,})$',df.at[ind,col]):
                a=df.at[ind,col].split('.')
                a1,b1=a[0],a[1]
                df.at[ind,col]=a1+'.'+b1[:2]
    return df
def preprocess(x):
    """
    Removes and corrects some ocr issues
    """
    x=str(x).replace('*',' ').strip()
    x=str(x).replace('+','').strip()
    x=str(x).strip(":")
    x=x.strip(';').strip()
    a=re.sub(r'^O$','0',x,flags=re.IGNORECASE)
    a=re.sub(r'o ',"0 ",a,flags=re.IGNORECASE)
    a=re.sub(r' o'," 0",a,flags=re.IGNORECASE)
    a=re.sub(r'^o$','0',a,flags=re.IGNORECASE)
    a=re.sub(r'^LO$','5',a,flags=re.IGNORECASE)
    a=re.sub(r'^CO$','6',a,flags=re.IGNORECASE)
    a=re.sub(r'^ I $','1',a,flags=re.IGNORECASE)
    a=re.sub(r'^I$','1',a,flags=re.IGNORECASE)
    a=re.sub(r' D ',' 0 ',a,flags=re.IGNORECASE)
    a=re.sub(r'hospo',"",a,flags=re.IGNORECASE)
    if re.match(r'rs\. \d+\.\d{2}',a.lower()):
        a=a.lower().replace("rs. ","")
    return a
def is_unique(s):
    a = s.to_numpy()
    if len(a)>0:
        return (a[0] == a).all() and a[0] in ['nan','']
    else:
        return False
def cleanup(df):
    df.columns=[str(i) for i in df.columns]
    for i in df.columns:
        df[i]=df[i].apply(lambda x:str(x).strip(":").rstrip("-").strip('.').strip())
    df.replace(":","",inplace=True)
    
    return df
def replace_negatives(a,pagerule):
    
    """
    Negative values replaced if pattern match is found
    """

    a=str(a).strip()
    if re.match(r'^(\(\d+(?:\.\d+)?\))$',a):
        a=re.sub(r'[^0-9\.\,\-\;\:\ ]','',a)
        a='-'+str(a)
        pagerule.append(f'negatives added at {a}')
    if re.match(r"^\d{2,}\.\d{3}$",a) and a[-1]!='0':
        a=a[:-1]

    return a
def replace_x(a):
    if re.match("^x$|^×$|^X$",a):
        a=""
    return a

def sort_tables(tables):
    table_dict={}

    cnt=0
    for table in tables:
        boxes=[box for box in table['bb_rowwise'].tolist() if isinstance(box,dict) and len(box)==8]
        if len(boxes)>0:
            table_dict[float(boxes[0]['y1'])]=cnt
            cnt+=1
    tkeys = sorted(list(table_dict.keys()))
    table_dict = {i: table_dict[i] for i in tkeys}
    if (len(table_dict) == len(tables)):
        tables = [tables[i] for i in table_dict.values()]
    return tables

def polishdf(dfa):
    if len(dfa) >0:
        dfa.replace({'':'VNA',' ':'VNA','nan':'VNA',':':'VNA'},inplace=True)
        dfa.fillna('VNA',inplace=True) 
        for i in dfa.index:
            if dfa.at[i,'Before_discount_amount'] == dfa.at[i,'After_discount_amount'] == 'VNA':
                if dfa.at[i,'Unit_price'] != 'VNA' and dfa.at[i,'Quantity'] != 'VNA':
                    try:
                        if 0 < float(str(dfa.at[i, "Quantity"]).replace(",", "")) < 100:
                            dfa.at[i,'After_discount_amount']=dfa.at[i,'Before_discount_amount']=str(round(float(dfa.at[i,"Quantity"])*float(dfa.at[i,'Unit_price']),2))
                    except Exception:
                        pass
                else:
                    dfa.at[i,'After_discount_amount']=dfa.at[i,'Before_discount_amount']='VNA'
                        
    return dfa
def prepare_rules(rule_dict,df,start,list_of_images,ocr_start,ocr_end,hospital_id,hospital_name,ocr_selection,docr_list):
    try:
        if all(df['Header prst'] == 'Yes'):
            header_pr = 'True'
        else:
            header_pr = 'False'
    except Exception:
        header_pr = ''
    end=datetime.now()
    try:
        detail_total = df[df['Table_type']=='Detail'].Before_discount_amount.astype(float).sum()
    except Exception:
        detail_total = 0

    try:
        summary_total = df[df['Table_type']=='Summary'].Before_discount_amount.astype(float).sum()
    except Exception:
        summary_total = 0
    try:
        line_dict = df['Page_no'].value_counts().to_dict()
        page_dict = df.groupby('Table_type').agg({'Page_no':'nunique'}).to_dict()['Page_no']
    except Exception:
        line_dict={}
        page_dict={}
    rule_dict = {str(k):v for k,v in rule_dict.items()}
    if len(list_of_images)>0:
        mongo_dict = {
            'hospid':hospital_id, 
            'ocr_type':ocr_selection,
            'hospital_name':hospital_name,
            'dynamic_ocr_pages':docr_list,
            'num_pages':len(list_of_images),
            'lines': line_dict,
            'pages':page_dict,
            'total_ocr_time': (ocr_end - ocr_start).seconds,
            'total_time':(end - start).seconds,
            'total_summary': summary_total,
            'total_detail':detail_total,
            'header_present':header_pr,
            'rules':rule_dict
                    }   
    else:
        mongo_dict = {
            'hospid':hospital_id, 
            'ocr_type':ocr_selection,
            'hospital_name':hospital_name,
            'dynamic_ocr_pages':docr_list,
            'num_pages':len(list_of_images),
            'lines': line_dict,
            'pages':page_dict,
            'total_ocr_time': 0,
            'total_time':0,
            'total_summary': summary_total,
            'total_detail':detail_total,
            'header_present':header_pr,
            'rules':rule_dict
                    } 
    return mongo_dict
def call_ocr_matchmaker(image):
    environ.Env.read_env()
    env = environ.Env()
    url = env('OCR_MATCH')

    files={
    'image': open(image,'rb')
    }
    headers = {
    'accept': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, files=files, verify=False)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"API FAILED {response.status_code}")
            return response.content
    except Exception as e:
        print("API FAILED")
        logging.info(f'API Failed due to {str(e)}')
        return ""

def particulars_correction(df,dfa):
    p_ign=['fdp', 'pbf', 'pct', 'pop', 'bmw', 'mop', 'dmo', 's/e', 'ggt', 'rmo', 'ip', 'rmt', 'idh', 'esr', 'cbp', 'cdc', 'bsf', 'bd', 'clr', 'tlc', 'usg', 'lb', 'sp', 'icu', 'hct', 'na+', 'tft', 'rbs', 'pvt', 'oae', 'alk', 'vbg', 'ppe', 'cef', 'crp.', 'pcv', 'hdu', 'la', 'bun', 'iv', 'hb', 'pan', 'pph', 'rl', 'tc', 'loh', 'pad', '1b', 'ros', 'ric', 'bss', 'tea', 'eow', 'cbg', 't4', 'ecg', 'his', 'lab', 'cbs', 'rds', 'eur', 'mg', 'ns', 'ver', 'niv', 'as', 'ift', 'cal', 'mvr', 'k+', 'ct', 'nsg', 'dns', 'cap', 'ed3', 'gbp', 'cpt', 'il6', 'co2', 'nhc', 'adg', 'mch', 'sot', 'cag', 'tti', 'cd', 'hot', 'ada', 'ch', 'tab', 'bnp', 'mrd', 'eeg', 'bbp', 'pcn', 'mrv', 'ivf', 'ras', 'tt4', 'cl', 'rd,', 'er', 'bt', 'iu', 'abg', 'hgm', 'cpk', 'ccu', 'psa', 'ncv', 'sub', 'ecg.', 'nst', 'tt3', 'c3r', 'ama', 'ffp', 'hmd', 'non', 'lft',  'igg', 'hpe', 'mp', 'pt', 'eto', 'mri', 'rft', 'max', 'inj', 'mcv', 'hgt', 'tsh', 'afb', 'gen', 'cg', 'nh', 'cue', 'a/c', 'fbs', 'bed', 'urs', 'kft', '1cu', 'ml', '25d', 'pac', 'dc', 'cvp', 'let', 'mvi', 'hcv', 'ldh', '1-6', 'hiv', 'ivp', 'tax', 'set', 'cg4', 'nie', 'crp', 'bsl', 'ls', 'an', 'srl', 'dal', 'cbc', 'vd', 'ft4', 'eua', 'eco', 'usv', 'cct', 'hb%', 'ctg', 'p+d', 'ft3', 'c/s', 'bst','il6','tb',"bsr","ent",'m.v.i']
    dfa=dfa.reset_index(drop=True)
    df=df.reset_index(drop=True)
    if len(df)==len(dfa):
        for index in dfa.index:
            
            if dfa.at[index,"Particular"] not in p_ign:
                if len(re.findall(alpha_pattern,str(dfa.loc[index,'Particular']).strip()))<4 or re.search(r'\d+(\-|/)\d+(\-|/)\d+',str(dfa.loc[index,'Particular']).strip())!=None:
                    lst=df.drop(columns=['bb_rowwise',"con_rowwise"]).loc[index,:].values.tolist()   
                    lst=[i for i in lst if str(i)!="nan"]
                    counter=[(len(re.findall(r'[A-Za-z- ]',ele))) for ele in lst]
                    if len(counter)>0:
                        cter=counter.index(max(counter))
                        if max(counter)>0:
                            logging.info(f"Invalid Particular {dfa.at[index,'Particular']}")
                            
                            dfa.loc[index,'Particular']=lst[cter]
            if re.match(r'^\d+$|^\d+(\.)\d+$',str(dfa.loc[index,'Particular']).strip()) or re.search(r'\d+(\-|/)[A-Za-z]{3,}(\-|/)\d+',str(dfa.loc[index,'Particular']).strip())!=None:
            
                lst=df.drop(columns=['bb_rowwise',"con_rowwise"]).loc[index,:].values.tolist()
                lst=[i for i in lst if str(i)!="nan"]
                counter=[(len(re.findall(r'[A-Za-z- ]',ele))) for ele in lst]
                if len(counter)>0:
                    cter=counter.index(max(counter))
                    logging.info(f"Invalid Particular {dfa.at[index,'Particular']}")
                    
                    dfa.loc[index,'Particular']=lst[cter]
        
    return dfa
def call_lm(img_path):
    logging.info('Calling lm')
    import random
    environ.Env.read_env()
    env = environ.Env()
    url = env('OCRT_URL')
    params={"IHXTrxID":str(random.randint(0,10000000000000000000))}
    files={
    'image': open(img_path,'rb')
    }
    headers = {
    'accept': 'application/json',
    }
    response = requests.request("POST", url, headers=headers, params=params, files=files)
    if response.status_code==200:
        return response.json()
    else:
        logging.info(f"blm api failed {response.status_code} {response.content} {img_path}")
        return {}
    
def remove_bug_from_amt_cols(i,pagerule):
    i=str(i)

    i=re.sub(r'[^0-9\.\,\-\;\:\ \=]','',i)
    i=i.rstrip('.').rstrip('-').strip()
    i=i.replace('--',"")
    if re.match(r"^\d+\.\d\s\d$",i):
        i=i.replace(" ","")
        pagerule.append(f'empty space removed at {i}')
    if i.count('.')>2:
        i=re.sub(r'\.\.+',r'',i) #Replacing the dots where more than 2 dots are present together to empty string
        pagerule.append(f'multiple dots removed at {i}')
    if bool(re.match(r'^\d+[,.]\d{3}[=-]\d{2}$|^\d+[=-]\d{2}$',i))==True:
        i=i.replace('=','.')
        i=i.replace('-','.')
    if bool(re.match(r"\d{,2}[-./]\d{,2}[-./]\d{4}",i))==True:
        a="Date"#Replacing date format with Date string
        pagerule.append('Date is identified in amount')
    elif bool(re.match(r"^\d{1,2}\,\d{3}\.\d{2}\ [;:]$",i))==True: 

        a=re.sub(r"^(\d{1,2})(\,)(\d{3})(\.)(\d{2})(\ )[;:]$",r'\1\3\4\5',i) # Replacing a case where '; or :' is #present at the end with a space
    elif re.match(r'^\d{1,2}(\,)\d{3}(\.)\d{2}(\ )\d$',i):
        a=re.sub(r'^(\d{1,2})(\,)(\d{3})(\.)(\d{2})(\ )(\d)$',r'\1\3\4\5',i)            
    
    elif bool(re.match(r"^\d{1,5}\ [;:]$",i))==True:

        a=re.sub(r"^(\d{1,5})\ [;:]$",r'\1',i)# Replacing a case where '; or :' is #present at the end with a space
    elif bool(re.match(r'^\d{1,4}\. \.\d{2}$',i))==True:
        a=re.sub(r'^(\d{1,4})(\. )(\.)(\d{2})$',r'\1\3\4',i)
    elif bool(re.match(r'^\d{1,5}\.\d{2}\ \d',i))==True:
        a=re.sub(r'^(\d{1,5}\.\d{2})(\ )(\d)',r'\1',i)
    elif re.match(r'^\d+\.\d\.\d$',i):
        a=i.rsplit(".",1)[0]
    elif re.match(r'^\d+\.\d{6}$',i):
        a=str(i.split('.')[0]) + '.' + str(i.split('.')[1])[:2]

    else:
        a=re.sub(r'\-\-+',r'',i) #Replacing double - strings (more than or equal to 2) to empty string
        a=re.sub(r'[\:\,\;\.\]]\B',r'',a) #Replace with empty strings based on word boundary
        a=re.sub(r'[\s\:\,\;](?=\d{1,2})(\b|\s)' , r'.',a) #Replacing with . based on the position of numerics
        a=re.sub(r'[\s\:\.\;](?=\d\d[^\b])' , r'' , a) #Replacing with dot
        a=re.sub(r'[^0-9\.\s\-]',r'',a)
    
    return a

def merge_separator(i,pagerule):
    i=str(i).strip()
    i=i.replace('--',"")
    i=re.sub(r'\-\ ',r'-',i)
    i=re.sub(r'[^0-9\.\,\-\;\:\ \=]','',i)
    i=i.rstrip('.').rstrip('-').strip('.').strip(":").strip("=").strip().rstrip(",").strip().rstrip("|").rstrip(",").strip()
    if re.match(r"^\d+(\,)\s\d{3}(\.)\d{2}$",i):
        i=i.replace(" ","").replace(",","")
    if ' ' in i:
        lst1=i.split()
        if len(lst1)==2:
            if len(lst1[-1])<=2:
                a=remove_bug_from_amt_cols(i,pagerule)
            else:
                if len(lst1[0])==1:
                    a=i
                else:
                    a1=remove_bug_from_amt_cols(lst1[0],pagerule)
                    a2=remove_bug_from_amt_cols(lst1[1],pagerule)
                    a=a1+' '+a2
        elif bool(re.match(r"^\d{1,3}\ \d{2,3}\ \d{2,3}$",i))==True:
            a=remove_bug_from_amt_cols(i,pagerule)
        elif re.match(r'^\d+(\ .|\. )\d{2}$',i):
            a=i.replace(" ","")
        else:
            a=i
    else:
        a=remove_bug_from_amt_cols(i,pagerule)
    if bool(re.match(r"\d{,2}[-./]\d{,2}[-./]\d{4}",i))==True:
        a="Date"
    
    return a

    
def get_standard_quantity(row):
    def decide_qty(qty,std_qty):
        try:
            qty=float(qty)
            std_qty=float(std_qty)
        except Exception as e:
            print(e)
            std_qty=qty
        if std_qty>qty:
            return round(qty*std_qty,2)
        else:
            return qty
    try:
        if row['l2']=='medicines/drugs':
            if re.search(r"(\d+\'s)|(\d+\'\ss)",row['Particular']):
                std_qty=re.search(r"(\d+\'s)|(\d+\'\ss)",row['Particular']).group().split("'")[0].strip()
            elif row['Particular'].endswith("'5"):
                std_qty=row['Particular'].strip("'5").strip()[-1]
            elif re.search(r'\d+s\s(capsules|tab)',row['Particular']):
                std_qty=re.search(r'\d+s\s(capsules|tab)',row['Particular']).group().split('s ')[0]
            elif re.search(r'\(\d+x\d+\)',row['Particular']):
                std_qty=re.search(r'\(\d+x\d+\)',row['Particular']).group().split('x')[-1].strip(')')
            elif re.search(r'(cap|tab)\s\d+\s(×|x)\s\d+',row['Particular']):
                std_qty=re.search(r'(cap|tab)\s\d+\s(×|x)\s\d+',row['Particular']).group().split('x')[-1].split('×')[-1].strip()
            elif re.search(r'(cap|tab)\s\d+s',row['Particular']):
                std_qty=re.sub(r'[^0-9]',"",re.search(r'(cap|tab)\s\d+s',row['Particular']).group())

            else:
                std_qty=row['Quantity']
            return decide_qty(row['Quantity'],std_qty)
        else:
            return row['Quantity']
    except Exception as e:
        print(e)
        return row['Quantity']

def apply_std_changes(df):
    for i in df.index:
        try:
            if df.at[i,"l2"] == "medicines/drugs" and round(float(df.at[i,"Before_discount_amount"])/float(df.at[i,'Quantity']))!=round(float(df.at[i,"Unit_price"])):
                df.at[i,'Unit_price']=str(round(float(df.at[i,"Before_discount_amount"])/float(df.at[i,'Quantity']),2))
        except Exception:
            pass
    return df
def ignore_omm(hosp_id):
    hosp_id=str(hosp_id).split('.')[0]
    ignore_ids=get_id.apollo_hospital()+get_id.fortis()+get_id.manipal()+get_id.yatharth()+get_id.hcg()+get_id.amritha()+get_id.kg_hospital()
    
    ignore_ids.append("123")

    if hosp_id in ignore_ids:
        return False
    else:
        return True
