import numpy as np
import re
import logging
def rescue_rules(dfa,df,ttc,f_header,pc_flag,pagerule):

    amount_cnt,cont,date_count,zero_count,sr_count,mer_count,d_count,len_count=0,0,0,0,0,0,0,0
    floatpattern=r'^(\d+(?:\.\d+)?)$'
    
    dfa['Before_discount_amount']=dfa['Before_discount_amount'].astype('str')
    dfa['After_discount_amount']=dfa['After_discount_amount'].astype('str')
    qty_flag=False
    for i in dfa.columns:
        dfa[i]=dfa[i].apply(lambda x:str(x).strip())

    for ind in dfa.index:
        if ((dfa.at[ind,'Quantity']==dfa.at[ind,'Unit_price']) or (dfa.at[ind,'Quantity']==dfa.at[ind,'Before_discount_amount']) \
             or (dfa.at[ind,'Quantity']==dfa.at[ind,'After_discount_amount'])) and dfa.at[ind,'Quantity']!='VNA':
            logging.info(f"Invalid Quantity line {ind} value {dfa.at[ind,'Quantity']}")
            pagerule.append(f"Invalid Quantity line {ind} value {dfa.at[ind,'Quantity']}")

            cont+=1
        if ((dfa.at[ind,'Discount']==dfa.at[ind,'After_discount_amount']) or (dfa.at[ind,'Discount']==dfa.at[ind,'Before_discount_amount'])) and \
            dfa.at[ind,'Discount']!='VNA':
            logging.info(f"Invalid Discount line {ind} value {dfa.at[ind,'Discount']}")
            pagerule.append(f"Invalid Discount line {ind} value {dfa.at[ind,'Discount']}")

            d_count+=1
        if len(str(dfa.at[ind,'Unit_price']).split())>1:
            mer_count+=1
            logging.info(f"Invalid Unit_price line {ind} value {dfa.at[ind,'Unit_price']}")
            pagerule.append(f"Invalid Unit_price line {ind} value {dfa.at[ind,'Unit_price']}")

        
        if len(str(dfa.at[ind,'Quantity']).strip().split('.')[0])>3:
            
            if re.match(r'^(\-|(?:))\d+[.,]\d{3}[.,]\d{2}$|^(\-|(?:))\d+[.,]\d{2}$',str(dfa.at[ind,'Unit_price']).strip()):
                dfa.at[ind,'Quantity']=""
            else:
                qty_flag=True
                len_count+=1
        if (len(str(dfa.at[ind,'Unit_price']).strip().split('.')[0].replace(',',''))>5 or len(str(dfa.at[ind,'Unit_price']).strip())==1 or len(str(dfa.at[ind,'Quantity']).strip().split('.')[0])>3 or '9993' in str(dfa.at[ind,'Quantity']).strip()) and qty_flag==False:
            len_count+=1
        if re.match(r'^\d{1,2}(-|/)(\d{1,2}|[A-Za-z]{3,9})(-|/)\d{2,}$|^\d{1,2}(\.|\. )\d{1,2}(-|/)(\d{1,2}|[A-Za-z]{3,9})(-|/)\d{2,}$',str(dfa.at[ind,'Particular']).strip()):
            date_count+=1
        if re.match(floatpattern,str(dfa.at[ind,'Particular']).strip().strip('.')):
            sr_count+=1
        if dfa.at[ind,'Before_discount_amount']==dfa.at[ind,'After_discount_amount']=='VNA':
            amount_cnt+=1
    for row in dfa.index:
        if dfa.at[row,'Before_discount_amount']=='VNA' and dfa.at[row,'After_discount_amount']=='VNA':
            zero_count+=1
    flot_cnt=0
    for row in dfa.index:
        if str(dfa.at[row,'Before_discount_amount']).strip() in ["0","0.00","VNA","nan"] \
        and str(dfa.at[row,'After_discount_amount']).strip() in ["0","0.00","VNA","nan"] and \
        dfa.at[row,'Discount'] in ["0","0.00","VNA","nan"] and dfa.at[row,"Unit_price"] in ["VNA","nan"]:
            if (dfa.at[row,"Quantity"] not in ["VNA","nan","0","0.00"]):
                flot_cnt+=1
        
        elif str(dfa.at[row,'Before_discount_amount']).strip() in ["0","0.00","VNA","nan"] and str(dfa.at[row,'After_discount_amount']).strip() in ["0","0.00","VNA","nan"] and dfa.at[row,'Discount'] in ["0","0.00","VNA","nan"]:
            if (dfa.at[row,"Unit_price"]!="VNA" and dfa.at[row,"Quantity"]!="VNA") or ttc=="Summary":
                try:
                    if float(str(dfa.at[row,"Quantity"]).replace(",","")) < 100:
                        if float(str(dfa.at[row,"Unit_price"]).replace(",",""))*float(str(dfa.at[row,"Quantity"]).replace(",",""))!=0:
                            flot_cnt+=1
                except Exception:
                    pass
        
    empt=True

    

    if np.all(dfa['Particular']=='VNA') or (np.all(dfa['After_discount_amount']== 'VNA') and np.all(dfa['Before_discount_amount']== 'VNA' )):
        coverage_=0
        logging.info("Calling rescue steps as particular / bda is empty")

        pagerule.append("Calling rescue steps as particular / bda is empty")
        
        empt=False
    elif len(dfa[dfa['Particular'].isin(['VNA','nan'])])>0.5*len(dfa):
        coverage_=0
        logging.info("Calling rescue steps as particular is empty")
        pagerule.append("Calling rescue steps as particular is empty")
    elif (cont>len(dfa)*0.25 or zero_count>=len(dfa)*0.75 or date_count>len(dfa)*0.5 or amount_cnt>len(dfa)*0.75 or sr_count>=len(dfa)*0.5 or mer_count>=len(dfa)*0.5 or d_count>=len(dfa)*0.5 or len_count>=len(dfa)*0.4 or flot_cnt>=len(dfa)*0.5) and len(dfa)>2:
        logging.info("Calling rescue steps as undesirable patterns in amount columns are found")
        coverage_=0
        pagerule.append("Calling rescue steps as conditions not satisfied")
    elif (len(dfa[dfa['Unit_price'].isin(['VNA','nan',""])])>0.8*len(dfa)) and (len(dfa[dfa['Quantity'].isin(['VNA','nan',""])])>0.8*len(dfa)) and ttc!="Summary":
        logging.info("Calling rescue steps as unitprice and qty is empty")
        
        coverage_=0
        pagerule.append("Calling rescue steps as unitprice and qty is empty")
    else:
        coverage_=1
    if f_header==1 and len([col for col in df.columns if str(col) not in ["bb_rowwise","con_rowwise"]])<3 and empt==True:
        coverage_=1
    
    threshold=0.1
    if pc_flag==1:
        coverage_,coverage=1,1
    try:
        if (dfa.shape[0]/df.shape[0])<threshold:
            coverage=0
        else:
            coverage=1
    except ZeroDivisionError:
        coverage=1
    return coverage,coverage_,pagerule