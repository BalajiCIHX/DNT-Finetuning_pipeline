def callisto_map(hospital_id,df,clf,clf_second,tfidf_word,tfidf_char,nme_clf,nme_tfidf_word,nme_tfidf_char,config,rule_dict):
    l1labels=config['calistol1lookup']
    from bill_modules.get_supreme_id import get_id
    import joblib
    import pandas as pd
    from scipy.sparse import hstack
    import numpy as np
    import re
    hospital_id=str(hospital_id).split(".")[0]
    pharmarules=[]
    lab_items=['bio chemistry (lft,rf,lipid)','haematology (blood count)',
    'microbiology (culture & sensitivity)','other diagnostic technologies','pathology (hpe,biopsy)','radiology (mri/ct/x-ray)','cardiology charges','serology (hiv, hcv, hbsag)']
    pharma_items=['icu consumables','ot consumables','ward consumables','implant/stent','iv fluids / disposables','medicines/drugs','vaccination drugs']
    summary_items=["medicines","medical bill","medicines charges","medical bills","medical store","medical store return","medicine charge","medicine amt",
                   "medicine adm charges","medicine bill","medicine  invoice attached","service consumables",
                   "service drugs and consumables","drug charges","medical bills  annexure", 'sales returns',"medicine bills"]
    def lab_pharm(final_df,colname,l):
        """Function to check whether the summation of pharma line items matches perfectly with medicine breakups"""
        d1=final_df[final_df[colname]==True]
        bda = bda_sum(d1)
        final_df['pharmacy_sum'] = bda
        g = final_df[final_df['l2'].isin(l)]
        g = g[g[colname] == False]
        r = bda_sum(g)
        if round(r) in range(round(bda)-10,round(bda)+10) and r!=0:
            
            final_df = final_df.drop(g.index)
            pharmarules.append('Double count pharmacy identified')

        return final_df
    
    def bda_sum(df):
        new_list = []
        for i in df["Before_discount_amount"].to_list():
            try:
                i=float(i)
                new_list.append(i)
            except ValueError:
                pass
        return sum(new_list)
    def cleaning_txt(str_):
        str_=str_.lower()
        s = re.sub(r'[^0-9a-z]' , ' ' , str_ , flags=re.I)
        s = re.sub(r'(hsn|mrp|pmin|pmis|tcode|batch id)\s*\d*' , ' ' , s , flags=re.I)
        s = re.sub(r'\b\d+\b' , '' , s)
        s = re.sub(r'  +', ' ', s)
        s = s.strip()
        return s
    
    def call_callisto(data,tfidf_word,tfidf_char,clf,clf_second,nme_clf,nme_tfidf_word,nme_tfidf_char):
        data.Particular1 = data.Particular.astype('str')
        data.Particular1 = data.Particular1.apply(cleaning_txt)
        tokenized = hstack([tfidf_word.transform(data.Particular1), tfidf_char.transform(data.Particular1)])
        data['mod1'] = clf.predict(tokenized)
        data['mod1_score'] = np.max(clf.decision_function(tokenized) , axis=1)
        data['mod2'] = clf_second.predict(tokenized)
        data['mod2_score'] = np.max(clf_second.decision_function(tokenized) , axis=1)
        data.loc[data.mod1 == 'others' , 'l2'] = data.loc[data.mod1 == 'others' , 'mod2']
        data.loc[data.mod1 != 'others' , 'l2'] = data.loc[data.mod1 != 'others' , 'mod1']
        data.loc[(data.mod1 != 'others') & (data.mod1_score <= 0.2) , 'l2'] = 'NHI'
        data.loc[(data.mod1 == 'others') & ((data.mod2_score <= 0.2) | (data.mod1_score <= 0.2)) , 'l2'] = 'NHI'
        tokenized = hstack([nme_tfidf_word.transform(data.Particular1), nme_tfidf_char.transform(data.Particular1)])
        data['nme_score'] = nme_clf.decision_function(tokenized)
        data['is_nme'] = nme_clf.predict(tokenized)
        data.loc[(data.nme_score >= -0.38) & (data.nme_score <= 0.27) , 'is_nme'] = 'NHI'
        data.drop(columns = ['mod1', 'mod1_score', 'mod2', 'mod2_score' , 'nme_score'] , inplace=True)
        return data
    
    final_df = pd.DataFrame()
    
    if len(df) > 0:
        
        df=call_callisto(df,tfidf_word,tfidf_char,clf,clf_second,nme_clf,nme_tfidf_word,nme_tfidf_char)
        for i in df.index:
            if re.sub(r"[^A-Za-z ]","",df.at[i,"Particular"].strip()).strip() in summary_items:
                df.at[i,"l2"]='medicines/drugs'
                pharmarules.append(f'line made as medicines/drugs at {i}')
        df['l1']=df['l2'].map(l1labels)
        for page in df.Page_no.unique():
            data=df[df['Page_no']==page]
           
            ph_cnt=0
            lab_cnt = 0
            for i in data.index:
                
                if data.loc[i,'l2'] in pharma_items and "pharmacy" not in str(data.loc[i,'Particular']).lower() and "drugs" not in str(data.loc[i,'Particular']).lower() and "medicines" not in str(data.loc[i,'Particular']).lower():
                    ph_cnt+=1
                if data.loc[i,'l2'] in lab_items and "lab" not in str(data.loc[i,'Particular']).lower():
                    lab_cnt+=1               

            if hospital_id in ["508974"]:
                data['is_lab']=False
                if ph_cnt+lab_cnt >= len(data)*0.75:
                    data['is_pharmacy']=True
                    pharmarules.append(f'Pharmacy page identified at {page}')
                else:
                    data['is_pharmacy']=False
            else:
                if ph_cnt>=len(data)*0.75:
                    data['is_pharmacy']=True
                    pharmarules.append(f'Pharmacy page identified at {page}')
                    
                else:
                    data['is_pharmacy']=False
                if lab_cnt>=len(data)*0.75:
                    data['is_lab']=True
                    pharmarules.append(f'Pharmacy page identified at {page}')
                    
                else:
                    data['is_lab']=False
                
            final_df=pd.concat([final_df,data])
            
        if hospital_id in get_id.pharm_lab():
            
            final_df = lab_pharm(final_df,'is_pharmacy', lab_items+pharma_items)
        else:
            final_df = lab_pharm(final_df,'is_pharmacy', pharma_items)
            final_df = lab_pharm(final_df,'is_lab', lab_items)
        final_df.drop(columns=['pharmacy_sum'], inplace=True)
        if hospital_id in get_id.pharm_x():
            for ind in final_df.index:
                if "medicines x" in final_df.loc[ind,"Particular"]:
                    final_df.drop(ind,inplace=True)
    else:
        final_df=df
    rule_dict['Pharmacy_rules']=pharmarules
    return final_df,rule_dict
