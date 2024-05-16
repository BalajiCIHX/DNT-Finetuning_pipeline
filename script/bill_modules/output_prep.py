def output_prep(all_pdfs,ocr_type,hospital_name,hospital_id,clf,clf_second,tfidf_word,tfidf_char,nme_clf,nme_tfidf_word,nme_tfidf_char,config,rule_dict,ocr_selection):
    from bill_modules.pharmacy_classifier import callisto_map
    from bill_modules.bill_score import output_score
    from bill_modules.utils import get_standard_quantity,apply_std_changes
    import numpy as np
    import pandas as pd
    mreq=config['calistoparticularmappings']
    ignore_drug=['swab','gauze','syringe']
    exclude_drug=['pharmacy','pharmacy charges','medicine','medicine charges','medicines','drug','drugs']
    modified=[]
    pprules=[]
    def single_ttc(df):
        if len(df)>0:
            df=df.reset_index(drop=True)
            for i in df["Page_no"].unique():
                data=df[df["Page_no"]==i]
                if data["Table_type"].nunique()>1:
                    pprules.append(f"Both table types found in page {i}")
                    t1=len(data[data["Table_type"]=="Detail"])
                    t2=len(data[data["Table_type"]=="Summary"])
                    if t1>t2:
                        lst=list(data.index)
                        df.loc[lst,"Table_type"]="Detail"

                    elif t2>t1:
                        lst=list(data.index)
                        df.loc[lst,"Table_type"]="Summary"
        return df
    def summate(to_amt,op):
        sum_lst = []
        to_amt = list(set(to_amt))
        for j in to_amt:
            for i in op['Page_no'].unique():
                lst = []
                if j == i:
                    lst1=op[op['Page_no'] == j]['Before_discount_amount'].to_list()
                    for i in lst1:
                        try:
                            i = float(i)
                            lst.append(i)
                        except:
                            pass
                    cleanedlst = [x for x in lst if str(x) != 'nan']
                    a = sum(cleanedlst)           
                    sum_lst.append(a)

        final_sum = sum(sum_lst)
        return final_sum


    for i in all_pdfs.columns:
        all_pdfs[i]=all_pdfs[i].replace('[]',np.nan)
        all_pdfs[i]=all_pdfs[i].apply(lambda x:str(x).strip())
        
    ####
    all_pdfs=single_ttc(all_pdfs)

    try:
        all_pdfs=all_pdfs.iloc[:,list(all_pdfs.columns).index("Particular"):]
        all_pdfs=all_pdfs.reset_index()
        all_pdfs=all_pdfs.drop_duplicates(keep='first')
    except ValueError:
        pass
    if 'Page_no' in all_pdfs.columns:
        all_pdfs['OCR_Engine']=all_pdfs['Page_no'].map(ocr_selection)
        all_pdfs['Page_no']=all_pdfs["Page_no"].apply(lambda x:x.split('_')[-1].split('.')[0].split("/")[-1])
    all_pdfs['Bill_Extracted_Hospital_Name']=hospital_name
    bd=len(all_pdfs)
    if bd>0:
        pflag=True
        all_detail=all_pdfs[all_pdfs['Table_type']=='Detail']
        all_detail,rule_dict=callisto_map(hospital_id,all_detail,clf,clf_second,tfidf_word,tfidf_char,nme_clf,nme_tfidf_word,nme_tfidf_char,config,rule_dict)
        if len(all_detail) != len(all_pdfs[all_pdfs['Table_type']=='Detail']):
            ap = all_pdfs[all_pdfs['Table_type']=='Summary']
            ap['is_pharmacy']=False
            all_pdfs=pd.concat([ap,all_detail])

        else:
            pflag=False
            all_pdfs,rule_dict=callisto_map(hospital_id,all_pdfs,clf,clf_second,tfidf_word,tfidf_char,nme_clf,nme_tfidf_word,nme_tfidf_char,config,rule_dict)
            if len(all_pdfs)!=bd:
                all_pdfs["Table_type"]="Detail"
    if "is_pharmacy" in all_pdfs.columns and len(all_pdfs[all_pdfs["is_pharmacy"]==True])>0:
        for i in all_pdfs[all_pdfs["is_pharmacy"]==True].index:
            if "." in str(all_pdfs.at[i,"Quantity"]):
                if str(all_pdfs.at[i,"Quantity"]).strip().endswith("5") or str(all_pdfs.at[i,"Quantity"]).strip().endswith("0"):
                    all_pdfs.at[i,"Quantity"]=all_pdfs.at[i,"Quantity"]
                else:
                    try:
                        if float(all_pdfs.at[i,"Quantity"])>0.5:
                            all_pdfs.at[i,"Quantity"]=str(round(float(all_pdfs.at[i,"Quantity"])))
                    except:
                        all_pdfs.at[i,"Quantity"]=all_pdfs.at[i,"Quantity"]
        all_pdfs['is_pharmacy']=False
    for ind in all_pdfs.index:
        try:
            if all_pdfs.at[ind,"l2"]=="medicines/drugs":
                all_pdfs.at[ind,"is_pharmacy"]=True
            if all_pdfs.at[ind,'Table_type']=="Summary":
                all_pdfs.at[ind,"is_pharmacy"]=False
            if any(x in all_pdfs.at[ind,'Particular'] for x in ignore_drug):
                all_pdfs.at[ind,"is_pharmacy"]=False
            if all_pdfs.at[ind,"Particular"] in exclude_drug:
                all_pdfs.at[ind,"is_pharmacy"]=False
        except Exception:
            pass
    if "Particular" in all_pdfs.columns and 'l2' in all_pdfs.columns:
        for ind in all_pdfs.index:
            if all_pdfs.at[ind,"l2"] in mreq.keys():
                
                if mreq[all_pdfs.at[ind,"l2"]] not in all_pdfs.at[ind,'Particular'] and 'bed' not in all_pdfs.at[ind,'Particular']:
                    modified.append({"page_no":all_pdfs.at[ind,"Page_no"],"line_index":ind,"unmodified_particular":all_pdfs.at[ind,'Particular'],
                                     'modified_particular':mreq[all_pdfs.at[ind,"l2"]]+"-"+all_pdfs.at[ind,"Particular"]})
                    all_pdfs.at[ind,"Particular"]=mreq[all_pdfs.at[ind,"l2"]]+"-"+all_pdfs.at[ind,"Particular"]
    try:
        
        all_pdfs["is_pharmacy"].fillna("f",inplace=True)
        all_pdfs["is_pharmacy"]=all_pdfs["is_pharmacy"].replace(True,"t").replace(False,"f")
        all_pdfs['Quantity']=all_pdfs.apply(get_standard_quantity,axis=1)
        all_pdfs=apply_std_changes(all_pdfs)
        all_pdfs=all_pdfs.loc[:,['index','Page_no','Particular', 'Unit_price', 'Quantity','Before_discount_amount', 'Discount', 'After_discount_amount','Table_type','l1','l2','is_nme','is_pharmacy','OCR_Engine',"Bill_Extracted_Hospital_Name","bb_rowwise","con_rowwise"]]
        df_new, df_col_score, score, df =output_score(all_pdfs)	
        
        
    except Exception:
        pass
    try:
        df=pd.concat([all_pdfs,df_new],axis=1)	
    except Exception as e:
        df=all_pdfs	

    if len(df)>0:
        ind=df.index
        for i in ind:
            if df.at[i,"Quantity"]=="" and df.loc[i,"Unit_price"]=="":
                df.at[i,"Quantity"]='1'
                df.at[i,"Unit_price"]=df.loc[i,"Before_discount_amount"]
            
        df = df.replace('--','1')

    if "Page_no" in df.columns:

        if (len(df)>0 and len(df)<3 and df['Page_no'].nunique()==1) or df['Page_no'].nunique()==1:
            df["Table_type"]="Detail"

        
        if len(df)>0 and df['Table_type'].nunique()==1 and df['Table_type'].unique()[0]=='Summary' and df['Page_no'].nunique()<3:
            df['Table_type']='Detail'
            pprules.append("Page made to Detail as only one page is found")

            
        if len(df)>0 and df["Page_no"].nunique()>1:
            df["Page_no"]=df["Page_no"].astype(float)
            sorted_pages=sorted(list(df["Page_no"].unique()))
            first_page=summate([sorted_pages[0]],df)
            rest=summate(sorted_pages[1:],df)
            
            if first_page>rest-10 and first_page<rest+10:
                pprules.append("Page made to Detail as only one page is found")
                
                for ind in df[df["Page_no"]==sorted_pages[0]].index:
                    
                    df.at[ind,"Table_type"]="Summary"
                for ind in df[df["Page_no"].isin(sorted_pages[1:])].index:
                    
                    df.at[ind,"Table_type"]="Detail"
        df["Page_no"]=df["Page_no"].astype(int).astype(str)
    rule_dict['CallistoModifications']=modified
    rule_dict['Postprocessrules']=pprules
    return df,rule_dict