def data_disambiguation(key_val,location_of,columns_to_be_extracted,merged_df,f_header,dict_,dict_values,pagerule):
    """
    ________________________________________________
    To separate out the columns based on their positions
    
    Input: Key_val: dictionary that gives the mapping of the columns and the column names from the dictionaries
           location_of: dictionary depicting the positions of the columns
           columns_to_be_extracted: the columns that were identified from the dataframe Type - list
           merged_df: operational dataframe
           f_header: variable that says whether a table contains a header or not
           
    Output: dfa - the modified dataframe
            merged_df - the unmodified dataframe
            headers_a - column names (that later comes useful in rescue steps)

    """
    pick_default_parent={'Particular':'description','Quantity':'qty','Unit_price':'rate','Before_discount_amount':"gross",'Discount':'discounts','After_discount_amount':'netamt'}
    import logging
    import re
    import numpy as np
    import pandas as pd
    dfa=pd.DataFrame(columns=['Particular','Unit_price','Quantity','Before_discount_amount','Discount','After_discount_amount'])
    def get_key(val,dict_):
        for i in dict_.keys():
            dict_val_k=dict_[i]

            dict_val_k = [str(item).lower() for item in dict_val_k]
            if val in dict_val_k:

                return i
        return "key doesn't exist"
    key_val1=key_val.copy()
    for i,j in key_val1.items():
        if i=="key doesn't exist":
            if type(j)==list and len(j)>1:
                  for ix in j:
                    if len(list(filter(ix.endswith, dict_)))>1:
                        k1=get_key(list(filter(ix.endswith, dict_values))[0],dict_)
                        val=pick_default_parent[k1]
                        try:
                            if k1 in key_val.keys() and type(key_val)==list:

                                key_val[k1].append(val)
                            else:
                                key_val[k1]=val

                        except KeyError:

                            key_val[k1]=val
                        try:
                            location_of[val]=location_of[ix]
                            columns_to_be_extracted.append(val)
                            location_of.pop(ix)
                        except KeyError:
                            pass


                    if len(list(filter(ix.startswith, dict_values)))>1:
                        k1=get_key(list(filter(ix.startswith, dict_values))[0],dict_)
                        val=pick_default_parent[k1]
                        try:
                            if k1 in key_val.keys() and type(key_val)==list:

                                key_val[k1].append(val)
                            else:
                                key_val[k1]=val
                            
                        except KeyError:
                            key_val[k1]=val
                        try:
                            location_of[val]=location_of[ix]
                            columns_to_be_extracted.append(val)
                            location_of.pop(ix)
                        except KeyError:
                            pass
                   
            else:
                for ix in j:
                    if len(list(filter(j[0].endswith, dict_)))>1:
                        val=pick_default_parent[get_key(list(filter(ix.endswith, dict_values))[0],dict_)]
                        key_val[get_key(list(filter(ix.endswith, dict_values))[0],dict_)]=val
                        location_of[val[0]]=location_of[ix]
                        columns_to_be_extracted.append(val[0])
                        location_of.pop(ix)
                       

                    if len(list(filter(j[0].startswith, dict_values)))>1:
                        val=pick_default_parent[get_key(list(filter(ix.startswith, dict_values))[0],dict_)]
                        key_val[get_key(list(filter(ix.startswith, dict_values))[0],dict_)]=val
                        location_of[val]=location_of[ix]
                        columns_to_be_extracted.append(val)
                        location_of.pop(ix)

    def preprocess(x):
        
        x=str(x).replace('*','').strip()
        a=re.sub(r'^O$','0',x,flags=re.IGNORECASE)
        a=re.sub(r'^LO$','5',a,flags=re.IGNORECASE)
        a=re.sub(r'^CO$','6',a,flags=re.IGNORECASE)
        a=re.sub(r'^I$','1',a,flags=re.IGNORECASE)
        a=re.sub(r'^-$','1',a,flags=re.IGNORECASE)
        a=re.sub(r'^(I|I )No$','1 No',a,flags=re.IGNORECASE)
        a=re.sub(r'^(\|) No$','1',a,flags=re.IGNORECASE)
        a=re.sub(r'\*',"",a,flags=re.IGNORECASE)
        a=a.replace('Rs.',"")
        a=a.replace('Rs .',"")
        a=a.strip()
        return a
    duplicated_idx = merged_df.columns.duplicated() #Checks duplicated index values
    duplicated = merged_df.columns[duplicated_idx].unique()
    rename_cols = []
    i = 1

    for col in merged_df.columns:

        if col in duplicated:
            rename_cols.extend([str(col) + '_' + str(i)])
            i+=1

        else:
            rename_cols.extend([col])

    merged_df.columns = rename_cols
    for i in merged_df.columns:
        merged_df[i]=merged_df[i].apply(lambda x:preprocess(x))
    final_loc={}
    if f_header==1:
        for i,j in key_val.items():
            if i!="key doesn't exist":
                if isinstance(j,list): 
                    if len(j)==1 and isinstance(location_of[j[0]],int):
                        ix=location_of[j[0]]
                        dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                        logging.info("Direct hit {}".format(str(j)))
                        
                        final_loc[i]=ix
                        pagerule.append("Direct hit {}".format(str(j)))

                    elif len(j)==1 and isinstance(location_of[j[0]],tuple) and i!="Particular":
                        
                        pagerule.append(f"Columns found merged {i} {location_of[j[0]]}")
                        ind,pos=location_of[j[0]]
                        final_loc[i]=pos
                        pos=pos-1
                        for inx in merged_df.index:
                            if len(merged_df.loc[inx,merged_df.columns[pos]].split())>=ind:
                                dfa.loc[inx,i]=merged_df.loc[inx,merged_df.columns[pos]].split()[ind-1]
                            else:

                                dfa.loc[inx,i]=merged_df.loc[inx,merged_df.columns[pos]]
                        
                    elif len(j)>1 and (i in ['Discount','Unit_price']):

                        if isinstance(location_of[j[-1]],int) and j[-1] not in ["mrp","m.r.p","mrp/u"]:
                            ix=location_of[j[-1]]
                            final_loc[i]=ix
                            logging.info("Double hit unitprice {}".format(str(j)))
                            pagerule.append("Double hit unitprice {}".format(str(j)))


                            dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                        elif j[-1] in ["mrp","m.r.p","mrp/u"]:
                            if isinstance(location_of[j[0]],int):
                                ix=location_of[j[0]]
                                final_loc[i]=ix
                                dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                                logging.info("Double hit unitprice {}".format(str(j)))
                                pagerule.append("Double hit unitprice {}".format(str(j)))
                                

                        elif isinstance(location_of[j[0]],int) and isinstance(location_of[j[-1]],tuple):
                            ix=location_of[j[0]]
                            final_loc[i]=ix
                            dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                            logging.info("Double hit unitprice/discount {}".format(str(j)))
                            pagerule.append("Double hit unitprice/discount {}".format(str(j)))

                            
                    
                    elif len(j)>1 and i=="Quantity":
                        if isinstance(location_of[j[0]],int):
                            ix=location_of[j[0]]
                            final_loc[i]=ix
                            dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                            logging.info("Double hit quantity {}".format(str(j)))
                            pagerule.append("Double hit quantity {}".format(str(j)))
                            

                    elif len(j)>1 and i!='Particular':
                        if i=="Before_discount_amount":
                            for ele in j:
                                if isinstance(location_of[ele],int):
                                    ix=location_of[ele]
                                    final_loc[i]=ix
                                    dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                                    logging.info("Double hit BDA {}".format(str(j)))
                                    pagerule.append("Double hit BDA {}".format(str(j)))
                                    break
                        else:
                            for ele in reversed(j):
                                if isinstance(location_of[ele],int):
                                    ix=location_of[ele]
                                    final_loc[i]=ix
                                    dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                                    logging.info("Double hit {} {}".format(str(i),str(j)))
                                    pagerule.append("Double hit {} {}".format(str(i),str(j)))
                                    break

                                
                        if dfa[i].isnull().all():
                                
                            if isinstance(location_of[j[-1]],tuple):
                                ind,pos=location_of[j[-1]]
                                final_loc[i]=pos
                                pos=pos-1

                                for inx in merged_df.index:
                                    if len(merged_df.loc[inx,merged_df.columns[pos]].split())>=ind:
                                        dfa.loc[inx,i]=merged_df.loc[inx,merged_df.columns[pos]].split()[ind-1]
                                    else:

                                        dfa.loc[inx,i]=merged_df.loc[inx,merged_df.columns[pos]]
                            elif isinstance(location_of[j[-1]],int):
                                ix=location_of[j[-1]]
                                final_loc[i]=ix
                                dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                    
                    elif len(j)>1 and i=='Particular':
                        dc_cnt=0
                        i_d=""
                        logging.info("Double hit particular {}".format(str(j)))
                        pagerule.append("Double hit particular {}".format(str(j)))
                        
                        for item in j:
                            if "head" in item:
                                dc_cnt+=1
                                i_d=item
                            if "description" in item:
                                dc_cnt+=1
                        j=[item for item in j if item!=i_d]
                        if dc_cnt==2 and len(j)==1:
                            try:
                                dfa["Particular"]=merged_df[j[0]]
                            except:
                                sum_cols={}
                                for col in merged_df.columns:
                                    if col!="bb_rowwise":
                                        merged_df[col]=merged_df[col].astype('str')
                                        sum_cols[col]=sum(merged_df[col].str.count(r'[A-Za-z ]'))
                                p_col = max(sum_cols, key= lambda x:sum_cols[x])
                                dfa['Particular']=merged_df[p_col]
                                
                        else:
                            sum_cols={}
                            for col in merged_df.columns:
                                if col!="bb_rowwise":
                                    merged_df[col]=merged_df[col].astype('str')
                                    sum_cols[col]=sum(merged_df[col].str.count(r'[A-Za-z ]'))
                            p_col = max(sum_cols, key= lambda x:sum_cols[x])
                            dfa['Particular']=merged_df[p_col]
                        

                    else:
                        dfa=pd.DataFrame(columns=['Particular','Unit_price','Quantity','Before_discount_amount','Discount','After_discount_amount'])
                        break
                if type(j)=='str':
                    if type(location_of[j])==int:
                        ix=location_of[j]
                        dfa[i]=merged_df[list(merged_df.columns)[ix-1]]
                    elif type(location_of[j])==tuple:
                        ind,pos=location_of[j]
                        pos=pos-1

                        for inx in merged_df.index:
                            if len(merged_df.loc[inx,merged_df.columns[pos]].split())>=ind:
                                dfa.loc[inx,i]=merged_df.loc[inx,merged_df.columns[pos]].split()[ind-1]
                            else:

                                dfa.loc[inx,i]=merged_df.loc[inx,merged_df.columns[pos]]
    
                    
    

    
    if f_header==0:
        for i,j in key_val.items():
            dfa[i]=merged_df[j]
       
    
    if dfa['Particular'].isnull().all():
        sum_cols={}
        for col in merged_df.columns:
            if col!='bb_rowwise':
                merged_df[col]=merged_df[col].astype('str')
                sum_cols[col]=sum(merged_df[col].str.count(r'[A-Za-z ]'))
        p_col = max(sum_cols, key= lambda x:sum_cols[x])
        dfa['Particular']=merged_df[p_col]
        cols=['Quantity','Unit_price','Before_discount_amount','Discount','After_discount_amount']
        cols=[i for i in cols if i in final_loc.keys()]
        if len(cols)>1:
            for col in cols:
                if list(merged_df.columns).index(p_col)+1==final_loc[col]:
                    dfa["Particular"]=np.nan
                    break

    def particular_validation(merged_df,p_col):
        float_count=0
        flag=False
        floatpattern=r'^(\d+(?:\.\d+)?)$'

        def alpha_cnt(x):
            if len(re.findall(r'[A-Za-z]',str(x).strip()))<3:
                return True
            else:
                return False
    
        float_count=merged_df[p_col].apply(lambda x:bool(re.match(floatpattern,str(x)))).sum()
        alpha_count=merged_df[p_col].apply(lambda x:alpha_cnt(x)).sum()
        merged_df[p_col] = merged_df[p_col].astype('str')
        di_chem=len(merged_df[merged_df[p_col].str.contains("dial|hemo|pharma|medic")])
        if len(merged_df)>0:
            pert=(len(merged_df)-len(set(merged_df[p_col].tolist())))/len(merged_df)
            if pert>0.75 and len(merged_df)>5 and di_chem<3:
                flag=True
        return float_count,flag,alpha_count
            
    def particulars_finding(merged_df):
        
        sum_cols={}
        for i in merged_df.columns:
            merged_df[i]=merged_df[i].astype('str')
            sum_cols[i]=sum(merged_df[i].str.count(r'[A-Za-z]'))

        p_col = max(sum_cols, key= lambda x:sum_cols[x])
        if sum_cols[p_col]==0:
            for i in merged_df.columns:
                merged_df[i]=merged_df[i].astype('str')
                sum_cols[i]=sum(merged_df[i].str.count(r'[A-Za-z0-9 ]'))
        p_col = max(sum_cols, key= lambda x:sum_cols[x])
        return p_col
            

    if f_header==1:
        float_cnt, fl, alp_cnt = particular_validation(dfa,'Particular')
        if float_cnt>2 or fl==True or alp_cnt>10:
            
            if 'Particular' in key_val.keys():
                if len(key_val['Particular'])==1 and type(key_val['Particular'])==list:
                    if type(location_of[key_val['Particular'][0]])==int:
                        merged_df_temp = merged_df.drop(columns=list(merged_df.columns)[location_of[key_val['Particular'][0]]-1])
                        for col in merged_df_temp.columns:
                            if col.startswith('con') or col.startswith('bb_'):
                                merged_df_temp.drop(columns=col, inplace=True)
                        p_col=particulars_finding(merged_df_temp)
                        dfa['Particular']=merged_df_temp[p_col]
                
        if "Unit_price" in key_val.keys() and "After_discount_amount" in key_val.keys():

            if len(key_val["Unit_price"])==len(key_val["After_discount_amount"])==1 and \
            type(location_of[key_val["Unit_price"][0]])==tuple and type(location_of[key_val["After_discount_amount"][0]])==tuple:
                if location_of[key_val["Unit_price"][0]][1]==location_of[key_val["After_discount_amount"][0]][1]:
                    logging.info("UP and ADA in same column")
                    
                    dfa["Particular"]=np.nan
                    
                

    dfa = dfa[dfa['Particular'].notna()]
    def remove_text(i):
        i=re.sub(r'[^0-9\.\,\-\;\:\ \=]','',str(i))
        return i
    def quantity_formatting(i):
        if re.match(r'(\d{1,}|\d{1,}\s)(x|x\s|×|×\s)(\d{1,}\s|\d{1,})',str(i).lower()):
            i=i.split("x")[0].split("×")[0]
        return i
    dfa["Quantity"]=dfa["Quantity"].apply(lambda x:quantity_formatting(x))
    for col in ['Quantity','Unit_price','Before_discount_amount','Discount','After_discount_amount']:
        dfa[col]=dfa[col].apply(lambda x:remove_text(x))
        dfa[col]=dfa[col].replace('',np.nan)
    # headers_a=key_val
    for i in dfa.index:
        dfa.loc[i,'bb_rowwise']=merged_df.loc[i,"bb_rowwise"]
        dfa.loc[i,'con_rowwise']=merged_df.loc[i,"con_rowwise"]
        
        if dfa.loc[i,"Discount"]==dfa.loc[i,"After_discount_amount"]:
            dfa.loc[i,"Discount"]=""
    return (dfa,merged_df,pagerule)


#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################