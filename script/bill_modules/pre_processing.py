def preprocessing_(df,hosp_id,pagerule):
    from bill_modules.get_supreme_id import get_id
    from bill_modules.utils import rounding_off,preprocess,is_unique,remove_duplicate_column_names,cleanup,replace_negatives,replace_x
    """
    Preprocessing
    ---------------
    + To remove some irrelevant rows and columns
    + To solve column splitting
    + To replace negatives
    + To identify returns
    """
    import re
    import numpy as np
    df=df.reset_index(drop=True)
    hosp_id=int(hosp_id)
    df.columns=[str(i) for i in df.columns]
    pagerule.append(f'OCR Output Dimensions prior to preprocessing (rows,columns) {df.shape}')
    for col in df.columns:
        df[col]=df[col].apply(lambda x:str(x).strip(":").rstrip("-").strip('.').strip())
    df.replace(":","",inplace=True)
    df=rounding_off(df)
    if len(df)>1:
        
        df.columns = remove_duplicate_column_names(df.columns)
        df.replace({'nan':np.nan,"":np.nan," ":np.nan},inplace=True)
        cols_to_check=[i for i in df.columns if i not in ["bb_rowwise","con_rowwise"]]

        df['is_na'] = df[cols_to_check].isnull().apply(lambda x: all(x), axis=1) #Removing null rows
        df=df[df["is_na"]==False]

        df.drop(columns="is_na",inplace=True)
        
        df=df.astype(str)
        
        if str(hosp_id)=="504637":	
            for i in df.columns:	
                df[i]=df[i].apply(lambda x:replace_x(str(x).strip()))	
            return df,pagerule
        if str(hosp_id)=="50205":
            for i in df.columns:
                df[i]=df[i].apply(lambda x:replace_x(str(x).strip()))
        def column_splitting(df):
            """
            Merges two adjacent columns together based on null values
            """
            #Removing obvious entities\
            df.fillna('nan')
            df.replace({'Sub':'nan',"sub total":"nan",'*':"nan","":'nan'," ":'nan'},inplace=True)
            col = [j for j in df.columns if df[j].eq('nan').all(axis=0)==False and j not in ['bb_rowwise','con_rowwise']]
            try:
                modified_cols,droped=[],[]
                for i in range(len(df.columns)-1):
                    col1 = col[i]
                    col2 = col[i+1]    
                    temp = df[[col1,col2]]
                    temp.drop(list(temp.index)[0],inplace=True) #remove first row
                    uniqvalues1,uniqvalues2=is_unique(temp[col1]),is_unique(temp[col2])
                    sug=True
                    if uniqvalues1==True:
                        droped.append(col1)
                        sug=False
                    if uniqvalues2==True:
                        droped.append(col2)
                        sug=False
                    if col1 not in ["bb_rowwise","con_rowwise"] and col2 not in ["bb_rowwise","con_rowwise"] and sug==True:
                        nullindex=[]
                        for j in temp.index:
                            if temp.at[j,col1]=="nan" and temp.at[j,col2]=="nan":
                                nullindex.append(j)
                        temp.drop(nullindex,inplace=True)

                        null1,notnull1,null2,notnull2 = len(temp[temp[col1]=="nan"]),len(temp[temp[col1] !="nan"]),len(temp[temp[col2]=="nan"]),len(temp[temp[col2] !="nan"])
                        if null1 == notnull2 and null2 == notnull1 and len(temp)>2:
                            for k in temp.index:
                                if df.at[k,col1]=="nan" and df.at[k,col2]!="nan":
                                    df.at[k,col1]=df.at[k,col2]
                                modified_cols.append(col2)
                df.drop(columns=modified_cols,inplace=True)
                if len(modified_cols) != 0:
                    pagerule.append(f'Column Splitting Logic active, Two columns separated {modified_cols}, hence merged')
                droped=list(set(droped))
                try:
                    df.drop(columns=droped,inplace=True)
                except Exception:
                    pass
                    
            except IndexError:
                pass
            return df
        if str(hosp_id) not in get_id.ign_col_split_deals():
            #Some hospitals depend on index positions, it is not require to modify the positions
            #This will be automatcially taken care by Hospital specific logic
            df=column_splitting(df)
        
        
        #To prev
        dt = {}
        for i in df.columns:
            df[i]=df[i].astype(str)
            df[i].replace("nan","",inplace=True)
            dt[i]=sum(df[i].str.count(r'[A-Za-z]'))
        keymax = max(dt, key= lambda x: dt[x])
        for j in df.index:
            if re.match(r'^\(No(\.|\. )\d+\)$|^\(no(\.|\. )\d+\)$|^no(\.|\. )\d+$',str(df.loc[j,keymax]).strip().lower()):
                try:
                    df.loc[j,keymax]=df.loc[j-1,keymax]
                    pagerule.append('Pattern specific to (no. 12334) found hence above row particulars mapped')
                except (ValueError, KeyError):
                    pass
            
        for i in df.columns:
            df[i]=df[i].apply(lambda x:preprocess(x))
        df=cleanup(df)
        df.replace('nan',np.nan,inplace=True)
        df.replace("",np.nan,inplace=True)
        df.replace(" ",np.nan,inplace=True)


        def returns_identifier(df):
            """
            To change values to negative in cases with positive returns (known cases)
            """
            flag=False
            df.columns=[str(i).lower() for i in df.columns]
            for i in df.columns:
                df[i] = df[i].apply(lambda x: str(x).lower())
            lst=" ".join(list(df.columns))
            lst2=df.values.astype(str)[-3:]
            lst2 = np.concatenate(lst2).ravel().tolist()
            lst2 = " ".join(lst2)
            lst3 = df.values.astype(str)[:3]
            lst3 = np.concatenate(lst3).ravel().tolist()
            lst3 = " ".join(lst3)
            lst = lst+lst2+lst3
            if "return" in lst:
                for i in df.columns:
                    l=df[i].tolist()
                    fl=[]
                    for j in l:
                        if re.match(r"^\d{1,2}$|^\d+[.,]\d{1,2}$|^\d+[.,]\d{3}[.,]\d{2}$",j.strip()):
                            j='-'+j.strip()
                            fl.append(j)
                            pagerule.append('Values made negative due to the presence of returns')
                        else:
                            fl.append(j)
                    df[i]=fl
            else:
                for i in df.index:
                    string=" ".join(list(df.loc[i,:].values))
                    if "return" in string or "refund" in string or "ret.qty" in string:
                        if "ip pharmacy" in string or "drug sale" in string or \
                        "pharmacy refund amount" in string or "medicine return" in string:
                            flag=False
                        else:
                            flag=True
                            break
                if flag==True:
                    for j in list(df.index)[list(df.index).index(i)+1:]:
                        l=list(df.loc[j,:].values)
                        fl=[]
                        for k in l:
                            if re.match(r"^\d{1,2}$|^\d+[.,]\d{1,2}$|^\d+[.,]\d{3}[.,]\d{2}$",k.strip()):
                                k='-'+k.strip()
                                fl.append(k)
                                pagerule.append('Values made negative due to the presence of returns')

                            else:
                                fl.append(k)
                        df.loc[j,:]=fl
                
            return df
        

        if str(hosp_id) in get_id.returns_identifier_ids():
            df = returns_identifier(df)
        if len([col for col in df.columns if str(col) not in ["bb_rowwise","con_rowwise"]])>3 and len(df)>0:
            serie=df[["con_rowwise","bb_rowwise"]]
            df.drop(columns=["con_rowwise","bb_rowwise"],inplace=True)
            row_lst=df.isnull().sum(axis=1).tolist()

            row_lst=list(np.array(len(df.columns))-np.array(row_lst))
            df['null_count_row']=row_lst
            df=df[df['null_count_row']>1]
            for ind in df.index:
                df.at[ind,"bb_rowwise"]=serie.at[ind,"bb_rowwise"]
                df.at[ind,"con_rowwise"]=serie.at[ind,"con_rowwise"]
            df.drop(columns=['null_count_row'],inplace=True)
            df=df.reset_index(drop=True)
            for i in df.columns:
                if df[i].isnull().sum(axis=0)>len(df)*0.85:
                    df.drop(columns=i,inplace=True)
                    pagerule.append(f'Column {i} removed as most values are null')
        
        df.columns = remove_duplicate_column_names(df.columns)
        if str(hosp_id)!="510488":
        
            for i in df.columns:
                df[i]=df[i].apply(lambda x:replace_negatives(x,pagerule))
        df=rounding_off(df)
    pagerule.append(f'Output Dimensions after post processing (rows,columns) {df.shape}')
    return df,pagerule


#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################