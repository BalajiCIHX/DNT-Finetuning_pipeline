def hospital_specific_logic(df,hosp_id,ttc):
    from bill_modules.get_supreme_id import get_id
    from bill_modules.utils import particulars_finding,preprocess,remove_duplicate_column_names
    """
    Some hospitals don't follow the regular format/ pattern and hence require special treatment\
    -------------------------
    Input
    -------------------------
    df - operational dataframe
    hosp_id - hospital id - marks the item
    ttc - table type
    -------------------------
    Output
    -------------------------
    df - modified dataframe
    """
    try:
        hosp_id=str(int(float(hosp_id)))
    except Exception:
        hosp_id=str(hosp_id)

    import re
    import numpy as np
    import pandas as pd
    import logging
    df=df.astype('str')
    seri=df.copy() #create a copy of the dataframe to prevent interference of dataframe
    if "bb_rowwise" in df.columns:
        df.drop(columns=['bb_rowwise',"con_rowwise"],inplace=True)
    
    df.columns=remove_duplicate_column_names(df.columns)
    
    if hosp_id in get_id.kg_hospital():
        """
        KG Hospital
        """
        
        df.columns=[str(i) for i in df.columns]
        pattern="^\d+[.,]\d{2}$|^\d+(\,|\ |\.)\d{3}(\,|\ |\.)\d{2}$"
        
        df.replace('',np.nan,inplace=True)
        df['total']=np.nan
        df=df.astype('str')

        for i in df.index:
            val=''
            amt_lst=[]
            for j in df.loc[i].values:
                j=j.strip('-').strip()
                if re.match(pattern,j):
                    amt_lst.append(j)
                    val='p'
            if len(amt_lst)>1 and 'Particulars' in df.columns:
                if df.at[i,'Particulars']==amt_lst[0]:
                    amt_lst=[ele for ele in amt_lst if ele!=amt_lst[0]]
            if val=='':
                df.at[i,'total']=['']
            else:
                df.at[i,'total']=amt_lst
        
        df['total']=df['total'].apply(lambda x:x[0])
        try:
            df.drop(columns=['amount'],inplace=True)
        except:
            df.drop(columns=['total'],inplace=True)
        logging.info("Specific Logic for KH Hospital")
    elif hosp_id in get_id.noble_hospital():
        """
        Noble Hospital
        """
        logging.info("Specific Logic for Noble Hospital")
        
        for j in df.columns:
            df[j]=df[j].apply(lambda x:str(x).strip())
        cnt=0
        for l in df.index:
            row=df.loc[l,:].values.tolist()
            row=[o for o in row if o!='']
            row=" ".join(row).lower()
            x=re.search(r'(\d{1,2}|\ )(x| x|: x)(\ )\d{1,5}(\.|\,|\ )\d{2,3}(\ = |\= |\- |\ - |\ )\d{1,5}(\.|\ |\:|\,)\d{1,2}',row)
            if x !=None:
                cnt+=1
                break

        if cnt!=0:
            string=[]
            for r in df.index:
                row=df.loc[r,:].values.tolist()
                row=[o for o in row if o!='']
                row=" ".join(row).lower()
                string.append(row)

            rows,pars=[],[]

            for i in string:
                i=i.replace('-','=').replace('×','x')
                
                x=re.search(r'(\d{1,2}|\ )(x| x|×| ×|: x)(\ )(\d{1,5}(\.|\,|\ )\d+|\d+ )(\ = |\= |\- |\ - |\ |\=)(\d{1,2}(\,)\d{3}[.,]\d{2}|\d+(\.|\ |\:|\,)\d{1,2}|\d+)',i)
                try:
                    pat=x.group(0)
                    par=i[:x.start()]
                except:
                    pat=""
                    par=""
                rows.append(pat)
                pars.append(par)

            df['Data']=rows
            
            def extract_items(x):
                try:
                    qty=x.split('x')[0]
                    amount=x.split('=')
                    if len(amount)==2:
                        amount=amount[-1]
                    elif len(amount)==1:
                        amount=x.split()[-2]
                        
                    if '=' in x: 
                        up=x.split('x')[1].split('=')[0]
                    else:
                        up=x.split()[2]
                except IndexError:
                    qty,amount,up="","",""
                return qty,amount,up
            
            df['particulars']=pars
            df['quantity']=df['Data'].apply(lambda x:extract_items(x)[0])
            df['price']=df['Data'].apply(lambda x:extract_items(x)[2])
            df["gross amount"]=df['Data'].apply(lambda x:extract_items(x)[1])
            df['net amount']=df['Data'].apply(lambda x:extract_items(x)[1])
            df.drop('Data',axis='columns', inplace=True)
            df=df[["particulars","quantity","price","gross amount","net amount"]]
        else:
            df=df
            
                   
    elif hosp_id in get_id.apollo_hospital() and ttc=='Detail':
        logging.info("Specific Logic for Apollo Hospital")
        cols=[]
        cnt=0
        flgger='No'
        sum_cols={}
        def float_convert(x):
            
            x=str(x).strip()
            if re.match(r"^\d{1,2}(\,)\d{3}(\.)\d{2}$",x):
                x=x.replace(",","")
            elif re.match(r"^\d+(\,)\d{2}$",x):
                x=x.replace(",",".")
            else:
                x=x
            return x
        for k in df.columns:
            df[k]=df[k].astype(str)
            df[k]=df[k].apply(lambda x:x.strip())
            df[k]=df[k].apply(lambda x:preprocess(x))
            
        for ind in df.index:
            r = " ".join([item for item in list(df.loc[ind,:].values) if item not in ["nan",""]])
            pattern=r'\d+\s(\d+(\,|\.|\ )\d{3}(\,|\.|\ )\d{2}|\d+(\,|\.|\ )\d{2})\s(\d+|\-\d+)\s(\d+(\,|\.|\ )\d{3}(\,|\.|\ )\d{2}|\d+(\,|\.|\ )\d{2})'
            pat=re.search(pattern,r)
            if pat != None:
                flgger="yes"
                break
        if flgger=='yes':
            for k in df.columns:
                sum_cols[k]=sum(df[k].str.count(r'[A-Za-z ]'))
            Keymax = max(sum_cols, key= lambda x: sum_cols[x])
            df.rename(columns={Keymax:'particular'},inplace=True)
            df1=df.copy()
            df1.drop(columns=df1.columns[0],inplace=True)
            try:
                df1.drop(columns=['particular'],inplace=True)
            except KeyError:
                pass
            for i in df1.columns:
                df1[i]=df1[i].astype('str')
                df1[i]=df1[i].apply(lambda x:(re.sub(r' o',r' 0',x)))
                df1[i]=df1[i].apply(lambda x:(re.sub(r'o ',r'0 ',x)))
                df1[i]=df1[i].apply(lambda x:(re.sub(r'[^0-9\.\:\;\-\ \,]',r'',x)))
            d=pd.DataFrame()
            for ind in df1.index:
                lst=[c.strip().split() for c in df1.loc[ind,:].to_list()]
                lst=[item for sublist in lst for item in sublist]
                mod_lst=[]
                for ele in lst:
                    try:
                        if ele=='00' and len(mod_lst)>0:
                            mod_lst[-1]=mod_lst[-1]+'.00'
                        elif len(ele)==2 and re.match(r"^\d{2,3}$|^\d{1,2}(\,|\.)\d{3}$",mod_lst[-1]):
                            mod_lst[-1]=mod_lst[-1]+'.'+ele
                        else:
                            mod_lst.append(ele)
                    except IndexError:
                        pass
                lst=mod_lst
                try:
                    if len(lst[-1])==2 and re.match('^\d{2-3}$|^\d{1,3}(,)\d{3}$',lst[-2]):
                        a,b=lst[-2],lst[-1]
                        lst=lst[:-2]
                        lst.append(".".join([a,b]))
                except IndexError:
                    pass
                str_lst=[c.strip().split() for c in df.loc[ind,:].to_list()]
                str_lst=[item for sublist in str_lst for item in sublist]
                str_lst=" ".join(str_lst)
                flg=True
                if re.search(r"\d{,2}hr(\ )[0-9|O|o]{,2}min",str_lst):
                    flg=False
                
                rem=['','.']
                lst=[i for i in lst if i not in rem]
                int_lst,amt_lst=[],[]
                for row in lst:
                    if re.match(r'^\d{1,2}$|^[-.]\d{1,2}$',row):
                        int_lst.append(row)
                    elif re.match(r'^\d+(\.|\ |\;|\,)\d{2}$|^\d{1,3}[.,]\d{3}(\.|\;|\,)\d{2}',row):
                        amt_lst.append(row)
                if len(int_lst)==2 and flg==True:
                    d.at[ind,'quantity']=int_lst[-2]
                    d.at[ind,'discount']=int_lst[-1]
                elif len(int_lst)==3 and flg==True:
                    d.at[ind,'quantity']=int_lst[1]
                    d.at[ind,'discount']=int_lst[-1]
                elif len(int_lst)==4 and int_lst.count('0')==2 and int_lst.count('1')==2 and flg==True:
                    d.at[ind,'quantity']=" ".join(int_lst[:2])
                    d.at[ind,'discount']=" ".join(int_lst[2:])
                else:
                    d.at[ind,'quantity']=""
                    d.at[ind,'discount']=""
                if len(amt_lst)==2:        
                    d.at[ind,'rate']=amt_lst[-2]
                    d.at[ind,'amount']=amt_lst[-1]
                elif len(amt_lst)==1:
                    d.at[ind,'amount']=amt_lst[-1]
                    d.at[ind,'rate']=''
                elif len(amt_lst)==3 and flg==False:
                    d.at[ind,'amount']=amt_lst[-2]
                    d.at[ind,'rate']=amt_lst[-1]

                elif len(amt_lst)==4:
                    if len(set(amt_lst))==1:
                        d.at[ind,'rate']=amt_lst[0]
                        d.at[ind,'amount']=amt_lst[0]
                    elif d.at[ind,'quantity']=='1' and len(set(amt_lst))!=len(amt_lst):
                        d.at[ind,'rate']=max(set(amt_lst), key = amt_lst.count)
                        d.at[ind,'amount']=max(set(amt_lst), key = amt_lst.count)
                    else:
                        d.at[ind,'rate']=" ".join(amt_lst[:2])
                        d.at[ind,'amount']=" ".join(amt_lst[2:])
                elif len(amt_lst)>4:
                    if d.at[ind,'quantity']=='1' and len(set(amt_lst))!=len(amt_lst):
                        d.at[ind,'rate']=max(set(amt_lst), key = amt_lst.count)
                        d.at[ind,'amount']=max(set(amt_lst), key = amt_lst.count)
                    elif len(d.at[ind,'quantity'].split())==2 and len(amt_lst)==5:
                        d.at[ind,'rate']=' '.join(amt_lst[:2])
                        d.at[ind,'amount']=" ".join(amt_lst[2:4])
                if ('Total' in str_lst or 'total' in str_lst) and len(amt_lst)==1 and len(int_lst)==0:
                    d.at[ind,'subtotal']='yes'
                else:
                    d.at[ind,'subtotal']='no'

                if 'rate' in d.columns and 'quantity' in d.columns:
                                        
                    if d.at[ind,'quantity']!="" and d.at[ind,'rate']!="":
                        rate_flag=False
                        if d.at[ind,'rate']==d.at[ind,'amount'] and d.at[ind,'quantity'].strip()!="1":
                            d.at[ind,'gross amount']=d.at[ind,'amount']
                            rate_flag=True
                        else:
                            d.at[ind,"rate"]=float_convert(d.at[ind,"rate"])
                            try:
                                d.at[ind,'gross amount']=str(round((float(d.at[ind,'rate'])*float(d.at[ind,'quantity'])),2))
                            except Exception as e:
                                d.at[ind,'gross amount']=""
                        if d.at[ind,'gross amount']==0:
                            d.at[ind,'gross amount']=d.at[ind,'amount']
                        if rate_flag==True:
                            d.at[ind,"gross amount"]=str(float_convert(d.at[ind,"gross amount"]))
                            try:
                                d.at[ind,"rate"]=str(round(float(d.at[ind,'gross amount'])/float(d.at[ind,'quantity']),2))
                            except Exception as e:
                                pass

                       
                    else:
                        d.at[ind,'gross amount']=""
            


            d['particular']=df['particular']
            df=d
            df=df[df['subtotal']=='no']
            df.drop(columns=['subtotal'],inplace=True)
        else:
            df=df
        
    elif hosp_id in get_id.saptagiri_hospital():
        logging.info("Specific Logic for Saptagiri Hospital")
        
        import numpy as np
        columns_=['particular', 'rate', 'quantity', 'gross amount']    
        
        df.replace(np.nan,'',inplace=True)
        df.replace(' ','',inplace=True)
        

        df=df.astype('str')
            
        pattern=r'\d{2}\-\d{4} [a-zA-Z 0-9]+( \d+\.\d{2}){3}'
        pattern_found=False
        
        for s_index in df.index:
            row = ' '.join(df.loc[s_index].tolist())
            if re.search(pattern,row):
                pattern_found=True
                break
                
        if pattern_found==True:
            for c in df.columns:
                if len(df[df[c]==''])>len(df)*0.70:
                    df.drop([c],axis=1,inplace=True)

            def columns_identify(mdf,col_name):
                sum_col={}
                for k in mdf.columns:
                    mdf[k]=mdf[k].astype(str)
                    if col_name=='particular':
                        sum_col[k]=sum(mdf[k].str.count(r'[A-Za-z ]'))
                    elif col_name=='quantity':
                        if k != 'particulars':
                            sum_col[k]=sum(mdf[k].str.count(r'^\d{1}(\.|\,|\s|\:)\d{,2}$'))#Regex Updated 2401
                    elif col_name=='amt':
                        for k in mdf.columns:
                            if k not in ['particulars','quantity']:
                                mdf[k]=mdf[k].astype(str)
                                sum_col[k]=sum(mdf[k].str.count(r'^\d{2,3}\.\d{1,2}$'))
                        col_idx={}
                        for key in sum_col.keys():
                            if sum_col[key]!=0:
                                index=list(mdf.columns).index(key)
                                col_idx[index]=sum_col[key]
                        mapp={mdf.columns[min(col_idx.keys())]:'rate', mdf.columns[max(col_idx.keys())]:'gross amount'}     
                        mdf.rename(columns=mapp,inplace=True)
                        return mdf

                Keymax = max(sum_col, key= lambda x: sum_col[x])
                mdf.rename(columns={Keymax:col_name},inplace=True)
                return mdf

            df=columns_identify(df,'particular')
            df=columns_identify(df,'quantity')
            df=columns_identify(df,'amt')

            df = df[columns_]
        else:
            df=df
        
    elif hosp_id in get_id.sunrise_hospital():
        """
        Sunrise Hospitals
        --------------------
        Follows a header that is different - amount as unitprice
        """
        logging.info("Specific Logic for Sunrise Hospital")
        
        col=" ".join([i for i in df.columns]).lower()
        if "amount" in col or "qty" in col:
            df.columns=["column_"+str(ind+1) for ind in range(len(df.columns))]
        for i in df.index:
            lst=df.loc[i,:].values.tolist()
            lst=[str(val).lower() for val in lst]
            lst=" ".join(lst)
            if "amount" in lst or "qty" in lst:
                df.drop(i,inplace=True)
                break 

    elif hosp_id in get_id.deenanath():
        logging.info("Specific Logic for Deenanath Hospital")
        
        df.fillna('',inplace=True)
        df.replace(' ','',inplace=True)
        df.replace(',','',regex=True,inplace=True)#Added on 0402
        col_count=0
        
        for col in df.columns:
            col_val=' '.join( df[col].values.tolist())
            if len(re.findall(r'\d+(?:\.\d+)', col_val))>len(df)*0.70:
                col_count+=1
        if col_count>3:
            df=df
        
        else:
            sum_cols={}
            for k in df.columns:
                df[k]=df[k].astype(str)
                sum_cols[k]=sum(df[k].str.count(r'\d{2,5}(?:\.\d+)'))
            Keymax = max(sum_cols, key= lambda x: sum_cols[x])
            df.rename(columns={Keymax:'gross amount'},inplace=True)

            for i in df.index:
                if df.loc[i]['gross amount']=='':
                    try:
                        row = ' '.join(df.loc[i].values.tolist())
                        x=re.search(r'\d+(?:\.\d+)',row)
                        df.loc[i]['gross amount']=x.group(0)
                    except AttributeError:
                        pass
            df.columns=['empty'+str(i) for i in range(len(df.columns))]
    elif hosp_id in get_id.manipal() and ttc=='Detail':
        logging.info("Specific Logic for Manipal Hospital")
        
        dfa=pd.DataFrame()
        cols=[]
        cnt=0
        df1=df.copy()
        for j in range(0,len(df.columns)):
            if str(df.columns[j])=='nan' or df.columns[j]=="" or df.columns[j]==" ":
                cnt +=1
                cols.append('Empty'+str(cnt))
            else:
                cols.append(df.columns[j])
        df.columns=cols
        collst=list(df.columns)
        df=df.astype('str')
        df.replace('nan','',inplace=True)
        df.replace('LO','5',inplace=True)
        subtot=[]
        for row in df.index:
            lst=df.loc[row,:].values.tolist()
            ele=" ".join(lst)

            ele_lst=ele.split()
            b=re.findall(r'[a-zA-Z ]+',ele)
            b=[i for i in b if i!=' ' and i!="  "]
            ele_lst=[re.sub(r'[^0-9\,\.\ ]', '', ele) for ele in ele_lst]
            a = [i for i in ele_lst if re.match(r'^\d+[.,]\d{2}$|^\d{1,2}[.,]\d{3}[,.]\d{2}$',i)]
            l=[]
            for j in a:
                if re.match(r'^\d{1,2}(\,)\d{3}(\.)\d{2}$',j):
                    j=j.replace(',','')
                    l.append(j)
                if re.match(r'^\d+(\,)\d{2}',j):
                    j=j.replace(',','.')
                    l.append(j)
                elif re.match(r'^\d{1,2}(\.)\d{3}[.,]\d{2}$',j):
                    j=re.sub(r'^(\d{1,2})(\.)(\d{3})([.,])(\d{2})$',r'\1\3\4\5',j) 
                    l.append(j.replace(",","."))
                else:
                    j=j
                    l.append(j)
            a=l
            if len(a)==2:
                dfa.at[row,"rate"]=a[0]
                dfa.at[row,'total']=a[1]
                if float(a[0])!=0:
                    if round(float(a[1])/float(a[0]))==0:
                        dfa.at[row,'quantity']=str(round(float(a[1])/float(a[0]),2))
                    else:
                        dfa.at[row,'quantity']=str(round(float(a[1])/float(a[0])))
                else:
                    dfa.at[row,'quantity']=""
                dfa.at[row,'particular']=(" ".join(b)).strip()
            elif len(a)==1:
                dfa.at[row,'rate']=""
                dfa.at[row,'quantity']=""
                dfa.at[row,'total']=a[0]
                dfa.at[row,'particular']=(" ".join(b)).strip()
            elif len(a)==3 or len(a)==4:
                dfa.at[row,'rate']=a[-3]
                dfa.at[row,'discount']=a[-2]
                dfa.at[row,'total']=a[-1]
                dfa.at[row,'particular']=(" ".join(b)).strip()
                if float(a[0])!=0:
                    if round(float(a[2])/float(a[0]))==0:
                        if float(a[-3])!=0:
                            
                            dfa.at[row,'quantity']=str(round(float(a[-1])/float(a[-3]),2))
                        else:
                            dfa.at[row,'quantity']=""
                    else:
                        if float(a[-3])!=0:
                            
                            dfa.at[row,'quantity']=str(round(float(a[-1])/float(a[-3]),2))
                        else:
                            dfa.at[row,'quantity']=""
                    try:
                        if float(dfa.at[row,'quantity'])>100:
                            dfa.at[row,'rate']=a[-2]
                            dfa.at[row,'total']=a[-1]
                            dfa.at[row,'quantity']=str(round(float(a[-1])/float(a[-2])))
                            dfa.at[row,'discount']=""
                    except Exception:
                        dfa.at[row,'quantity']=""

                else:
                    dfa.at[row,'quantity']=""

            else:
                dfa.at[row,'rate']=""
                dfa.at[row,'quantity']=""
                dfa.at[row,'total']=""
                dfa.at[row,'particular']=(" ".join(b)).strip()
            if dfa.at[row,'particular']=="" or len(dfa.at[row,'particular'])<4 or re.match(r'\(\\d+\/\d+\/\d+',dfa.at[row,'particular']) or "charged" in str(dfa.at[row,'particular']).lower():
                try:
                    dfa.at[row,'particular']=dfa.at[row-1,'particular']
                    if dfa.at[row,'particular']=="":
                        dfa.at[row,'particular']=dfa.at[row-2,'particular']
                        
                    
                except:
                    dfa.at[row,'particular']=""
            if 'discount' in dfa.columns:
                if dfa.at[row,'total']==dfa.at[row,'discount']:
                    dfa.at[row,'discount']=""
            if 'sub total' in dfa.at[row,'particular'].lower():
                subtot.append(row)
        if 'tax' in " ".join([str(i).lower().strip() for i in df.columns]):
            try:
                dfa.drop(columns=['discount'],inplace=True)
            except KeyError:
                pass

        dfa.drop(subtot,inplace=True)
        df=dfa
        if len(set(df['total'].tolist())) == 1 and df['total'].tolist()[0]=='':
            df = df1        
        #fortis
    elif hosp_id in get_id.fortis():
        df.columns=['empty'+str(xindex+1) for xindex in range(len(df.columns))]
        logging.info("Specific Logic for Fortis Hospital")
        patt_found=False

        for row in df.index:
            items=[i.strip() for i in list(df.loc[row,:].values)]
            #12 12.12 12.12 12.12
            x=re.search(r'\d+\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+'," ".join(items))
            if x!=None:
                patt_found=True
                break
        if patt_found==True:
            for k in df.columns:
                df[k]=df[k].astype(str)
                df[k]=df[k].apply(lambda x:x.strip())
                df[k]=df[k].apply(lambda x:preprocess(x))
            p_col=particulars_finding(df)
            cols=list(df.columns)[list(df.columns).index(p_col):]
            df1=df[cols]        
            data=pd.DataFrame(columns=["particulars","quantity","gross","discount","amount"])
            data["particulars"]=df[p_col]
            for ind in df1.index:
                lst=[str(item).strip() for item in list(df1.loc[ind,:].values) if str(item).strip()!=""]
                qty_lst=[]
                amt_lst=[]
                for item in lst:
                    if re.match(r'^\d{,3}$',item):
                        qty_lst.append(item)
                    if re.match(r'^\d+[.,]\d{2}$|^\d+(\,|\.|\:)\d{3}[.,]\d{2}$',item):
                        if re.match("^\d\,\d{3}\.\d{2}$",item):
                            amt_lst.append(item.replace(",",""))
                        elif re.match("^\d+(\:)\d{3}(\.)\d{2}$",item):
                            amt_lst.append(item.replace(":",""))
                        elif re.match(r"^\d+\,\d{2}$",item):
                            amt_lst.append(item.replace(",","."))
                        elif re.match("^\d+\,\d{3}\,\d{2}$",item):
                            amt_lst.append(re.sub("(^\d+)(\,)(\d{3})(\,)(\d{2})$",r'\1\3.\5',item))
                        else:
                            amt_lst.append(item)

                if len(qty_lst)==1:
                    data.at[ind,"quantity"]=qty_lst[0]
                if len(amt_lst)==3:
                    data.at[ind,"gross"]=amt_lst[0]
                    data.at[ind,"discount"]=amt_lst[1]
                    data.at[ind,"amount"]=amt_lst[2]
                if len(amt_lst)==5:
                    data.at[ind,"gross"]=amt_lst[0]
                    try:
                        data.at[ind,"discount"]=round(float(amt_lst[1])+float(amt_lst[2]),2)
                        data.at[ind,"amount"]=round(float(amt_lst[0])-data.at[ind,"discount"],2)
                    except:
                        pass

            # Logic to convert particulars to valid particulars
            for index in df.index:
                part = str(df.at[index,p_col]).strip('.').strip()
                par = re.sub(r'[^A-Za-z ]','',part)

                if len(par) < 4:

                    lst=df.loc[index,:].values.tolist()
                    lst=[i for i in lst if str(i)!="nan"]

                    counter=[(len(re.findall(r'[A-Za-z ]',ele))) for ele in lst]
                    if len(counter)>0:
                        cter=counter.index(max(counter))
                        df.at[index,p_col]=lst[cter]

            if len(data.columns)>1:
                data["particulars"]=df[p_col]
                df=data            
    #Pranaam Hospital
    elif hosp_id in get_id.pranam() and ttc=='Detail':
        
        df.columns=['empty'+str(i+1) for i in range(len(df.columns))]
        df=df.fillna('')
        logging.info("Specific Logic for Pranaam Hospital")
        
        result_df=pd.DataFrame(index=df.index,columns=['Particulars','Quantity','Rate','Amount'])
        result_df=result_df.fillna('')
        
        sum_cols={}
        for k in df.columns:
            df[k]=df[k].astype(str)
            sum_cols[k]=sum(df[k].str.count(r'[A-Za-z ]'))
        Keymax = max(sum_cols, key= lambda x: sum_cols[x])
        result_df['Particulars']=df[Keymax]
        df.drop([Keymax],axis=1,inplace=True)
        cnt=0
        for index in df.index:
            row=df.loc[index].values.tolist()
            row=[j for i in row for j in i.split()]
            qty_lst=[i for i in row if re.match(r'^\d{1}[.,]\d{2}$',i.strip())]
            amt_lst=[i for i in row if re.match(r'^\d+[.,]\d{2}$|^\d+(\,)\d{3}(\.)\d{2}$',i.strip())]
            if len(qty_lst)>0:
                amt_lst=[i for i in amt_lst if i not in qty_lst[0]]
            if len(amt_lst)>1 and len(qty_lst)>0:
                cnt+=1
                result_df.at[index,'Quantity']=qty_lst[0]
                result_df.at[index,'Rate']=amt_lst[0]
                result_df.at[index,"Amount"]=amt_lst[-1]
            elif len(amt_lst)==1:
                result_df.at[index,"Amount"]=amt_lst[0]
        if cnt>len(df)*0.5:
            df=result_df
        else:
            df=df
        
        ###HCG###
    elif hosp_id in get_id.hcg() and ttc=='Detail':
        logging.info("Specific Logic for HCG Hospitals")
        
        df_act=df
        for i in df_act.columns:
            df_act[i]=df_act[i].astype('str')
            df_act[i] = df_act[i].apply(lambda x: preprocess(x))
        t = "No"
        for k in df_act.index:
            r = ' '.join(list(df_act.loc[k,:].values))
            pat = re.search(r'\d{1,2}(\ )((\d{1,2}[.,]\d{3}[.,]\d{2})|(\d{1,3}[.,]\d{2}))(\ )((\d{1,2}[.,]\d{3}[.,]\d{2})|(\d{1,3}[.,]\d{2}))(\ )\d{1,2}(\ )((\d{1,2}[.,]\d{3}[.,]\d{2})|(\d{1,3}[.,]\d{2}))',r)
            if pat != None:
                t = "yes"
                break
        if t == "yes":
            df_act.columns=['empty'+str(co) for co in range(len(df_act.columns))]
            def particulars(df_act):
                sc={}
                for i in df_act.columns:

                    sc[i]=sum(df_act[i].str.count(r'[A-Za-z ]'))
                perti_col = max(sc, key= lambda x:sc[x])
                return perti_col
            perti_col=particulars(df_act)

            New_df = pd.DataFrame()

            df_act1 = df_act.drop(df_act.columns[0:list(df_act.columns).index(perti_col)+1],axis=1)
            for j in df_act1.columns:                      
                df_act1[j] = df_act1[j].apply(lambda x: re.sub(r'[^0-9\.\,\-\;\:\ ]','',x))
            for i in df_act1.index:
                df_act1.loc[i,:].values
                l = [s for s in df_act1.loc[i,:].values if s != 'nan']
                l = [preprocess(x) for k in l for x in str(k).split()]
                qt = []
                amts = []
                for val in l:
                    if re.match(r'^\d{1,4}(\.|\,|\ )\d{2}$|^\d{1,2}(\,|\.|\ )\d{3}(\,|\.|\ )\d{2}$',val):
                        amts.append(val)
                    elif re.match(r'^\d{1,2}$',val):
                        qt.append(val)
                if len(amts) > 0:
                    New_df.at[i,'Amount']=amts[-1]
                    if len(amts) > 1:
                        New_df.at[i,'Unit Price']=amts[0]
                    else:
                        New_df.at[i,'Unit Price']=""
                else:
                    New_df.at[i,'Unit Price']=""
                    New_df.at[i,'Amount']=""
                if len(qt) == 1:
                    if qt[0] == "0":
                        New_df.at[i,"Discount"] = qt[0]
                        New_df.at[i,"Quantity"] = ""
                    else:
                        New_df.at[i,"Quantity"] = qt[0]
                        New_df.at[i,"Discount"] = ""
                elif len(qt) == 2:
                    New_df.at[i,"Quantity"] = qt[0]
                    New_df.at[i,"Discount"] = qt[1]
                else:
                    New_df.at[i,"Quantity"] = ""
                    New_df.at[i,"Discount"] = ""
            New_df["Particulars"] = df_act[perti_col]
            New_df = New_df[['Particulars','Quantity','Unit Price','Discount','Amount']]
        else:
            New_df = df_act
        df=New_df
        
    elif hosp_id in get_id.opjindal():
        logging.info("Specific Logic for Op Jindal Hospital")
        
        df.columns=[str(i) for i in df.columns]
        flag=False
        for ind in df.index:
            lst=list(df.loc[ind,:].values)
            lst=[item for item in lst if str(item) not in ['nan',""]]
            pat=re.search(r'\d+(\.)\d{2} (\×|x|X) \d+(\.)\d{2}'," ".join(lst))
            if pat!=None:
                flag=True
                break
        if flag==True:
            
            final_df=pd.DataFrame()
            p_col=particulars_finding(df)
            amt_col=list(df.columns)[-1]
            final_df['Particulars']=df[p_col]
            final_df['Gross Amount']=df[amt_col]
            for ind in df.index:
                lst=list(df.loc[ind,:].values)
                lst=[item for item in lst if str(item) not in ['nan',""]]
                pat=re.search(r'\d+(\.)\d{2} (\×|x|X) \d+(\.)\d{2}'," ".join(lst))
                if pat!=None:
                    final_df.at[ind,'Quantity']=pat.group().split()[-1]
                    final_df.at[ind,'Rate']=pat.group().split()[0]
            df=final_df[['Particulars',"Quantity","Rate","Gross Amount"]]
        else:
            df=df
            
    ##R G Urological 
    elif hosp_id in get_id.motherhood() and ttc=="Detail":
        logging.info("Specific logic for R G Urological / Motherhood")
    
        df_act=df.copy()

        New_df = pd.DataFrame()

        perti_col=particulars_finding(df_act)

        df_act.rename(columns={perti_col:"Particular"},inplace=True)

        New_df['Particulars'] = df_act['Particular']

        for j in df_act.columns:                           
            df_act[j] = df_act[j].apply(lambda x: re.sub(r'[^0-9\.\,\-\;\:\ ]','',x))
        cnt=0
        for i in df_act.index:
            l = [s.strip() for s in list(df_act.loc[i,:].values) if s != 'nan']
            l = [s for s in l if re.match(r'(^(()|\-)\d{1,4}(\.|\,|\ )|(()|\-)\d+(\.|\,|\ )\d{2}$)|(^(()|\-)\d{1,2}(\,|\.|\ )\d{3}(\,|\.|\ )\d{2}$)',s)]
            if len(l) == 4:
                cnt+=1
                New_df.at[i,'Price'] = l[0]
                New_df.at[i,'Quantity'] = l[1]
                New_df.at[i,'Discount'] = l[2]
                New_df.at[i,'Net Amount'] = l[3]
        if len(New_df.columns) == 1:
            New_df = df
        if cnt>len(df)*0.5:
            df = New_df
    elif hosp_id in get_id.yatharth() and ttc=="Detail":
        logging.info("Specific Logic for Yatharth")

        perti_col=particulars_finding(df)        
        df['Particular']=df[perti_col]
        final_df = pd.DataFrame()
        flag=0
        for r in df.index:
            e = list(df.loc[r,:].values)
            e = [i for i in e if str(i) not in [' ','','nan']]
            o = " ".join(e)    
            ll = re.search(r'\@',o)
            if ll != None:
                flag=1
                break
        if flag==1:
            for k in df.index:    
                l = [s for s in list(df.loc[k,:].values) if s != 'nan']
                l = [s for s in l if re.match(r'^(()|\-)\d{1,5}(\.|\,|\ )\d{2}$|^(()|\-)\d{1,2}(\,|\.|\ )\d{3}(\,|\.|\ )\d{2}$',s)]
                 
                if len(l) == 1:
                    final_df.at[k,'Amount'] = l[0]
                amt = []
                for i in df.at[k,'Particular'].split():
                    i=str(i).lower().replace("rs.","")
                    i=i.replace("rs","")
                    pat = re.search(r'^(()|\-)\d{1,5}(\.|\,|\ )\d{2}$|^(()|\-)\d{1,2}(\,|\.|\ )\d{3}(\,|\.|\ )\d{2}$',i)
                    if pat != None:           
                        amt.append(pat.group(0))
                if len(amt) > 1:
                    final_df.at[k,'Price']=amt[0]
                    final_df.at[k,'Quantity']=amt[-1]
            if len(final_df)>0:
                if all(x in list(final_df.columns) for x in ['Price', 'Quantity','Amount']):
                    final_df['Particulars'] = df['Particular']
                    final_df=final_df[["Particulars","Quantity","Price","Amount"]]
                elif all(x in list(final_df.columns) for x in ["Amount"]):
                    final_df['Particulars'] = df['Particular']
                    final_df=final_df[["Particulars","Amount"]]
                final_df["Particulars"]=df["Particular"]
                df=final_df
            else:
                df=df
        else:
            df=df
    elif hosp_id in get_id.slraheja() and ttc=="Detail":
        logging.info("Specific Logic for S L Raheja")
        f=pd.DataFrame()
        par_col=particulars_finding(df)
        f["particular"]=df[par_col]
        columns=list(df.columns)[list(df.columns).index(par_col)+1:]
        def fc_con(x):
            x=x.strip()
            if re.match(r"^\d+\,\d{,2}$",x):
                x=x.replace(",",".")
            else:
                x=x.replace(",","")
            return x
        for ind in df.index:
            lst=[val for val in df.loc[ind,columns].values.tolist() if val!=""]
            fc=[val for val in lst if re.match("^\d+[.,]\d{1,2}$|^\d+(\,)\d{3}(\.)\d{1,2}$",val)]
            if len(fc)>0:
                f.at[ind,"net amount"]=fc[-1]
            ic=[val for val in lst if re.match("^\d{1,4}$",val)]
            if len(ic)==1:
                f.at[ind,"quantity"]=ic[0]
            elif len(ic)==2 and len(fc)>0:
                f.at[ind,"quantity"]=ic[0]
                f.at[ind,"discount"]=ic[-1]
                f.at[ind,"gross amount"]=float(fc_con(fc[-1]))+float(ic[-1])
            if len(fc)==2 and len(ic)==1:
                f.at[ind,"quantity"]=ic[0]
                f.at[ind,"discount"]=fc[0]
                f.at[ind,"gross amount"]=float(fc_con(fc[-1]))+float(fc_con(fc[-2]))
                
        if "gross amount" not in f.columns:
            f["gross amount"]=""
            f["discount"]=""
        if len(f.columns)==5:
            df=f[["particular","quantity","gross amount","discount","net amount"]]
    elif hosp_id in get_id.chord_road():
        logging.info("Specific Logic for Chord Road")
        
        flage = 0
        New_df = pd.DataFrame()
        par_col=particulars_finding(df)
        for k in df.index:
            l = [s for s in list(df.loc[k,:].values) if re.match(r"(\d+\.\d{2}|\d+)( x |x| x|x )\d{1,2}",str(s)) ]
            if len(l) > 0:
                flage = 1
                break
        if flage == 1:
            for j in df.index:
                l = [s for s in list(df.loc[j,:].values) if re.match(r"(\d+\.\d{2}|\d+)( x |x| x|x )\d{1,2}",str(s)) ]
                if len(l) > 0:
                    New_df.at[j,'price']=l[0].split('x')[0].strip()
                    New_df.at[j,'qty']=l[0].split('x')[-1].strip()
                h = [u.strip() for u in list(df.loc[j,:].values) if re.match(r'\d+\.\d{1,2}$',str(u))]
                if len(h) > 0:
                    New_df.at[j,'amount']=h[-1]
            New_df['particular']=df[par_col] 
            df=New_df
    
    elif hosp_id in get_id.lislie_hospital() and ttc=="Detail":

        perti_col=particulars_finding(df)
        df.drop(columns=df.columns[:list(df.columns).index(perti_col)],inplace=True)
        df.rename(columns={perti_col:"particular"},inplace=True)
        logging.info("Specific Logic for Lisie Hospital")
        final_df=pd.DataFrame()
        for i in df.index:
            lst=df.loc[i,:].values.tolist()
            if hosp_id=="186295":
                lst=[j for k in lst for j in str(k).split()]
            lst=[ele for ele in lst if re.match(r'^\d+$|^\d+\.\d{2}$|^\d+[.,]\d{3}[.,]\d{2}$',str(ele).strip())]
            if len(lst)>1:
                final_df.at[i,"gross amount"]=lst[-1]
                final_df.at[i,"quantity"]=lst[0]
            if len(lst)==1:
                final_df.at[i,"gross amount"]=lst[0]
            final_df["particular"]=df["particular"]
        if "quantity" in final_df.columns:
            df=final_df[["particular","quantity","gross amount"]]
        else:
            df=final_df[["particular","gross amount"]]
    elif hosp_id in get_id.amritha():
        logging.info("Specific Logic for Amritha")

        fl = 0
        indexes=list(df.index)[:3]
        for ind in indexes:
            val = df.loc[ind,:].values.tolist()
            val = map(lambda item : str(item).lower(), val)
            val = ' '.join(val)

            if 'debit' in val:
                fl = 1
                df.columns=df.loc[ind,:].values
                break

        if fl:
            for i in df.index:
                ocr_values = df.loc[i].values.tolist()
                val = ' '.join([str(val) for val in ocr_values])
                try:
                    if val.lower().startswith('bill'):
                        df.drop(i, inplace=True)
                    else:
                        match = re.findall(r'\d+\.\d{1,2}', val)
                        if(match):
                            df.at[i,'amount'] = round(float(match[-2]) - float(match[-1]),2)
                        else:
                            df.at[i,'amount'] = np.nan


                except (TypeError, IndexError):
                    pass

            cols = df.columns
            duplicated_idx = df.columns.duplicated()
            duplicated = df.columns[duplicated_idx].unique()
            rename_cols = []
            dupl_no = 1
            df.drop(columns=cols[-3:-1], inplace=True)
            for col in df.columns:

                if col in duplicated:
                    rename_cols.extend([str(col) + '_' + str(dupl_no)])
                    dupl_no+=1
                else:
                    rename_cols.extend([col])

            df.columns = rename_cols
            p_col=particulars_finding(df)
            df.rename(columns={p_col:"particular"},inplace=True)
	
        else:
            df = df

    elif hosp_id in get_id.sparsh():
        logging.info("Specific Logic for Sparsh Hospital")
        l1 = set(['medicine', 'consumable'])
        for i in df.index:
            values = df.loc[i,:].tolist()
            values = set(values)
            if l1.intersection(values):
                df.drop(i, inplace=True)
        
    elif hosp_id in get_id.woodland() and ttc=="Detail":
        
        p_col=particulars_finding(df)
        logging.info("Specific Logic for Woodland Hospitals")
        df.rename(columns={p_col:"particular"},inplace=True)
        data=pd.DataFrame()
        data["particular"]=df["particular"]
        for i in df.index:
            line=list(df.loc[i,:].values)
            floats=[item for item in line if re.match("^\d+\.\d{2}$",str(item))]
            if len(floats)>=4:
                data.at[i,"rate"]=floats[0]
                data.at[i,"gross"]=floats[1]
                data.at[i,"discount"]=floats[2]
                if len(floats)==4:
                    data.at[i,"amount"]=floats[3]

                if len(floats)>4:
                    data.at[i,"amount"]=str(float(floats[-1])-float(floats[-2]))
        df=data

    elif hosp_id in get_id.ck_memorial() and ttc=="Summary":
        df.columns=['empty'+str(xindex+1) for xindex in range(len(df.columns))]

        for k in df.columns:
            df[k]=df[k].astype(str)
            df[k]=df[k].apply(lambda x:x.strip())
            df[k]=df[k].apply(lambda x:preprocess(x))
            p_col=particulars_finding(df)
            cols=list(df.columns)[list(df.columns).index(p_col):]
            df1=df[cols]        
            data=pd.DataFrame()
            amt_col=list(df.columns)[-1]
            data["particulars"]=df[p_col]
            data['gross amount']=df[amt_col]
        
        df=data

    
    for i in df.index:
        df.at[i,"bb_rowwise"]=seri.at[i,"bb_rowwise"]
        df.at[i,"con_rowwise"]=seri.at[i,"con_rowwise"]
    return df

#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################