from re import I


def Post_processing(df,post_processing_dict,ppdict_pm,ppdict_par,pagerule):
    import numpy as np
    import pandas as pd
    import string
    import re
    import gc
    from bill_modules.utils import merge_separator,remove_bug_from_amt_cols
    ppdict_pm=[item for item in ppdict_pm["Items"].to_list() if item not in [""]]
    ppdict_par=[item for item in ppdict_par["Items"].to_list() if item not in [""]]
    ign_items=["medi assist","total","balance","share","tax","bill amount","payer","discount","grand total","tpa pvt ltd","mediassist","total for"]
    def quantity_corrector(i):
        i=str(i)
        i=i.rstrip('.').strip()
        i=i.strip(';').strip()
        i=i.rstrip('.').strip()        
        i=i.rstrip('-').strip()
        i=i.strip(",").strip()
        if i.count('.')>1:
            i=i.replace('.'," ")
            i=i.strip()
            pagerule.append(f"Quantity value extracted with incorrect format {i}")
        if i.count(":")>1:
            i=i.replace(':'," ")
            i=i.strip()
        i=i.strip(":").strip()
        if i.count('-')>1:
            i=i.replace("-","")
            i=i.strip()
        if re.match(r'^(\-) \d{1,2}$|^(\-) \d+(\.)\d+$',i):
            i="-"+i.strip('-').strip()
        if re.match(r'^((?:)|\-)(\ |(?:)|\: )\d{,2}(\,|\: |\:|\ )0{1,3}$',i):
            i=re.sub(r'^((?:)|\-)(\ |(?:)|\: )(\d{,2})(\,|\: |\:|\ )(0{1,3})$',r'\1\3.\5',i)
            pagerule.append(f"Excess spaces removed from qty {i}")
        if re.match(r"^\d{1,2}[:;]\d{1,3}$",i):
            i=re.sub(r"^(\d{1,2})([:;])(\d{1,3})$",r'\1.\3',i)
        i=re.sub(r'^:+$|^-+$',r"",i)
        if re.match(r'^\d+\,\s\d+$',i):
            i=i.split(',')[-1].strip()
        return i
    def str_remove(i):
        i=str(i)
        i=re.sub(r'[^0-9\.\,\-\;\:\ ]','',i)
        i=i.strip(':').strip().rstrip('.')
        i=i.rstrip('-').strip()
        if re.match(r'^\d+(\,)\d{2}$',i):
            i=i.replace(',','.')
        return i
    df["Quantity"]=df["Quantity"].apply(lambda x:quantity_corrector(x))
    df['Discount']=df['Discount'].apply(lambda x:str_remove(x))
    amt_variants=["Unit_price",'Discount',"Before_discount_amount","After_discount_amount"]
    for i in amt_variants:
        try:
            df[i]=df[i].apply(lambda x: merge_separator(str(x),pagerule))
        except Exception:
            continue
    
    p_dict=[str(i).lower() for i in post_processing_dict['Title'].to_list() if str(i).lower() not in ["nan",""," "]]
        
    gc.collect()
    for i in df.index:
        if str(df.at[i,"Particular"]).strip() in ["na","nan","NE","NaN",""] or str(df.at[i,'Particular']).startswith('empty'):
            df.drop(i,inplace=True)
    if len(df)>0:
        if str(df.at[list(df.index)[0],"Particular"]).startswith(("mrs.","mr.","miss.","name")):
            df.drop(list(df.index)[0],inplace=True)
    df = df[df['Particular'].notna()] 

    final_dfa=df
    del df
   
    final_dfa.reset_index(inplace = True,level=0)
    final_dfa.replace("*",np.nan,inplace=True)


    final_dfa_rows_to_kill=[]
    final_dfa=final_dfa.astype('str')
    def merging_issue(data):
        data.replace("",'nan',inplace=True)
        temp_=pd.DataFrame()
        rows_to_drop=[]
        for i in data.index:
            ql=[item for item in data.at[i,"Quantity"].split() if item not in ["00","000","0"]]
            ul=[item for item in data.at[i,"Unit_price"].split() if item not in ["00","000","0"]]
            bl=[item for item in data.at[i,"Before_discount_amount"].split() if item not in ["00","000","0"]]
            al=[item for item in data.at[i,"After_discount_amount"].split() if item not in ["00","000","0"]]
            
            if len(ql)==len(ul)==1 and len(bl)==len(al) and len(al)>1:
                """
                If BDA AND ADA are merged but quantity and unitprice are not merged
                """
                if ul==["nan"]:
                    temp=pd.DataFrame([data.loc[i,['index','Particular','Quantity','Unit_price','Before_discount_amount','After_discount_amount',"bb_rowwise","con_rowwise"]]]*len(ul)).reset_index()
                    rows_to_drop.append(i)
                    for l in range(len(al)):
                        temp.at[l,"Unit_price"]=ul[0]
                        temp.at[l,"Quantity"]=ql[0]
                        temp.at[l,"Before_discount_amount"]=bl[l]
                        temp.at[l,"After_discount_amount"]=al[l]
                    temp_=pd.concat([temp_,temp],axis=0)
                elif ul==ql==["nan"]:
                    try:
                        if round(float(ul[0])*float(bl[0]))==round(float(bl[1])):
                            data.at[i,"Quantity"]=bl[0]
                            data.at[i,"Before_discount_amount"]=data.at[i,"After_discount_amount"]=bl[-1]
                    except ValueError:
                        pass
                    try:
                        if round(float(ul[0])*float(bl[-1]))==round(float(bl[0])):
                            data.at[i,"Quantity"]=bl[-1]
                            data.at[i,"Before_discount_amount"]=data.at[i,"After_discount_amount"]=bl[0]
                    except ValueError:
                        pass
                    try:
                        if round(float(ul[0]))==round(float(bl[0])):
                            data.at[i,"Quantity"]="1"
                            data.at[i,"Before_discount_amount"]=data.at[i,"After_discount_amount"]=bl[0]
                    except ValueError:
                        pass
                    try:
                        if round(float(ul[0]))==round(float(bl[-1])):
                            data.at[i,"Quantity"]="1"
                            data.at[i,"Before_discount_amount"]=data.at[i,"After_discount_amount"]=bl[-1]
                    except ValueError:
                        pass

                else:
                    for e in bl:
                        try:
                            if float(ql[0])*float(ul[0])==float(e):
                                data.at[i,"Before_discount_amount"]=e
                                data.at[i,"After_discount_amount"]=e
                        except ValueError:
                            try:
                                if float(ql[0])*float(ul[0])==float(e.replace(',',"")):
                                    data.at[i,"Before_discount_amount"]=e
                                    data.at[i,"After_discount_amount"]=e
                            except ValueError:
                                pass

                    if len(bl)==2:
                        if bl[0]=="0" or bl[-1]=="0":
                            if len([ele for ele in bl if ele!="0"])!=0:
                                data.at[i,"Before_discount_amount"]=[ele for ele in bl if ele!="0"][0]
                                data.at[i,"After_discount_amount"]=[ele for ele in bl if ele!="0"][0]
                        else:
                            try:
                                if round(float(ql[0])*float(ul[0]))==round(float("".join(bl))):
                                    data.at[i,"Before_discount_amount"]="".join(bl)
                                    data.at[i,"After_discount_amount"]="".join(bl)

                            except ValueError:
                                pass
                        if bl[0]==bl[-1]:
                            data.at[i,"Before_discount_amount"]=data.at[i,"After_discount_amount"]=bl[0]
            if len(ul)>1 and len(ql)==len(bl)==len(al)==1:
                """
                If Unitprice is merged, rest not merged
                """
                ul1=[item for item in ul if len(str(item).split(".")[0])<6]
                if len(ul1)==0:
                    data.at[i,"Unit_price"]=""
                elif len(ul1)==1:
                    data.at[i,"Unit_price"]=ul1[0]
                elif len(ul1)==2:
                    if len(ul1[0])==1 and len(ul1[-1])>1:
                        data.at[i,"Unit_price"]=ul1[-1]
                        pagerule.append(f'Unitprice alone merged at {ul}')
                
            if len(ul)==len(ql)==len(bl)==1 and len(al)>1:
                """
                If ADA alone merged, rest not merged
                """
                if ul[0]==ql[0]==bl[0]=="nan":
                
                    temp=pd.DataFrame([data.loc[i,['index','Particular','Quantity','Unit_price','Before_discount_amount','After_discount_amount',"bb_rowwise","con_rowwise"]]]*len(al)).reset_index()
                    rows_to_drop.append(i)
                    for l in range(len(al)):
                        temp.at[l,'After_discount_amount']=al[l]
                    temp_=pd.concat([temp_,temp],axis=0)
                else:
                    flo=False
                    for item in al:
                        try:
                            if round(float(ql[0])*float(ul[0]))==round(float(item)):
                                data.at[i,"After_discount_amount"]=item
                                flo=True
                                break
                        except Exception:
                            pass
                    if flo==False:               
                        data.at[i,"After_discount_amount"]=al[0]
                        
            if len(ul)==len(bl)==len(al) and len(ul)>1:
                """
                If everything merged
                """
                pagerule.append('Multiple rows merged')
                temp=pd.DataFrame([data.loc[i,['index','Particular','Quantity','Unit_price','Before_discount_amount','After_discount_amount',"bb_rowwise","con_rowwise"]]]*len(ul)).reset_index()
                rows_to_drop.append(i)
                for l in range(len(ul)):

                    temp.at[l,'Unit_price']=ul[l]
                    temp.at[l,'Before_discount_amount']=bl[l]
                    temp.at[l,'After_discount_amount']=al[l]
                    if len(ql)==len(ul):
                        temp.at[l,"Quantity"]=ql[l]
                    elif len(ql)==1:
                        temp.at[l,"Quantity"]=ql[0]
                temp_=pd.concat([temp_,temp],axis=0)
                
                
            if len(ul)==len(ql)==len(al)==1 and len(bl)>1 and ul[0]=="nan":
                """BDA alone merged"""
                
                bl1=[item for item in bl if item not in ["0.00","0","00"]]
                if ql==ul==al==["nan"] and len(bl)>1 and len(bl[0].split(".")[0])<2 and bl[0] not in ["0.00","0"]:
                    data.at[i,"Quantity"]=bl[0]
                    data.at[i,"Before_discount_amount"]=bl[1]
                    
                
                    
                else:
                
                    if len(bl1)!=0:
                        bl=bl1
                    if len(bl)>1:
                       
                        if len(bl)==2 and len(bl[0].lstrip("0"))==1 and re.match("^\d+\,\d{3}\.\d{2}$|^\d+\.\d{2}$",bl[1]):
                            data.at[i,"Before_discount_amount"]=data.at[i,"After_discount_amount"]=bl[-1]
                        

                        else:
                            temp=pd.DataFrame([data.loc[i,['index','Particular','Quantity','Unit_price','Before_discount_amount','After_discount_amount',"bb_rowwise","con_rowwise"]]]*len(bl)).reset_index()
                            rows_to_drop.append(i)
                            for l in range(len(bl)):
                                temp.at[l,"Before_discount_amount"]=bl[l]
                            temp_=pd.concat([temp_,temp],axis=0)
                        
                    elif len(bl1)==1:
                        data.at[i,"Before_discount_amount"]=bl1[0]
            if len(ul)==len(ql)==len(al)==1 and len(bl)>1 and ul[0]!="nan":
                if len(bl[-1].split(".")[0])<2 and bl[-1]!="0" and len(bl[0].split(".")[0])>1:
                    data.at[i,"Quantity"]=bl[-1]
                    data.at[i,"Before_discount_amount"]=bl[0]


            if len(ul)==len(ql) and al==bl==["nan"] and len(ul)>1:
                
                """
                Only up and qty merged
                """
                
                temp=pd.DataFrame([data.loc[i,['index','Particular','Quantity','Unit_price','Before_discount_amount','After_discount_amount',"bb_rowwise","con_rowwise"]]]*len(bl)).reset_index()
                rows_to_drop.append(i)
                for l in range(len(ql)):
                    temp.at[l,"Quantity"]=ql[l]
                    temp.at[l,"Unit_price"]=ul[l]
                temp_=pd.concat([temp_,temp],axis=0)
            if len(ql)==len(bl)==len(al) and ul==["nan"] and len(ql)>1:
                """Only qty merged and up==1"""
                temp=pd.DataFrame([data.loc[i,['index','Particular','Quantity','Unit_price','Before_discount_amount','After_discount_amount',"bb_rowwise","con_rowwise"]]]*len(bl)).reset_index()
                rows_to_drop.append(i)
                for l in range(len(ql)):
                    temp.at[l,"Quantity"]=ql[l]
                    temp.at[l,"Before_discount_amount"]=bl[l]
                    temp.at[l,"After_discount_amount"]=bl[l]
                    
                temp_=pd.concat([temp_,temp],axis=0)
            
            if len(ql)>1 and len(bl)==len(ul)==len(al)==1:
                """Only qty merged and up>1"""
                
                if len(ql)==2 and len(ql[0].split(".")[0])==1:
                    try:
                        if float(ql[0])*float(ql[-1])==float(bl[0]):
                            data.at[i,"Quantity"]=ql[0]
                            data.at[i,"Unit_price"]=ql[-1]
                            pagerule.append(f"Quantity alone is merged {ql} selected {ql[0]}")
                    except Exception:
                        pass
                    
                    try:
                        if float(ql[0])*float(ql[-1])==float(al[0]):
                            data.at[i,"Quantity"]=ql[0]
                            data.at[i,"Unit_price"]=ql[-1]
                            pagerule.append(f'Quantity and up merged {ql}')
                    except Exception:
                        pass
                if len(ql)==2 and len(ql[1].split(".")[0])==1:
                    try:
                        if float(ql[1])*float(ql[0])==float(bl[0]):
                            data.at[i,"Quantity"]=ql[1]
                            data.at[i,"Unit_price"]=ql[0]
                    except Exception:
                        pass
                    
                    try:
                        if float(ql[1])*float(ql[0])==float(al[0]):
                            data.at[i,"Quantity"]=ql[1]
                            data.at[i,"Unit_price"]=ql[0]
                    except Exception:
                        pass
            if len(ql)==len(ul)==len(al)==1 and len(bl)>1 and ul!=["nan"]:
                for item in bl:
                    try:
                        if round(float(ql[0])*float(ul[0]))==round(float(item)):
                            data.at[i,"Before_discount_amount"]=item
                            break
                    except Exception:
                        pass
             

        data.drop(rows_to_drop,inplace=True)
        data=pd.concat([data,temp_],axis=0)
        data['index']=data['index'].astype('float')    
        data=data.sort_values(['index'])
        data=data.reset_index(drop=True)
        data.replace("nan","",inplace=True)
        return data
    
    
    final_dfa=merging_issue(final_dfa)
    final_dfa=final_dfa.astype('str')
    
    amt_variants=["Unit_price","Before_discount_amount","After_discount_amount"]
    
    for i in amt_variants:
        try:
            final_dfa[i]=final_dfa[i].apply(lambda x: merge_separator(str(x),pagerule))
        except Exception:
            continue
    final_dfa['Quantity']=final_dfa['Quantity'].apply(lambda x: quantity_corrector(x))
    final_dfa["Discount"]=final_dfa["Discount"].apply(lambda x:str_remove(x))
    
    for i in final_dfa.index:
        if final_dfa.at[i,'Particular']=="nan" or final_dfa.at[i,'Particular']=="vna" or final_dfa.at[i,'Particular']=="":
            final_dfa_rows_to_kill.append(i)
    for i in final_dfa.index:
        if final_dfa.at[i,'Unit_price'] in ["vna","nan","","Date"] and final_dfa.at[i,"Quantity"] in ["vna","nan","","Date"] and \
        final_dfa.at[i,'Before_discount_amount'] in ["vna","nan","","Date"] and final_dfa.at[i,'Discount'] in ["vna","nan","","Date"] and \
        final_dfa.at[i,"After_discount_amount"] in ["vna","nan","","Date"]:
            final_dfa_rows_to_kill.append(i)
            pagerule.append(f"Null row {i}")
        elif "total for" in final_dfa.at[i,"Particular"] and final_dfa.at[i,"Quantity"] in ["vna","nan","","Date"] and \
        final_dfa.at[i,'Before_discount_amount'] in ["vna","nan","","Date"] and final_dfa.at[i,'Discount'] in ["vna","nan","","Date"] and \
        final_dfa.at[i,"After_discount_amount"] in ["vna","nan","","Date"]:
            final_dfa_rows_to_kill.append(i)
            pagerule.append(f'Suspected Subtotal row {i}')
                            
    def sno_removal(txt):
        if bool(re.match(r"^\d+(\/|\-|\.|\- )\d+(\/|\-|\.|\- )\d{2,4}$",str(txt)))==True:
            txt=txt
        else:

            try:
                txt=txt.strip()
                txt=txt.replace("rs.","")
                txt=txt.replace("rs .","")
                txt=txt.strip(".").strip('₹')
                txt=txt.strip("-")
                txt=txt.strip(".").strip()
                txt=txt.strip("=").strip()
                lst=txt.split()


                x=lst[0]

                x=re.sub(r'[^A-Za-z0-9 ]',r"",x)
                if re.match(r'^\d{1,3}$',x):
                    txt=" ".join(lst[1:])
                    pagerule.append(f"Serial number identified {lst}")
            except IndexError:
                pass
        
        return txt
    temp_va=final_dfa
    for row in final_dfa.index:
        if final_dfa.at[row,'After_discount_amount']=='':
            final_dfa.at[row,"After_discount_amount"]=final_dfa.at[row,'Before_discount_amount']
        final_dfa.at[row,"Particular"]=str(final_dfa.at[row,"Particular"]).replace("%"," ").strip()
    def final_correction(final_dfa,final_dfa_rows_to_kill):
        final_dfa.replace('vna','',inplace=True)
        final_dfa.replace('nan',"",inplace=True)
        final_dfa.fillna('')
        for i in final_dfa.index:
            final_dfa.at[i,"Particular"]=sno_removal(final_dfa.at[i,"Particular"])
            if final_dfa.at[i,'Before_discount_amount'] == '' and final_dfa.at[i,'After_discount_amount'] == '':
                if final_dfa.at[i,'Unit_price']!='' and final_dfa.at[i,'Quantity']=='':
                    final_dfa.at[i,'After_discount_amount']=final_dfa.at[i,'Unit_price']
                elif final_dfa.at[i,'Unit_price'] != '' and final_dfa.at[i,'Quantity'] != '':
                    try:
                        final_dfa.at[i,'After_discount_amount']=final_dfa.at[i,'Before_discount_amount']=float(final_dfa.at[i,"Quantity"])*float(final_dfa.at[i,'Unit_price'])
                        final_dfa.at[i,'Before_discount_amount']=str(round(final_dfa.at[i,'Before_discount_amount'],2))
                        final_dfa.at[i,'After_discount_amount']=str(round(final_dfa.at[i,'After_discount_amount'],2))
                        
                    except Exception:
                        final_dfa.at[i,'After_discount_amount']=final_dfa.at[i,'Before_discount_amount']=final_dfa.at[i,'After_discount_amount']
            if final_dfa.at[i,"Discount"]!="" :
                
                try:
                    
                    if float(final_dfa.at[i,"After_discount_amount"])==float(final_dfa.at[i,"Before_discount_amount"]) and float(final_dfa.at[i,"Discount"])!=0:
                        try:
                            final_dfa.at[i,'Before_discount_amount']=float(final_dfa.at[i,"Quantity"])*float(final_dfa.at[i,'Unit_price'])

                            final_dfa.at[i,'After_discount_amount']=float(final_dfa.at[i,'Before_discount_amount'])-float(final_dfa.at[i,'Discount'])
                            final_dfa.at[i,'Before_discount_amount']=str(round(final_dfa.at[i,'Before_discount_amount'],2))
                            final_dfa.at[i,'After_discount_amount']=str(round(final_dfa.at[i,'After_discount_amount'],2))
                            pagerule.append(f'Ada calculated from discount and bda {i}')

                        except Exception:
                            final_dfa.at[i,'Before_discount_amount']=final_dfa.at[i,'After_discount_amount']
                except Exception:
                    pass
            if final_dfa.at[i,"After_discount_amount"]=="" and final_dfa.at[i,'Before_discount_amount']=="" and final_dfa.at[i,"Unit_price"]=="":
                final_dfa_rows_to_kill.append(i)

                
        return final_dfa
    final_dfa=final_correction(final_dfa,final_dfa_rows_to_kill)
    def default_fill(final_dfa):
        for row in final_dfa.index:
            if len(final_dfa.at[row,"Unit_price"].split(".")[0])>7 and len(final_dfa.at[row,"After_discount_amount"].split(".")[0])<6 and \
            final_dfa.at[row,"After_discount_amount"] not in ["VNA","nan",""] and final_dfa.at[row,"Before_discount_amount"] in ["VNA","nan",""]:
                final_dfa.at[row,"Unit_price"]=""
            
            if final_dfa.at[row,'Before_discount_amount']=='':

                if str(final_dfa.at[row,'After_discount_amount']) not in ["","VNA",'nan','0.00'] and \
                str(final_dfa.at[row,'Discount']) in ["","VNA",'nan']:
                    final_dfa.at[row,'Before_discount_amount']=str(final_dfa.at[row,'After_discount_amount'])

                else:
                    if final_dfa.at[row,'Quantity']!='' and final_dfa.at[row,'Quantity']!='nan':
                        try:
                            final_dfa.at[row,'Before_discount_amount']=float(final_dfa.at[row,'Unit_price'])*float(final_dfa.at[row,'Quantity'])
                            final_dfa.at[row,'Before_discount_amount']=str(round(final_dfa.at[row,'Before_discount_amount'],2))
                            pagerule.append(f'BDA calculated from unitprice and qty {row}')
                        except Exception:
                            final_dfa.at[row,'Before_discount_amount']=str(final_dfa.at[row,'After_discount_amount'])
                            pagerule.append(f'BDA made same as ADA {row}')
                    else:
                        final_dfa.at[row,'Before_discount_amount']=str(final_dfa.at[row,'After_discount_amount'])


        final_dfa=final_dfa.astype('str')

        for i in final_dfa.index:
            if final_dfa.at[i,'Quantity']=="" and final_dfa.at[i,'Unit_price']!="" and final_dfa.at[i,'Before_discount_amount']!="":
                try:
                    if float(final_dfa.at[i,'Unit_price'])!=0:
                        final_dfa.at[i,"Quantity"]=float(final_dfa.at[i,"Before_discount_amount"])/float(final_dfa.at[i,'Unit_price'])
                        final_dfa.at[i,"Quantity"]=str(round(final_dfa.at[i,"Quantity"],2))
                        pagerule.append(f"Quantity calculated from up and bda {i}")
                except Exception:
                    if len(str(final_dfa.at[i,"Unit_price"]).split())==2:

                        lst=str(final_dfa.at[i,"Unit_price"]).split()
                        if len(lst[-1]) < 5:
                            final_dfa.at[i,'Unit_price']=lst[0]
                            final_dfa.at[i,"Quantity"]=lst[1]
                        else:
                            final_dfa.at[i,"Quantity"]=str(final_dfa.at[i,"Quantity"])
                    else:
                        final_dfa.at[i,"Quantity"]=str(final_dfa.at[i,"Quantity"])


            if final_dfa.at[i,'Unit_price']=="" and final_dfa.at[i,'Quantity']!="" and final_dfa.at[i,'Before_discount_amount']!="":
                try:
                    if float(final_dfa.at[i,'Quantity'])!=0:
                        final_dfa.at[i,"Unit_price"]=float(final_dfa.at[i,"Before_discount_amount"])/float(final_dfa.at[i,'Quantity'])
                        final_dfa.at[i,"Unit_price"]=str(round(final_dfa.at[i,"Unit_price"],2))
                except Exception:
                    final_dfa.at[i,"Unit_price"]=final_dfa.at[i,"Unit_price"]
                    final_dfa.at[i,"Unit_price"]=str(final_dfa.at[i,"Unit_price"])


            if re.match(r"-",final_dfa.at[i,'Quantity']) or re.match(r'-',final_dfa.at[i,'Unit_price']) or re.match(r'-',final_dfa.at[i,'Before_discount_amount']):

                if re.match(r"-",final_dfa.at[i,'After_discount_amount']):
                    final_dfa.at[i,'After_discount_amount']=final_dfa.at[i,'After_discount_amount']
                elif re.match(r'^-$|^(-)\1+$',final_dfa.at[i,'Quantity']) or  re.match(r'^-$|^(-)\1+$',final_dfa.at[i,'Unit_price']):
                    final_dfa.at[i,'After_discount_amount']=final_dfa.at[i,'After_discount_amount']
                else:
                    final_dfa.at[i,'After_discount_amount'] = '-'+str(final_dfa.at[i,'After_discount_amount'])
            if re.match(r"-",final_dfa.at[i,'Quantity']) or re.match(r'-',final_dfa.at[i,'Unit_price']) or re.match(r'-',final_dfa.at[i,'After_discount_amount']):
                pagerule.append(f'Negative sign added before all values {i}')  
                if re.match(r"-",final_dfa.at[i,'Before_discount_amount']):
                    final_dfa.at[i,'Before_discount_amount']=final_dfa.at[i,'Before_discount_amount']
                elif re.match(r'^-$|^(-)\1+$',final_dfa.at[i,'Quantity']) or  re.match(r'^-$|^(-)\1+$',final_dfa.at[i,'Unit_price']):
                    final_dfa.at[i,'Before_discount_amount']=final_dfa.at[i,'Before_discount_amount']
                else:
                    final_dfa.at[i,'Before_discount_amount'] = '-'+str(final_dfa.at[i,'Before_discount_amount'])
            try:
                if (float(final_dfa.at[i,"Before_discount_amount"])==float(final_dfa.at[i,"After_discount_amount"])) and final_dfa.at[i,"Discount"]!="":
                    if (float(final_dfa.at[i,"Discount"])==float(final_dfa.at[i,"Before_discount_amount"])==float(final_dfa.at[i,"After_discount_amount"])) or (float(final_dfa.at[i,"Discount"])>float(final_dfa.at[i,"Before_discount_amount"])):
                        final_dfa.at[i,"Discount"]=0
                    
                    else:
                        final_dfa.at[i,"After_discount_amount"]=str(round(float(final_dfa.at[i,"Before_discount_amount"])-float(final_dfa.at[i,"Discount"]),2))
            except Exception:
                pass

            if len(str(final_dfa.at[i,"Quantity"]).split(".")[0])>3:
                try:
                    if float(final_dfa.at[i,"Unit_price"])!=0:
                        final_dfa.at[i,"Quantity"]=str(round(float(final_dfa.at[i,"Before_discount_amount"])/float(final_dfa.at[i,"Unit_price"]),2))
                except Exception as e:
                    final_dfa.at[i,"Quantity"]=final_dfa.at[i,"Quantity"]
            if " " in final_dfa.at[i,"Unit_price"] or "," in final_dfa.at[i,"Unit_price"] or len(str(final_dfa.at[i,"Unit_price"]).split(".")[0])>6:
                try:
                    if float(final_dfa.at[i,"Quantity"])!=0:
                        final_dfa.at[i,"Unit_price"]=str(round(float(final_dfa.at[i,"Before_discount_amount"])/float(final_dfa.at[i,"Quantity"]),2))
                except Exception as e:
                    
                    final_dfa.at[i,"Unit_price"]=final_dfa.at[i,"Unit_price"]
            if len(str(final_dfa.at[i,"Quantity"]))>6:
                final_dfa.at[i,"Quantity"]=""
            if len(str(final_dfa.at[i,"Discount"]).split('.')[0])>4 or " " in str(final_dfa.at[i,"Discount"]):
                try:
                   
                    final_dfa.at[i,"Discount"]=float(final_dfa.at[i,"After_discount_amount"])-float(final_dfa.at[i,"Before_discount_amount"])
                    final_dfa.at[i,"Discount"]=round(final_dfa.at[i,"Discount"],2)
                except Exception as e:
                    final_dfa.at[i,'Discount']='0'                
                

        return final_dfa
    
    final_dfa=default_fill(final_dfa)
    final_dfa=merging_issue(final_dfa)
    amt_variants=["Unit_price","Before_discount_amount","After_discount_amount"]
    for i in amt_variants:
        try:
            final_dfa[i]=final_dfa[i].apply(lambda x: merge_separator(str(x),pagerule))
        except Exception:
            continue
    final_dfa['Quantity']=final_dfa['Quantity'].apply(lambda x: quantity_corrector(x))
    final_dfa["Discount"]=final_dfa["Discount"].apply(lambda x:str_remove(x))
    final_dfa=final_dfa[final_dfa['Particular'].notna()]
    final_dfa=default_fill(final_dfa)
    for i in amt_variants:
        final_dfa[i]=final_dfa[i].apply(lambda x: merge_separator(str(x),pagerule))
    final_dfa_rows_to_kill=list(set(final_dfa_rows_to_kill))
    final_dfa.drop(final_dfa_rows_to_kill,inplace=True)
    for j in ppdict_pm:
        #perfect match
        for i in final_dfa.index:
            if j == final_dfa['Particular'][i].lower().strip():
                final_dfa.drop(i,inplace=True)
                pagerule.append(f'Row dropped as particular had {j}')
    for j in p_dict:
        for i in final_dfa.index:
            
            if j in str(final_dfa['Particular'][i]).lower() and final_dfa['Quantity'][i] in ['vna','nan',""]:
                
                final_dfa.drop(i,inplace = True)
                pagerule.append(f'Row dropped as particular had {j}')

    for j in ppdict_par:
        
        #####code with partial match irrespective of the rest of the other columns
        for i in final_dfa.index:
            if j in str(final_dfa['Particular'][i]).lower():
                final_dfa.drop(i,inplace = True)
                pagerule.append(f'Row dropped as particular had {j}')
                
    for i in final_dfa.index:
        try:
            
            
            if float(final_dfa.at[i,"Before_discount_amount"])==0 and float(final_dfa.at[i,"After_discount_amount"])==0:
                final_dfa.drop(i,inplace=True)
                pagerule.append(f"Row droppped as both ADA and BDA were zero {i}")
            
        except Exception:
            pass
        try:
            if float(final_dfa.at[i,"Before_discount_amount"])==0 and float(final_dfa.at[i,"After_discount_amount"])!=0 and \
            float(final_dfa.at[i,"Unit_price"])!=0 and float(final_dfa.at[i,"Quantity"])!=0:
                final_dfa.at[i,"Before_discount_amount"]=round(float(final_dfa.at[i,"Unit_price"])*float(final_dfa.at[i,"Quantity"]),2)

        except Exception as e:
            pass
    for j in final_dfa.index:
        test_str=str(final_dfa.at[j,'Particular']).strip().strip(",").strip()
        res = len([ele for ele in test_str if ele in string.ascii_uppercase or ele in string.ascii_lowercase])

        if len(str(final_dfa.at[j,'Particular']).strip())<2:
            final_dfa.drop(j,inplace=True)
        elif (str(final_dfa['Particular'][j]).lower().endswith(("due")) or \
        str(final_dfa['Particular'][j]).lower().startswith(('amount','page','issue no','address','ph:','phone no','discount')))and \
        str(final_dfa['Particular'][j]).lower() not in ["pagenax injection"]:

            final_dfa.drop(j,inplace = True)
        elif any(e in str(final_dfa.at[j,'Particular']).strip() for e in ign_items) and len(final_dfa)>1 and str(final_dfa.at[j,'Quantity']) in ['vna','nan',""]:
            
            
            if "wbc" in str(final_dfa.at[j,'Particular']).strip() or "knee" in str(final_dfa.at[j,'Particular']).strip() or "package" in str(final_dfa.at[j,'Particular']):
                tot=0
                
            else:
                final_dfa.drop(j,inplace=True)
                pagerule.append(f'Row removed {j}')
                
        elif "9993" in str(final_dfa.at[j,"After_discount_amount"]) and "9993" in str(final_dfa.at[j,"Before_discount_amount"]):
            final_dfa.drop(j,inplace=True)
            pagerule.append(f'Row removed {j} as amount has HSN code')
        elif re.match(r'^\d{1,2}:\d{2}((?:)|\s)(am|pm)$|^\d{1,2}:\d{2}:\d{2}((?:)|\s)(am|pm)$|^\d{,2}(\/|-)\d{,2}(\?|-)\d{4}\s\d+\.\d+\s(am|pm)$',str(final_dfa.at[j,'Particular']).strip()):
            pagerule.append(f'Row removed {j} as particular has timestamp')
            
            final_dfa.drop(j,inplace=True)
        elif re.match(r'^\d{1,2}(\/|\-)[A-Za-z]{3}(\/|\-)\d{2,4}\s\d{2}(\:|\.)\d{2}\s(am|pm)$',str(final_dfa.at[j,"Particular"].strip())):
            final_dfa.drop(j,inplace=True)
            pagerule.append(f'Row removed {j} as particular has timestamp')

        if (res==0 and bool(re.match('\d{1,2}(\-|\/|\.)\d{1,2}(\-|\/|\.)\d{2,4}',test_str))==False) or re.match("^\d+\.\d+\srs$",test_str):
            try:
                final_dfa.drop(j,inplace=True)
                pagerule.append(f'Row removed {j} as particular has Date')

            except Exception:
                pass
        try:
            if float(str(final_dfa.at[j,"After_discount_amount"])) > 800000:
                final_dfa.at[j,"After_discount_amount"] = '--'
                pagerule.append(f'High amount value found at {float(str(final_dfa.at[j,"After_discount_amount"]))}')
                
            if float(str(final_dfa.at[j,"Before_discount_amount"])) > 800000:
                final_dfa.at[j,"Before_discount_amount"] = '--'
                pagerule.append(f'High amount value found at {float(str(final_dfa.at[j,"Before_discount_amount"]))}')

            if float(str(final_dfa.at[j,"Discount"])) > 800000:
                final_dfa.at[j,"Discount"] = '--'
                pagerule.append(f'High amount value found at {float(str(final_dfa.at[j,"Discount"]))}')

            if float(str(final_dfa.at[j,"Unit_price"])) > 800000:
                final_dfa.at[j,"Unit_price"] = '--'
                pagerule.append(f'High amount value found at {float(str(final_dfa.at[j,"Unit_price"]))}')

        except Exception:
            pass

    del p_dict
    
    return final_dfa,pagerule


#####################################################################
 # Copyright(C), 2022 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################