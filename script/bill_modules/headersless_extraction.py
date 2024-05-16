def Headerless_extraction_module(merged_df,f_header,pagerule):
    """
    Description
    
    Module to identify the exact columns on headerless cases, through 3 approaches
    Approach 1:
    
    Identify Particulars and Before Discount Amount:
        For particulars: Check for the column with the maximum sum of alphanumeric characters.
        For bda: Check for the column with the maximum sum of numericals
    Approach 2:
    
    Identify Unitprice, Quantity and Before Discount Amount
        Using the logic: UP*Qty = BDA
        getting a combinations of 2 numerical columns and then multiplying columns to check whether any one of the column matches with 75% of the product column.
    
    Approach 3:
    Identify Quantity
        Identifying the column which has int/float values between -10 to 10 in 50% of the non-nan cases.
    ____________________________________________________________________________________________________________________    
    Input:
    
    merged_df (dataframe)
        Dataframe from the OCR with modified header names
    f_header (integer -0/1/-1)
        Integer which tells whether the dataframe has a header or not
        if f_header==1:
            Header present
        else:
            Header absent
    
    ____________________________________________________________________________________________________________________
    Output:
    
    merged_df (dataframe)
        Modified DataFrame with 2(par,bda) or 3(par,bda,qty) or 4 columns(par,up,bda,qty)
    if Header present
        key_val (dictionary)
            dictionary which has key, value pairs of the Particulars, BDA , qty (if extracted), up(if extracted) and their corresponding column names 
        columns_to_be_extracted (list)
            list of columns to be extracted
        location_of (dictionary)
            dictionary which stores the position of the columns
    if Header absent
        key_val - empty string, location_of - empty string, columns to be extracted - empty string

    """

    import pandas as pd
    import numpy as np
    import re
    import gc
    from itertools import combinations
    import logging
    merged_df_copy=merged_df.copy()
    merged_df.drop(columns=['bb_rowwise','con_rowwise'],inplace=True)
    merged_df_cp=merged_df.copy()

    def gst_function(df):
        def to_float(x):
            try:
                x=float(x)
            except ValueError:
                pass
            return x
        gst_lit=[6.00,2.5,9,5,12,18]
        df=df.astype(str)
        l=[]
        for i in df.columns: #Bug identified here
            lst=[ele for ele in df[i].to_list() if len(ele.strip()) > 0]
            lst=[to_float(ele) for ele in lst if ele.strip()[0].isnumeric()]
            lst = [to_float(f.strip('% ')) for f in lst if '%' in str(f)]
            res = [ele for ele in lst if ele in gst_lit]
            if len(res) >= 0.85*len(lst) and len(lst) > 1:
                l.append(i)
                pagerule.append(f'gst column removed {i}')
        return l
    def merge_separator(i):
        i=str(i).strip()
        i=re.sub(r'\-\ ',r'-',i)
        if re.match(r'^\d{2}\-[A-Za-z]{3}\-\d{2}$',i):
            i="Date"
        i=re.sub(r'[^0-9\.\,\-\;\:\/\ \=]','',i)
        
        i=i.rstrip('.').rstrip('-')
        i=i.strip('.')
        i=i.strip("=").strip()
        
        
        if ' ' in i:
            lst1=i.split(' ')
            if len(lst1)==2:
                if len(lst1[-1])<=2:
                    a=remove_bug_from_amt_cols(i)
                else:
                    if len(lst1[0])==1:
                        a=i
                    else:
                        a1=remove_bug_from_amt_cols(lst1[0])
                        a2=remove_bug_from_amt_cols(lst1[1])
                        a=a1+' '+a2

            elif bool(re.match(r"^\d{1,3}\ \d{2,3}\ \d{2,3}$",i))==True:
                a=remove_bug_from_amt_cols(i)

            else:
                a=i
        else:
            a=remove_bug_from_amt_cols(i)
        if bool(re.match(r"\d{,2}[-./]\d{,2}[-./]\d{4}|\d{4}[/]\d{1,2}",i))==True:
            a="Date" #Replacing date format with Date string
        if re.match(r'\d{1,2}[/-][A-Za-z]+[/-]\d{4}',i):
            a="Date"
        
        return a
    def remove_bug_from_amt_cols(i):
        i=str(i)
        i=i.replace('x'," ")
        i=re.sub(r'[^0-9\.\,\-\;\:\/\ \=]','',i)

        i=i.strip().strip('.')
        i=i.rstrip('-')
        i=i.strip()
        i=re.sub(r'^-$','1',i)
        if i.count('.')>2:
            i=re.sub(r'\.\.+',r'',i) #Replacing the dots where more than 2 dots are present together to empty string
        if bool(re.match(r'^\d+(\,|\.)\d{3}(\=|\-)\d{2}$|^\d+(\=|\-)\d{2}$',i))==True:
            i=i.replace('=','.')
            i=i.replace('-','.')
        if bool(re.match(r"\d{,2}(\-|\.|\/)\d{,2}(\-|\.|\/)\d{4}|\d{4}(\/)\d{1,2}",i))==True:
            a="Date" #Replacing date format with Date string
        if re.match(r'\d{1,2}(\/|\-)[A-Za-z]+(\/|\-)\d{4}',i):
            a="Date"
        elif bool(re.match(r"^\d{1,2}\,\d{3}\.\d{2}\ (\;|\:)$",i))==True: 

            a=re.sub(r"^(\d{1,2})(\,)(\d{3})(\.)(\d{2})(\ )(\;|\:)$",r'\1\3\4\5',i) # Replacing a case where '; or :' is #present at the end with a space
        elif bool(re.match(r"^\d{1,5}\ (\;|\:)$",i))==True:

            a=re.sub(r"^(\d{1,5})\ (\;|\:)$",r'\1',i)# Replacing a case where '; or :' is #present at the end with a space

        elif bool(re.match(r'^\d{1,5}\.\d{2}\ \d{1}',i))==True:
            a=re.sub(r'^(\d{1,5}\.\d{2})(\ )(\d{1})',r'\1',i)
        elif bool(re.match(r'^\d{1,4}\. \.\d{2}$',i))==True:
            a=re.sub(r'^(\d{1,4})(\. )(\.)(\d{2})$',r'\1\3\4',i)
        elif bool(re.match(r"^(\- \d+)$",i))==True:
            a=i
        else:
            a=re.sub(r'\-\-+',r'',i) #Replacing double - strings (more than or equal to 2) to empty string
            a=re.sub(r'[\:\,\;\.\]]\B',r'',a) #Replace with empty strings based on word boundary
            a=re.sub(r'[\s\:\,\;](?=\d{1,2})(\b|\s)' , r'.',a) #Replacing with . based on the position of numerics
            a=re.sub(r'[\s\:\.\;](?=\d\d[^\b])' , r'' , a) #Replacing with dot
            a=re.sub(r'[^0-9\.\s\-]',r'',a)
        return a
    def patient_company(df):
        df.columns=[str(i).strip(".").lower().strip() for i in df.columns]
        pat_amt=['patient amount', 'patient amt', '- patient', 'y - putient amt', 'patient -amt', 'putient- amt', 'patient anit','pat amt',"customer pay"]

        cmp_amt=['company','company amount','conipany amt','gurupany mt','a crimpany amt', 'company amt a', 'gunipany amt', 'cunspany amt',
                  'conupany amt', '-pn conpany amt ', 'cumpany amt', 'company amount', 'company', 'company amount', 'tpa/corp amount','comp.amt',
                  'agency amount', 'company amt', 'company am',"company pay","payer amount"]

        org_col=df.columns
        cnt=0
        for i in df.columns:
            if i in pat_amt:
                cnt+=1
                df.rename(columns={i:'patient amount'},inplace = True)
            if i in cmp_amt:
                cnt+=1
                df.rename(columns={i:'company amount'}, inplace=True)
        if cnt==1:
            df.columns=org_col
        fl=0
        if 'company amount' in df.columns and 'patient amount' in df.columns:
            
            for el in [e for e in df.columns if e not in ['patient amount','company amount']]:
                if 'amount' in el:
                    fl=0
                    break
                else:
                    fl=1
            if fl==1:

                pagerule.append('Patient and company amount pattern found')         
                df['company amount'] = df['company amount'].apply(lambda x:remove_bug_from_amt_cols(x))
                df['patient amount'] = df['patient amount'].apply(lambda x:remove_bug_from_amt_cols(x))
                for i in df.index:
                    try:
                        df.at[i,'total amount'] = round(float(df.at[i,'patient amount']) + float(df.at[i,'company amount']),2)
                    except ValueError:
                        pass
                df.drop(columns=['company amount','patient amount'],inplace=True)
            else:
                df.drop(columns=['company amount','patient amount'],inplace=True)
        else:
            if cnt==2:
                df.columns=org_col
        
        return df,fl

    if f_header==0 or f_header==-1:
        
        try:
            merged_df=merged_df.iloc[:,:list(merged_df.columns).index("conf1")]
        except:
            pass
        
        pc_flag = 0
        if len(merged_df) > 0:
            col_names=[]
            for i in range(len(merged_df.columns)):
                col_names.append('column_'+str(i))
            merged_df.columns=col_names
            int_float_pattern='^\d{1,6}$|\d+\.\d+|\d+\,\d+\.\d+|\d+\,\d+|\d+\.\d+|\d{1}|\d{1,5}\.\d{1,2}'
            def preprocess(x):
                x=str(x).replace('*',' ').replace("₹","").strip()
                a=re.sub(r'^O$','0',x,flags=re.IGNORECASE)
                a=re.sub(r'^LO$','5',a,flags=re.IGNORECASE)
                a=re.sub(r'^CO$','6',a,flags=re.IGNORECASE)
                a=re.sub(r'^I$','1',a,flags=re.IGNORECASE)
                a=re.sub(r'^-$','1',a,flags=re.IGNORECASE)
                a=re.sub(r'^(I|I )No$','1 No',a,flags=re.IGNORECASE)
                a=re.sub(r'^(\|) No$','1 No',a,flags=re.IGNORECASE)
                a=re.sub(r'(\d+\ |\d+)(nos|numbers|no|number)',r'\1',a,flags=re.IGNORECASE)            
                a=re.sub(r'\*',"",a,flags=re.IGNORECASE)
                if re.match(r"^\d+(\:|\;|\.|\,)\d+(\,|\.)\d{2}$",a):
                    a=re.sub(r'[^0-9]','',a)
                    a=a[:-2]+"."+a[-2:]
                    
                return a

            for i in merged_df.columns:
                merged_df[i]=merged_df[i].apply(lambda x:preprocess(x))

            def demerge_columns(x,y):
                
                x=str(x).split()
                if len(x)==1:
                    first,second='',''
                else:
                    try:
                        first=x[0]
                        second=x[1]
                    except:
                        first,second='',''               
                return first,second
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
            p_col=particulars_finding(merged_df)
            temp_part=p_col
            
            colum=[i for i in merged_df.columns if i!=p_col]
            for r in colum:
                fir,sec=0,0
                for j in merged_df[r]:
                    temp_lst=[re.sub("[A-Za-z]","",j) for j in str(j).split()]
                    temp_lst=[j for j in temp_lst if j not in ["","00"]]
                    if len(temp_lst)==2:
                            if len(temp_lst[0].split('.')[0])<3 and len(temp_lst[0].split('.')[0])>0:
                                fir+=1
                            elif len(temp_lst[1].split('.')[0])==1:
                                sec+=1

                if fir>0.25*(len(merged_df)) and merged_df.columns.get_loc(r)>1 and fir>1:

                    merged_df.insert(list(merged_df.columns).index(r),'col1',[demerge_columns(x,1)[0] for x in merged_df[r].to_list()],allow_duplicates=True)
                    merged_df.insert(list(merged_df.columns).index(r),'col2',[demerge_columns(x,1)[1] for x in merged_df[r].to_list()],allow_duplicates=True)
                    merged_df.drop(columns=r,inplace=True)
                    pagerule.append('Headerless Column split into 2 due to the occurence of null values')
                    break
                elif sec>0.25*(len(merged_df)) and merged_df.columns.get_loc(r)>1 and sec>1:

                    merged_df.insert(list(merged_df.columns).index(r),'col1',[demerge_columns(x,1)[0] for x in merged_df[r].to_list()],allow_duplicates=True)
                    merged_df.insert(list(merged_df.columns).index(r),'col2',[demerge_columns(x,1)[1] for x in merged_df[r].to_list()],allow_duplicates=True)
                    merged_df.drop(columns=r,inplace=True)
                    pagerule.append('Headerless Column split into 2 due to the occurence of null values')
                    
                    break

            
            floatpattern=r'^(\d+(?:\.\d+)?)$'
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
                di_chem=len(merged_df[merged_df[p_col].str.contains("dial|hemo|pharma|medic")])
                if len(merged_df)>0:
                    pert=(len(merged_df)-len(set(merged_df[p_col].tolist())))/len(merged_df)
                    if pert>0.75 and len(merged_df)>5 and di_chem<3:
                        flag=True
                        pagerule.append('Particular column Skipped')
                return float_count,flag,alpha_count

            float_count,flag,alpha_count=particular_validation(merged_df,p_col)
            
            p_cols=[p_col]
            column_count=len(merged_df)
            while float_count>2 or list(merged_df.columns).index(p_col)>3 or flag==True or alpha_count>10:
                pagerule.append('Particular column Skipped')
                
                try:
                    merged_df_temp=merged_df.drop(p_cols,axis=1)
                    # try:
                    p_col=particulars_finding(merged_df_temp)
                    # except ValueError:
                    #     p_col=''
                    column_count=len(merged_df_temp.columns)
                    float_count,flag,alpha_count=particular_validation(merged_df,p_col)
                    p_cols.append(p_col)
                    
                except ValueError:
                    logging.info(f'Undesirable particular columns found')
                    
                    column_count=0
                    break
                
            if len(p_cols)>1:
                cd_cnt=0
                for j in [str(i).strip() for i in merged_df[p_col]]:
                    if len(j)<4:
                        cd_cnt+=1
                if cd_cnt>=len(merged_df)*0.75:
                    p_col=p_cols[0]
            if column_count==0 and len(merged_df)>0:
                p_col=temp_part
                pert=(len(merged_df)-len(set(merged_df[p_col].tolist())))/len(merged_df)
                if ((pert>0.75 and len(merged_df)>6) or merged_df[p_col].apply(lambda x:bool(re.match(floatpattern,str(x)))).sum()>4) and len(merged_df.columns)>1:
                    merged_df_temp=merged_df.drop(p_col,axis=1)
                    p_col=particulars_finding(merged_df_temp)
                    if len([re.match(floatpattern,ele) for ele in merged_df_temp[p_col].tolist() if ele!=None])>2 :
                        p_col=temp_part
                    
                                    
            merged_df.rename(columns={p_col:'particular'},inplace=True)
            p_col='particular'

            for i in merged_df.columns:
                merged_df[i]=merged_df[i].apply(lambda x: str(x).strip())

            merged_df_=pd.DataFrame()
            def date_convert(x):
                if re.match(r'^\d{1,2}(\/|\-)\d{1,2}(\/|\-)\d{4}$',x):
                    x=re.sub(r'^\d{1,2}(\/|\-)\d{1,2}(\/|\-)\d{4}$',r'',x)
                
                return x
            for i in merged_df.columns:
                merged_df_[i]=merged_df[i].apply(lambda x:date_convert(x))
                merged_df_[i]=merged_df_[i].apply(lambda x: re.findall(int_float_pattern,x))

            def index_finding(merged_df_,p_col):
                sum_of_cols={}
                for i in merged_df_.columns:
                    
                    k=([(i[0]) for i in merged_df_[i] if type(i)==list and len(i)>0])  
                    try:
                        sum_of_cols[i]=sum([float(re.sub(",",'.',i)) for i in k])
                    except ValueError:
                        sum_of_cols[i]=sum([float(re.sub(",",'',i)) for i in k])
                try:
                    if len(sum_of_cols)>1:
                        sum_of_cols.pop(p_col)
                except:
                    pass
                for i,j in sum_of_cols.items():
                    if j==np.max(list((sum_of_cols.values()))):
                    
                        highest_amt=i
                        break
                amounts_final_index=list(merged_df_.columns).index(highest_amt)
                
                return amounts_final_index
        
            
            amounts_final_index=index_finding(merged_df_,p_col)
            amount_col = merged_df_.columns[amounts_final_index]
            def amount_validation(merged_df_,amount_col):
                amt_lst=merged_df_[amount_col].to_list()
                amt_lst=[l for l in amt_lst if l!=[]]
                empty_dict={}
                for co in merged_df_.columns:
                    len_l=merged_df_[co].to_list()
                    empty_dict[co]=len([l for l in len_l if l!=[]])
                del empty_dict[p_col]
                cnt,high_cnt,dat_cnt,single_cnt,date_count=0,0,0,0,0
                for i in merged_df.index:
                    if '9993' in merged_df.at[i,amount_col] or "3004" in merged_df.at[i,amount_col] or "9018" in merged_df.at[i,amount_col]:
                        cnt+=1
                        pagerule.append(f'HSN Code in amount column {merged_df.at[i,amount_col]}')
                    elif re.match(r"\d{7,}",re.sub(r'[^\d\.\,\-\;\:\ \=]','',merged_df.at[i,amount_col])):
                        high_cnt+=1
                    elif re.match(r"\d{1,2}(/|-)\d{1,2}(/|-)\d{2,4}",merged_df.at[i,amount_col]):
                        dat_cnt+=1
                    elif str(merged_df.at[i,amount_col]).endswith(tuple([str(i) for i in list(range(2010,2031))])):
                        date_count+=1
                    elif re.match(r'^[1-9]{1}$|^(\-)[1-9]{1}$',str(merged_df.at[i,amount_col]).strip()):
                        single_cnt+=1
                    try:
                        temp_val=re.sub(r'[^\d\.\-\;\:\ \=]','',merged_df.at[i,amount_col]).strip('.').strip()
                        
                        if float(temp_val)>=200000:
                            high_cnt+=1
                    except (ValueError,TypeError) as e:
                        pass
                
                return amt_lst,cnt,high_cnt,dat_cnt,single_cnt,empty_dict,date_count
            temp_col=amount_col
            amt_lst,cnt,high_cnt,dat_cnt,single_cnt,empty_dict,date_count=amount_validation(merged_df_,amount_col)
            while cnt>0 or high_cnt>2 or dat_cnt>2 or single_cnt>=len(merged_df)*0.5 or list(merged_df.columns).index(p_col)>=list(merged_df.columns).index(amount_col) or date_count>0.4*len(merged_df) or (len(amt_lst)<0.35*len(merged_df) and len(amt_lst)<max(empty_dict.values())):
                try:
                    pagerule.append('Amount column skipped')
                    merged_df_.drop(merged_df_.columns[amounts_final_index],axis=1,inplace=True)   
                    amounts_final_index=index_finding(merged_df_,p_col)
                    amount_col = merged_df_.columns[amounts_final_index]
                    amt_lst,cnt,high_cnt,dat_cnt,single_cnt,empty_dict,date_count=amount_validation(merged_df_,amount_col)
                except Exception as e:
                    logging.info('Undesirable amount column patterns')
                    
                    amount_col=temp_col
                    break
                    
            def quantity_discount_identifier(w1,amount_col,p_col ):
                w=w1.copy()
                
                w.replace('',np.nan,inplace=True)
                w.replace('nan',np.nan,inplace=True)
                w.dropna(thresh=3,inplace=True)
                w.fillna('',inplace=True)
                w.drop(columns=gst_function(w),inplace=True)
                ind=[]
                for i in w.columns:

                    
                    item_lst=w[i].tolist()
                    item_lst=[item for item in item_lst if item!='']
                    if len(item_lst)>=w[i].count()/2:
                        int_count=0
                        float_list=[]
                        for k in item_lst:
                            try:
                                k=float(k)
                                float_list.append(k)
                            except:
                                pass

                        res = all(i < j for i, j in zip(float_list, float_list[1:]))

                        try:
                            if float_list[-1]==float_list[-2]+1==float_list[-3]+2:
                                res=True
                                pagerule.append('Serial Number column ignored in quantity detection')

                        except:
                            pass
                        try:
                            if len(list(set(float_list)))==1 and float_list[0] in [6,2.5,9,5]:
                                res=True
                        except:
                            pass

                        if len(float_list)==0:
                            res=True
                            
                        if res==False:

                            for j in item_lst:
                                j=j.rstrip('.').rstrip('-').rstrip(',')
                                j=re.sub(r'[^0-9\.\,\-\;\:\ ]','',j)
                                try:
                                    j=abs(float(j))
                                    if j>0 and j<11:
                                        int_count+=1
                                except:
                                    pass
                            if int_count >= len(item_lst)/2:
                                if i not in [amount_col,p_col,'total','amount','discount']:
                                    w1.rename(columns={i:'quantity'},inplace=True)    
                                    lst=list(w1.columns)   
                                    ind.append(lst.index('quantity'))
                                    pagerule.append('Quantity identified by default rule')
                                    break
                if "quantity" in w1.columns and amount_col in w1.columns:
                    if list(w1.columns).index('quantity')>list(w1.columns).index(amount_col):
                        w_=w1.drop(columns=amount_col)

                        for i in w_.columns:
                            w_[i]=w_[i].apply(lambda x:date_convert(x))
                            w_[i]=w_[i].apply(lambda x: re.findall(int_float_pattern,x))
                        w_1=w_.drop(columns=w_.columns[:list(w_.columns).index('quantity')+1])
                        if len(w_1.columns)>1:
                            amounts_final_index=index_finding(w_1,p_col)
                            amount_col = list(w_1.columns)[amounts_final_index]

                return w1,ind,amount_col
            
                
            def quantity_price_extraction(df,p_col):
                def alpha_check(x):
                    
                    x=str(x).strip()
                    if x[0].isalpha() or " " in x or (not x[0].isnumeric() and not x[0].isdigit()):
                        return True
                    else:
                        return False
                df1=df.drop(columns=[p_col])
                df1.replace(' ',np.nan,inplace = True)   
                df1.replace('',np.nan,inplace=True)
                df1.replace('nan',np.nan,inplace=True)
                df1.drop(columns=gst_function(df1),inplace=True)
                rows_to_drop=[]
                for row in df1.index:
                    list1=[alpha_check(cell) for cell in df1.loc[row,:].values.tolist()]
                    if len(set(list1))==1 and list1[0]==True:
                        rows_to_drop.append(row)

                df1.drop(rows_to_drop,inplace=True)
                try:
                    for ite in df1.iloc[df1.index[0]].values.tolist():
                        if str(ite).startswith('empty') or str(ite).startswith('Empty'):
                            df1=df1.drop(df1.index[0])
                            break
                except IndexError:
                    pass
                
                def rounder(x):
                    try:
                        x=str(x)
                        if x=="Date":
                            x="///"
                        x=re.sub(r'[^0-9\.\,\-\;\:\/\ \Date]','',x)
                        x=round(float(x),2)
                    except ValueError:
                        x=x
                    return x
                for i in df1.columns:

                    try:
                        
                        df1[i]=df1[i].apply(lambda x:str(x))   
                        df1[i]=df1[i].apply(lambda x:x.strip())   
                        df1[i]=df1[i].apply(lambda x:merge_separator(x))
                        df1[i]=df1[i].apply(lambda x:rounder(x))

                    except:
                        continue
                for i in df1.columns:
                    dcount,space_count=0,0
                    for dc in df1[i].tolist():
                        if dc=="///":
                            dcount+=1
                        if " " in str(dc).strip() or str(dc).strip()=="":
                            space_count+=1
                        
                    if dcount>=0.5*len(df1) or space_count>=0.9*len(df1):
                        df1.drop(columns=[i],inplace=True)

                row_rem=[]
                

                for row in list(df1.index):
                    try:
                        for val in list(df1.loc[row,:].values):
                            if str(val).count('.')>2 or len(str(val).split())>1:
                                row_rem.append(row)
                    except IndexError:
                        pass
                df1.drop(row_rem,inplace=True)
                df1.replace(' ',np.nan,inplace = True)   
                df1.replace('',np.nan,inplace=True)
                if len(df1)>3:
                    df1.dropna(inplace = True,thresh=3,axis=1)
                    
                df1.dropna(inplace = True,axis=0,thresh=df1.shape[1]-1)
                for i in df1.columns:
                    try:
                        df1[i]=df1[i].astype('float')   
                    except:
                        continue
                numeric = df1.select_dtypes(exclude = 'object')
                
                df1.fillna(0,inplace=True)
                comb = list(combinations(numeric.columns,2))
                iden=0
                if len(df1)>0:
                    for j in numeric.columns:
                    
                        for i in range(len(comb)):
                            if j!=comb[i][0] and j!=comb[i][1]:
                                try:

                                    df1['mul']=df1.loc[:,comb[i][0]]*df1.loc[:,comb[i][1]]   
                                    mat=0
                                    
                                    for k in df1.index:
                                        if round(abs(df1.at[k,'mul']))==round(abs(df1.at[k,j])) and (df1.at[k,'mul'] not in [0,9993,999311]):
                                            mat+=1
                                    if mat>len(df1)*0.75 and iden==0:
                                        iden+=1

                                        df1.loc[:,comb[i][0]]=df1.loc[:,comb[i][0]].abs()
                                        df1.loc[:,comb[i][1]]=df1.loc[:,comb[i][1]].abs()
                                        df1.loc[:,j]=df1.loc[:,j].abs()
                                        df1['comp']=df1[comb[i][0]]>df1[comb[i][1]]  
                                        if len(df1[df1['comp']==True]) >= len(df1)*0.75:

                                            df.rename(columns={comb[i][0]:'rate',comb[i][1]:'quantity',j:'total'},inplace=True)
                                            numeric.rename(columns={comb[i][0]:'rate',
                                                                    comb[i][1]:'quantity',j:'total'},inplace=True)
                                        else:

                                            df.rename(columns={comb[i][1]:'rate',comb[i][0]:'quantity',j:'total'},inplace=True)
                                            numeric.rename(columns={comb[i][1]:'rate',comb[i][0]:'quantity',j:'total'},inplace=True)
                                        pagerule.append('Unitprice, quantity, total identified through default route')

                                        ch=numeric[numeric['quantity']!=1]
                                        ch['rate']=ch['rate'].abs()
                                        ch['total']=ch['total'].abs()
                                        ch['com']=ch['rate']>ch['total']
                                        if len(ch)>0:
                                            if len(ch[ch['com']==True]) >= len(ch)*0.50:

                                                df.rename(columns={'total':'Rate','rate':'Total'},inplace=True)
                                            df.columns= df.columns.str.strip().str.lower()
                                        break
                                    
                                
                                except Exception as e:
                                    continue
                def discount(merged_df):
                    df=merged_df.copy()
                    df.drop(columns=gst_function(df),inplace=True)
                    try:
                        df.drop(columns=p_col,inplace=True)
                    except KeyError:
                        pass
                    try:
                        for ite in df.iloc[df.index[0]].values.tolist():
                            if str(ite).startswith('empty') or str(ite).startswith('Empty'):
                                df=df.drop(df.index[0])
                                break
                    except IndexError:
                        pass
                    def replace_zeros(i):
                        i=re.sub(r'^O$','0',i)
                        return i
                    for i in df.columns:

                        df[i]=df[i].apply(lambda x:str(x).strip())
                        df[i]=df[i].apply(lambda x:replace_zeros(x))
                        df[i]=df[i].apply(lambda x:merge_separator(x))
                    for i in df.columns:
                        dcount,space_count=0,0
                        if i not in ['quantity','rate','total']:
                            for dc in df[i].tolist():
                                if dc=="Date":
                                    dcount+=1
                                if " " in str(dc).strip() or str(dc).strip()=="":
                                    space_count+=1


                            if dcount>=0.5*len(df) or space_count>=0.9*len(df):
                                df.drop(columns=[i],inplace=True)
                    df.replace(' ',np.nan,inplace = True)   
                    df.replace('',np.nan,inplace=True)
                    if len(df)>3:
                        df[df.columns[~df.columns.isin(['quantity','rate','total'])]].dropna(inplace = True,thresh=3,axis=1)
                    
                    df.dropna(inplace = True,axis=0,thresh=df.shape[1]-1)
                    for co in df.columns:
                        try:
                            df[co]=df[co].astype('float')
                        except:
                            df[co]=df[co]
                    df_s=df.copy()
                    df.fillna(0,inplace=True)
                    if len(df)>0:
                        
                        if all(x in list(df.columns) for x in ['rate', 'quantity']) == True:

                            df=df.drop(columns=['quantity','rate'])
                            numeric = df.select_dtypes(exclude = 'object') 
                            comb = list(combinations(numeric.columns,2))
                            comb=[item for item in comb if 'total' not in item]
                            for col in comb:
                                if all(co in list(numeric.columns) for co in ['total'])==True:
                                    df['diff']=df['total']-df[col[0]]
                                    mat=0
                                    for k in df.index:
                                        try:
                                            if abs(round(df.at[k,'diff']))==abs(round(df.at[k,col[1]])) and round(df.at[k,col[1]])!=0:
                                                mat+=1
                                        except TypeError:
                                            pass
                                else:
                                    mat=0
                                if mat>(len(df)*0.75):
                                    df[col[0]]=df[col[0]].abs()
                                    df[col[1]]=df[col[1]].abs()
                                    df['comp']=df[col[0]]>df[col[1]]  

                                    if len(df[df['comp']==True]) > len(df)*0.75:

                                        merged_df.rename(columns={col[0]:'amount',col[1]:'discount'},inplace=True)
                                    else:

                                        merged_df.rename(columns={col[1]:'amount',col[0]:'discount'},inplace=True)
                                    pagerule.append('Unitprice, quantity, total identified through default route')
                                    
                                    break
                
                        else:
                            numeric = df.select_dtypes(exclude = 'object') 
                            comb = list(combinations(numeric.columns,2))
                            cn=0
                            for j in numeric.columns:
                                for i in range(len(comb)):
                                    try:
                                        if cn==0:
                                            if j!=comb[i][0] and j!=comb[i][1]:
                                                df['diff']=df[comb[i][0]]-df[comb[i][1]]
                                                matc=0
                                                for k in df.index:
                                                    if (df.at[k,comb[i][0]]==1 and df.at[k,comb[i][1]]==0) or (df.at[k,comb[i][1]]==1 and df.at[k,comb[i][0]]==0):
                                                        val=""
                                                    else:
                                                        try:
                                                            if abs(round(df.at[k,'diff']))==abs(round(df.at[k,j])) and abs(round(df.at[k,j]))!=0:
                                                                matc+=1
                                                        except TypeError:
                                                            pass
                                                if matc>(len(df)*0.75):
                                                    cn+=1
                                                    df[comb[i][0]]=df[comb[i][0]].abs()
                                                    df[comb[i][1]]=df[comb[i][1]].abs()
                                                    df['comp']=df[comb[i][0]]>df[comb[i][1]]
                                                    if len(df[df['comp']==True]) >= len(df)*0.75:
                                                        merged_df.rename(columns={comb[i][0]:'Total'},inplace=True)
                                                        numeric.rename(columns={comb[i][0]:'Total'},inplace=True)
                                                        df['comp_d']=df[comb[i][1]]>df[j]
                                                        if len(df[df['comp_d']==True]) >= len(df)*0.75:
                                                            pagerule.append('Discount and ADA identified through default rule')
                                                            
                                                            merged_df.rename(columns={comb[i][1]:'Amount',j:'Discount'},inplace=True)
                                                            numeric.rename(columns={comb[i][1]:'Amount',j:'Discount'},inplace=True)                                                        
                                                        else:
                                                            merged_df.rename(columns={comb[i][1]:'Discount',j:'Amount'},inplace=True)
                                                            numeric.rename(columns={comb[i][1]:'Discount',j:'Amount'},inplace=True)


                                                    else:
                                                        merged_df.rename(columns={comb[i][1]:'Total'},inplace=True)
                                                        numeric.rename(columns={comb[i][1]:'Total'},inplace=True)
                                                        df['comp_d']=df[comb[i][0]]>df[j]
                                                        if len(df[df['comp_d']==True]) >= len(df)*0.75:
                                                            merged_df.rename(columns={comb[i][0]:'Amount',j:'Discount'},inplace=True)
                                                            numeric.rename(columns={comb[i][0]:'Amount',j:'Discount'},inplace=True)                                                        
                                                        else:
                                                            merged_df.rename(columns={comb[i][0]:'Discount',j:'Amount'},inplace=True)
                                                            numeric.rename(columns={comb[i][0]:'Discount',j:'Amount'},inplace=True)
                                                    merged_df.columns= merged_df.columns.str.strip().str.lower()
                                                    numeric.columns= numeric.columns.str.strip().str.lower()
                                                    numeric['com']=numeric['amount']>numeric['total']

                                                    if len(numeric[numeric['com']==True]) > 0:

                                                        merged_df.rename(columns={'amount':'Total','total':'Amount'},inplace=True)
                                                    merged_df.columns= merged_df.columns.str.strip().str.lower()
                                                    break
                                                    


                                    except:
                                        pass
                        
                        try:
                            df.drop(columns=['diff'],inplace=True)
                        except:
                            pass
                    if all(x in list(merged_df.columns) for x in ['amount','discount'])==False:
                        disflag=0
                        if len(df)>2:
                            for dis in df_s.columns:
                                if (df_s[dis]==0).replace(False,np.nan).count()>0.95*len(df):
                                    disflag+=1
                            if disflag<3:

                                for dis in df_s.columns:
                                    if (df_s[dis]==0).replace(False,np.nan).count()>0.95*len(df):

                                        if dis not in ['amount','rate','quantity','total','particular']:
                                            merged_df.rename(columns={dis:'discount'},inplace=True)
                                            break
                                            

                    if all(x in list(merged_df.columns) for x in ['rate', 'quantity','total','amount','discount']):
                        flg=2
                    elif all(x in list(merged_df.columns) for x in ['rate', 'quantity','total','discount']):
                        flg=1
                    elif all(x in list(merged_df.columns) for x in ['rate', 'quantity','total']):
                        flg=0.75
                    elif all(x in list(merged_df.columns) for x in ['total','amount','discount']):
                        flg=0.5
                    elif all(x in list(merged_df.columns) for x in ['discount']):
                        flg=0.25
                    else:
                        flg=0

                    
                    del df
                    return merged_df,flg
                df,flg=discount(df)
                del df1
                return df,flg
            merged_df,flg=quantity_price_extraction(merged_df,p_col)
            if all(x in list(merged_df.columns) for x in ['rate', 'quantity']) == True:
                
                if all(x in list(merged_df.columns) for x in ['particular'])==False:
                    part=merged_df.columns
                    for a in ['rate', 'quantity','total','amount','discount']:
                        try:
                            part.drop(columns=a,inplace=True)
                        except:
                            pass
                    
                    p_col=particulars_finding(part)
                    del part
                    merged_df.rename(columns={p_col:'particular'},inplace=True)
                    
                if flg==2:
                    logging.info(f'All 6 columns identified in Headerless')
                    
                    key_val={'Particular':'particular','Before_discount_amount':'total','Quantity':'quantity','Unit_price':'rate',
                            'After_discount_amount':'amount','Discount':'discount'}
                    columns_to_be_extracted=['particular','total','quantity','rate','amount','discount']
                    location_of={'particular':0+1,'total':1+1,'quantity':2+1,'rate':3+1,'amount':4+1,'discount':5+1}
                    cols_ext=['particular','total','quantity','rate','amount','discount']
                    merged_df_=merged_df[cols_ext]
                    merged_df=merged_df_
                elif flg==1:
                    
                    key_val={'Particular':'particular','Before_discount_amount':'total','Quantity':'quantity','Unit_price':'rate',
                            'Discount':'discount'}
                    columns_to_be_extracted=['particular','total','quantity','rate','discount']
                    logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                    location_of={'particular':0+1,'total':1+1,'quantity':2+1,'rate':3+1,'discount':4+1}
                    cols_ext=['particular','total','quantity','rate','discount']
                    merged_df_=merged_df[cols_ext]
                    merged_df=merged_df_
                elif flg==0.75:
                    key_val={'Particular':'particular','Before_discount_amount':'total','Quantity':'quantity','Unit_price':'rate'}
                    columns_to_be_extracted=['particular','total','quantity','rate']
                    logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                    
                    location_of={'particular':0+1,'total':1+1,'quantity':2+1,'rate':3+1}
                    cols_ext=['particular','total','quantity','rate']
                    merged_df_=merged_df[cols_ext]
                    merged_df=merged_df_
                
            else:
                merged_df,ind,amount_col=quantity_discount_identifier(merged_df,amount_col,p_col)
                
                    
                if len(ind)==0:
                    
                    if flg==0.5:
                        cols_ext=['particular','total','amount','discount']
                        columns_to_be_extracted=['particular','total','amount','discount']
                        logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                        
                        merged_df_=merged_df[cols_ext]   
                        merged_df_.columns=columns_to_be_extracted 
                        key_val={'Particular':'particular','Before_discount_amount':'total',
                                'Discount':'discount','After_discount_amount':'amount'}
                        
                        location_of={'particular':0+1,'total':1+1,'amount':2+1,'discount':3+1}
                        
                    elif flg==0.25:
                        if all(x in list(merged_df.columns) for x in [amount_col]) == False:
                            tmp=merged_df.drop(columns=['discount'])
                            for i in tmp.columns:
                                tmp[i]=tmp[i].apply(lambda x: re.findall(int_float_pattern,x))   
                            
                            amounts_final_index=index_finding(tmp,'particular')
                            
                            amount_col = tmp.columns[amounts_final_index]
                        cols_ext=['particular',amount_col,'discount']
                        columns_to_be_extracted=['particular','total','discount']
                        logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                        
                        merged_df_=merged_df[cols_ext]   
                        merged_df_.columns=columns_to_be_extracted 
                        key_val={'Particular':'particular','Before_discount_amount':'total',
                                'Discount':'discount'}
                                    
                        location_of={'particular':0+1,'total':1+1,'discount':2+1}
                    
                    
                    elif flg==0:
                        cols_ext=['particular',amount_col]
                        columns_to_be_extracted=['particular','total']
                        merged_df_=merged_df[cols_ext]   
                        merged_df_.columns=columns_to_be_extracted 
                        logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                        
                        key_val={'Particular':'particular','Before_discount_amount':'total'}
                                    
                        location_of={'particular':0+1,'total':1+1}

                    merged_df=merged_df_   
                else:
                    if all(x in list(merged_df.columns) for x in ['discount'])==False:
                        flg=0
                        
                    if flg==0.5:
                        cols_ext=['particular','total','amount','discount','quantity']
                        columns_to_be_extracted=['particular','total','amount','discount','quantity']
                        logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                        
                        merged_df_=merged_df[cols_ext]
                        
                        merged_df_.columns=columns_to_be_extracted 
                        key_val={'Particular':'particular','Before_discount_amount':'total',
                                'Discount':'discount','After_discount_amount':'amount','Quantity':'quantity'}
                        
                        location_of={'particular':0+1,'total':1+1,'amount':2+1,'discount':3+1,'quantity':4+1}
                        
                    elif flg==0.25:
                        if all(x in list(merged_df.columns) for x in [amount_col]) == False:
                            tmp=merged_df.drop(columns=['discount','quantity'])
                            for i in tmp.columns:
                                tmp[i]=tmp[i].apply(lambda x: re.findall(int_float_pattern,x))
                                
                            amounts_final_index=index_finding(tmp,'particular')
                            amount_col = tmp.columns[amounts_final_index]
                        
                        cols_ext=['particular',amount_col,'discount','quantity']
                        columns_to_be_extracted=['particular','total','discount','quantity']
                        logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                        
                        merged_df_=merged_df[cols_ext]   
                        merged_df_.columns=columns_to_be_extracted 
                        key_val={'Particular':'particular','Before_discount_amount':'total',
                                'Discount':'discount','Quantity':'quantity'}
                                    
                        location_of={'particular':0+1,'total':1+1,'discount':2+1,'quantity':3+1}
                    
                    elif flg==0:
                        

                        cols_ext=[p_col,amount_col,'quantity']
                        merged_df_=merged_df[cols_ext]   
                        merged_df_.columns=['particular','total','quantity']  
                        

                        key_val={'Particular':'particular','Before_discount_amount':'total','Quantity':'quantity'}
                        columns_to_be_extracted=['particular','total','quantity']
                        logging.info('columns identified : {}'.format(str(columns_to_be_extracted)))
                        
                        location_of={'particular':0+1,'total':1+1,'quantity':2+1}
                    merged_df=merged_df_ 
            del merged_df_
            gc.collect()
            skip_rows=[]
            for j in merged_df.index:
                
                l=merged_df.loc[j,:]
                
                for i in l:
                    if str(i).lower().startswith('empty') or str(i).startswith('mrs.') or str(i).startswith("mr") or str(i).startswith("miss."):
                        skip_rows.append(j)
                        break


            if len(skip_rows)>0:
                merged_df.drop(skip_rows[0],inplace=True)
            
            if 'quantity' in merged_df.columns:
                for li in merged_df.index:
                    if len(str(merged_df.at[li,'quantity']).strip().split('.')[0])>3:
                        merged_df.at[li,'quantity']=""
            for li in merged_df.index:
                if str(merged_df.at[li,"particular"])=="nan":
                    lst=merged_df_cp.loc[li,:].values.tolist()
                    counter=[(len(re.findall(r'[A-Za-z- ]',ele))) for ele in lst]
                    cter=counter.index(max(counter))
                    merged_df.at[li,'particular']=lst[cter]
        
        else:
            merged_df=merged_df
            key_val={}
            columns_to_be_extracted=''
            location_of=''
            return merged_df,key_val,columns_to_be_extracted,location_of,pagerule,pc_flag
    else:

        merged_df=merged_df
        merged_df,pc_flag=patient_company(merged_df)
        cols=gst_function(merged_df)
        merged_df.drop(columns=cols,inplace=True)
        key_val=''
        columns_to_be_extracted=''
        location_of=''

    for ind in merged_df.index:
        merged_df.at[ind,'bb_rowwise']=merged_df_copy.at[ind,'bb_rowwise']
        merged_df.at[ind,'con_rowwise']=merged_df_copy.at[ind,'con_rowwise']
    merged_df=merged_df.reset_index(drop=True)
    return merged_df,key_val,columns_to_be_extracted,location_of,pagerule,pc_flag


#####################################################################
 # Copyright(C), 2022 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################