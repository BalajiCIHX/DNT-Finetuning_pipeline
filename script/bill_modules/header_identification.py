
from bill_modules.utils import remove_duplicate_column_names,replace_negatives
import pandas as pd
import gc
from itertools import chain
import logging

def header_identification(dict_values,df,head,pagerule):
    ambiguous_items=["total","details","medicine","net","package","balance"]
    df=df.reset_index(drop=True)
    df=df.rename(columns=lambda x: str(x).strip().lower())
    df=df.rename(columns=lambda x: x.replace('sub total',''))
    for i in df.columns:
        if i=='net' and list(df.columns).index(i)!=len(df.columns)-1:
            pagerule.append('Ambiguous column -(net) removed')
            df.rename(columns={i:""},inplace=True)
    dict_values=[i for i in dict_values if i not in ambiguous_items]
        
    column_names=[ix.split() for ix in set([str(j).strip().lower() for j in list(df.columns.values)])]
    individual_elements=[ix for ix in set([str(j).strip().lower() for j in list(df.columns.values)])]
    column_names.append(individual_elements)
    column_names=list(set(list(chain.from_iterable(column_names))))

    if len(set(column_names).intersection(set(dict_values)))>1:
        f_header=1
        logging.info('Headers identified from table ')
        pagerule.append(f"Headers are identified from the table {column_names}")

    else:
        f_header=0
        if len(df)<3:
            rows_to_consider=len(df)
        else:
            rows_to_consider=3
        if df.shape[0]>1:
            for location in range(0,rows_to_consider):

                ls=[i.split(" ") for i in set([str(j).lower().strip('.').strip(':').strip() for j in list(df.loc[location,:].values)])]
                pls=[l for im in ls for l in im]
                if len(set(pls).intersection(set(dict_values)))>1:
                    f_header=1
                    logging.info('Header found in first 3 lines')
                    pagerule.append(f"Headers are identified from first 3 lines {set(pls).intersection(set(dict_values))}")
                    columns=df.loc[df.index[location], :].values.tolist()
                    if f_header==1 and len(df)!=location+1:
                        df.columns=columns
                        df=df.loc[df.index[location+1]:,:]
                    break
                else:
                    location=0
            
    try:
        df=df.rename(columns=lambda x: x.strip('.').strip(':').strip().lower())
    except AttributeError:
        pass
    
    df.columns=remove_duplicate_column_names(df.columns)
    for coll in df.columns:
        try:
            df[coll]=df[coll].apply(lambda x:replace_negatives(x,pagerule))
        except (ValueError,KeyError):
            pass
    df.rename(columns={list(df.columns)[-1]:"con_rowwise"},inplace=True)

    df.rename(columns={list(df.columns)[-2]:"bb_rowwise"},inplace=True)
    if f_header == 1:
        head = df.columns
        
    if f_header == 0 or f_header == -1:
        if len(df.columns) == len(head):
            f_header = 1
            df=df.append(pd.Series(),ignore_index=True)
            df=df.shift(periods=1,axis=0)
            df.loc[df.index[0]]=[str(ele).split('_')[0] for ele in df.columns]
            df.columns=head
            pagerule.append('Headers matched taking header from previous pages')
            logging.info("Headers matched taking header from previous pages")
            if df.loc[df.index[0],"bb_rowwise"]=="bb":
                df.drop(df.index[0],inplace=True)
                df=df.reset_index(drop=True)
        else:
            df=df.append(pd.Series(),ignore_index=True)
            df=df.shift(periods=1,axis=0)
            df.loc[df.index[0]]=[str(ele).split('_')[0] for ele in df.columns]
            if df.loc[df.index[0],"bb_rowwise"]=="bb":
                df.drop(df.index[0],inplace=True)
                df=df.reset_index(drop=True)
    gc.collect()

    if f_header == 0:
        logging.info('Headers not identified/present')
        pagerule.append('Headers not identified/present')
    return df,f_header,head,pagerule


#####################################################################
# Copyright(C), 2023 IHX Private Limited. All Rights Reserved
# Unauthorized copying of this file, via any medium is 
# strictly prohibited 
#
# Proprietary and confidential
# email: care@ihx.in
#####################################################################