def sub_heading_clearing_module(df,ttc,hosp_id,pagerule):
    import pandas as pd
    import numpy as np
    import gc
    import logging
    def is_unique(s):
        a = s.to_numpy()
        return (a[0] == a).all()

    for i in df.columns:
        df[i]=df[i].apply(lambda x:str(x).lower())
    
    df = df.replace('',np.nan)
    df = df.dropna(thresh=1)
    df = df.fillna('')

    if ttc=="Detail" or len(df)>15:
        df.replace("VNA",np.nan,inplace=True)
        df.replace("vna",np.nan,inplace=True)
        df.replace('nan',np.nan,inplace=True)
        df.replace('',np.nan,inplace=True)
        df.replace(' ',np.nan,inplace=True)
        df["sub_heading"]=""
        df["row_lst"]=df[['Particular', 'Unit_price', 'Quantity',
           'Before_discount_amount', 'Discount', 'After_discount_amount']].isnull().sum(axis=1).tolist()
        if len(df[df['row_lst']>=5]) > 0:
            pagerule.append("Rows with all null values found")

        df=df[df["row_lst"]<5]
        df.drop(columns=["row_lst"],inplace=True)
        row_cleaning_df =df
        t = row_cleaning_df.shape[0]
        flag = 0
        row_cleaning_df=row_cleaning_df[~row_cleaning_df['Particular'].isna()]

        if row_cleaning_df["Unit_price"].isna().sum() < t or row_cleaning_df["Quantity"].isna().sum() < t :
            flag = 1
        else:
            flag= 0
        if flag == 1:
            not_nan_in_up = row_cleaning_df[row_cleaning_df["Unit_price"].isna() == False].shape[0]
            nan_in_up = row_cleaning_df[row_cleaning_df["Unit_price"].isna() == True].shape[0]
            not_nan_in_qty = row_cleaning_df[row_cleaning_df["Quantity"].isna() == False].shape[0]
            nan_in_qty = row_cleaning_df[row_cleaning_df["Quantity"].isna() == True].shape[0]

            fill_na_pattern = "****"

            row_cleaning_df.fillna(fill_na_pattern,inplace=True)
            if hosp_id in ['514974']:
                for i in row_cleaning_df.index:
                    if row_cleaning_df.loc[i,"Quantity"] == fill_na_pattern:
                        row_cleaning_df.loc[i,"sub_heading"] = "yes"
            if not_nan_in_up  >= nan_in_up or not_nan_in_qty  >= nan_in_qty :
                '''filled values in up and qty > null values'''    

                for i in row_cleaning_df.index:
                    if is_unique(row_cleaning_df['Unit_price']) == False:
                        if row_cleaning_df.loc[i,"Quantity"] == fill_na_pattern and row_cleaning_df.loc[i,"Unit_price"] == fill_na_pattern :
                            row_cleaning_df.loc[i,"sub_heading"] = "yes" 
                            logging.info('Sub headers identified')
                            pagerule.append(f"Sub headers identified at {i}")
                            
                row_cleaning_df_ = row_cleaning_df.replace(fill_na_pattern,np.nan)
                row_cleaning_df_ = row_cleaning_df_[row_cleaning_df_.sub_heading != "yes"].drop(["sub_heading"],axis=1)
            
            else:
                
                if hosp_id in ['515787']:
                    for i in row_cleaning_df.index:
                        if row_cleaning_df.loc[i,"Quantity"] == fill_na_pattern and row_cleaning_df.loc[i,"Unit_price"] == fill_na_pattern :
                                row_cleaning_df.loc[i,"sub_heading"] = "yes" 
                                print('Sub headers identified')
                                pagerule.append(f"Sub headers identified at {i}")

                    row_cleaning_df_ = row_cleaning_df.replace(fill_na_pattern,np.nan)
                    row_cleaning_df_ = row_cleaning_df_[row_cleaning_df_.sub_heading != "yes"].drop(["sub_heading"],axis=1)


                else:
                    row_cleaning_df_ = row_cleaning_df.replace(fill_na_pattern,np.nan)

        else :        
            row_cleaning_df_ = row_cleaning_df

        del df,row_cleaning_df
        gc.collect()
        return row_cleaning_df_,pagerule
    if ttc=="Summary":
        logging.info('Not calling Subheading as the table type is summary')
        return df,pagerule
