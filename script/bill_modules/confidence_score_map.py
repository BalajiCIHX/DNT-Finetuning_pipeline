def conf_map(df):
    from bill_modules.utils import remove_duplicate_column_names

    if len(df)>0:
        df.columns=remove_duplicate_column_names(df.columns)
        collist=[col for col in df.columns if str(col).startswith("conf_")]
        conf_list=[]
        for idx in df.index:
            conf_scr = []
            for item in df.loc[idx,collist].values:
                try:
                    if float(item)>1:
                        conf_scr.append(float(item)/100)
                        conf_list.append(float(item)/100)
                    else:
                        conf_scr.append(float(item))
                        conf_list.append(float(item))
                except ValueError:
                    pass
            if len(conf_scr)>1:
                df.at[idx,'conf_row']=round(sum(conf_scr)/len(conf_scr),2)
            else:
                df.at[idx,'conf_row']=0
                
        if len(conf_list)>0:
            df['conf_avg']=round(sum(conf_list)/len(conf_list),2)
        else:
            df['conf_avg']=0
        df['con_rowwise']="{'i1':"+df["conf_row"].astype(str)+",'i2':"+df["conf_avg"].astype(str)+"}"
        df.drop(columns=["conf_row","conf_avg"],inplace=True)
    return df

#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium s 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################