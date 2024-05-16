def output_score(df):
    import pandas as pd
    import numpy as np
    import math
    import re
    key_columns = ["Particular","Unit_price","Quantity","Before_discount_amount","Discount","After_discount_amount"]
    df=df.loc[:,["Particular","Unit_price","Quantity","Before_discount_amount","Discount","After_discount_amount"]].copy()
    
    # create a copy of the input dataframe, which will remain untouched
    original_df=df.copy()
    df = df.reset_index(drop=True)
    
    # create empty output score dataframe
    #output_score_df=pd.DataFrame(index=range(df.shape[0]), columns=range(df.shape[1]))
    output_score_df=pd.DataFrame(index=df.index, columns=range(df.shape[1]))
    output_score_df.columns=df.columns
    # start with an initial score of 100 for all cells
    output_score_df[df.columns]=100
    
    # function to count the number of digits in a numerical column
    def count_digits(n):
        import math
        if n is not np.nan and type(n) is not str:
            if n > 0:
                digits = int(math.log10(n))+1
            elif n == 0:
                digits = 1
            else:
                digits = int(math.log10(-n))+2
        elif n is not np.nan and type(n) is str:
            digits=len(n)
        else:    
            digits=0
        return digits
    
    def cds_qty_version(n):
        import math
        import numpy as np
        import re
        if n is not np.nan and type(n) is not str:
            if n > 0:
                digits = int(math.log10(n))+1
            elif n == 0:
                digits = 1
            else:
                digits = int(math.log10(-n))+1
        elif n is not np.nan and type(n) is str:
            #This regex checks that input is NUMBER,optionally it can have single hyphen '-' or upto 2 digits beyond decimal point
            #If it doesn't match this pattern it returns the entire string length "25 6" returns 4
            #Else it will return the correct number of digits of pre-decimal part e.g. "-256.00" returns 3
            #But "-256.000" returns 8
            match = re.search(r"-{0,1}([0-9]{1,}){1}(?:\.[0-9]{0,2}){0,1}",n)
            if match is not None:
                match_len = match.end() - match.start()
                if ((len(n) == match_len) and (len(match.groups())>0)):
                    digits=len(match.group(1))
                else:
                    digits=len(n)
            else:
                digits = len(n)
        else:    
            digits=0
        return digits
    
    def fnPeriod(val):
        valSp =str(val).split(".") 
        if len(valSp)==2:
            if valSp[1]=='':
                return False
            else:
                return True
        elif len(valSp)==1:
            return True
        else:
            return False
    
    # check for missing values in each of the columns
    for i in list(df.columns):
        
        if df[i].isnull().values.any():
            #print_output("Missing values found in "+i+ " (-100)", "red")
            # get their indices and penalize those cells by 100
            nan_idx=list(df.loc[df[i].isnull()].index)
            output_score_df.loc[nan_idx,i]=output_score_df.loc[nan_idx][i].sub(100)
       
    # *************************************************** Particular ******************************************************
    
    # check whether Particular is of object datatype
    if df["Particular"].dtypes==object:
        #print_output("Particular is of object datatype", "green")
        
        # check whether cells in Particular contain only digits by creating Particular series and removing spaces between words
        part_series=pd.Series([],dtype=pd.StringDtype())
        part_series=df["Particular"].apply(lambda x: 
                                           str(x).replace(" ","").replace(".","").replace(",","").replace("-","").replace(":","").replace("/","").replace("\\","")) 
        part_bool_list=list(part_series.apply(str.isdigit))
        # get their indices and penalize those cells by 100
        part_dig_idx=[i for i in range(len(part_bool_list)) if part_bool_list[i]==True]
        
        if part_dig_idx:
            output_score_df.loc[part_dig_idx,"Particular"]=output_score_df.loc[part_dig_idx].Particular.sub(100)
        
            #print_output("There are entities consisting of only digits in Particular (-100)", "red")
            
            
    else:
        #print_output("Particular is not of object datatype (-100 for the whole column)", "red")
        output_score_df.loc[:,"Particular"]=output_score_df.loc[:].Particular.sub(100)
    for row in range(len(df['Particular'].index)):
        
        ind=df['Particular'].index[row]
        if re.match(r'[0-9]{1,2}(-|/)[0-9]{1,2}(-|/)[0-9]{4}',str(df.loc[ind,'Particular']).strip()) or re.match(r'[0-9]{7,}',str(df.loc[ind,'Particular']).strip()):
            output_score_df.loc[ind,'Particular']=output_score_df.loc[ind,'Particular']-10
        
    
    # *******************************************  Other (numerical) columns *************************************************
    
    num_col_list=list(df.columns[1:])
    for i in list(num_col_list):
        
        # check whether number of digits in Quantity is greater than 2
        try:
            if i=="Quantity":

                qty_dig_idx=[]
                null_idx=list(df.loc[df[i].isnull()].index)
                for j in range(len(df[i])):
                    if ((cds_qty_version(df[i].iloc[j])>2) and (df[i].iloc[j] is not np.nan) and (j not in null_idx)):
                        qty_dig_idx.append(j)

                # get their indices and penalise those cells by 10
                if  qty_dig_idx:
                    output_score_df.loc[qty_dig_idx,i]=output_score_df.loc[qty_dig_idx][i].sub(10)
                
                    #print("There are entities in "+i+" containing more than 2 digits (-10)", "red")
                    

        # numpy NaN is found instead of float NaN
        except ValueError:
            #print_output("Numpy NaN found in "+i+" instead of float NaN", "yellow")
            not_null_idx=list(df.loc[df[i].isnull()==False].index)
            for j in not_null_idx:
                if (cds_qty_version(df[i].iloc[j])>2):
                    qty_dig_idx.append(j)
            
            # get their indices and penalise those cells by 10
            if qty_dig_idx:
                output_score_df.loc[qty_dig_idx,i]=output_score_df.loc[qty_dig_idx][i].sub(10)
            
                #print("There are entities in "+i+" containing more than 2 digits (-10)", "red")
                
                
        # check whether number of digits in After_discount_amount is 1
        try:
            if i=="After_discount_amount":
                
                aft_dig_idx=[]
                for j in range(len(df[i])):
                    if ((count_digits(df[i].iloc[j])==1) and (df[i].iloc[j] is not np.nan)):
                        aft_dig_idx.append(j)

                # get their indices and penalise those cells by 10
                if  aft_dig_idx:
                    output_score_df.loc[aft_dig_idx,i]=output_score_df.loc[aft_dig_idx][i].sub(10)
                
                    #print("There are entities in "+i+" containing exactly one digit (-10)", "red")
                    

        # numpy NaN is found instead of float NaN
        except ValueError:
            #print_output("Numpy NaN found in "+i+" instead of float NaN", "yellow")
            not_null_idx=list(df.loc[df[i].isnull()==False].index)
            for j in not_null_idx:
                if (count_digits(df[i].iloc[j])==1):
                    aft_dig_idx.append(j)

            # get their indices and penalise those cells by 10
            if  aft_dig_idx:
                output_score_df.loc[aft_dig_idx,i]=output_score_df.loc[aft_dig_idx][i].sub(10) 
            
                #print_output("There are entities in "+i+" containing exactly one digit (-10)", "red")
                
        
        # check whether column has int/float datatype
        if ((df[i].dtypes!=int) & (df[i].dtypes!=float)):
            #print(i+" is not of int or float datatypes", "red")

            # check whether column has object datatype
            if df[i].dtypes==object:
                #print("Object type found in "+i+" (-50)", "red")
                # get their indices and penalize those cells by 50
                obj_idx=list(df.loc[df[i].apply(type)==str].index)
                output_score_df.loc[obj_idx,i]=output_score_df.loc[obj_idx][i].sub(50)
                                
                # check for NE
                ne_idx=[]
                for j in range(len(df[i])):
                    if (df[i].iloc[j]=="NE"):
                        ne_idx.append(j)
        
                # get their indices and offset 50 for object datatype
                if ne_idx:
                    output_score_df.loc[ne_idx,i]=output_score_df.loc[ne_idx][i].add(50)
    
                    #print("There are NEs in "+i+" (offset 50 for object)", "green")
                    
                
                # check whether there is a single dash ("-") in numerical columns
                dash_idx=list(df.loc[df[i]=="-"].index)
                if dash_idx:
                    output_score_df.loc[dash_idx,i]=output_score_df.loc[dash_idx][i].sub(50)

                    # get their indices and penalize those cells by 50
                    #print_output("Entity in "+i+" is just a single dash (-50)", "red")
                    
                    
                    # offset 10 for exactly 1-digit logic in After_discount_amount (to prevent negative values)
                    if i=="After_discount_amount":
                        #print_output("Entity in "+i+" is just a single dash and is exactly 1 digit (offset 10 for exactly 1 digit)", "yellow")
                        output_score_df.loc[dash_idx,i]=output_score_df.loc[dash_idx][i].add(10)
                
                # check whether there are multiple entities in column
                if df[i].str.split(" ").notnull().values.any()==True:
                    # get their indices and penalize those cells by 50
                    temp_space_idx=list(df.loc[df[i].str.split(" ").notnull()].index)
                    space_idx=[]
                    # check whether length of the string list is greater than 2 (i.e. contains at least 2 words)
                    for idx in temp_space_idx:
                        if len(df[i].str.split(" ")[idx])>=2:
                            space_idx.append(idx)

                    if  space_idx:
                        output_score_df.loc[space_idx,i]=output_score_df.loc[space_idx][i].sub(50)
                        #print_output("Multiple entities separated by a space are present in "+i+" (-50)", "red")
                        
                
                        # offset 10 for more than 2-digits logic in Quantity (to prevent negative values)
                        if i=="Quantity":
                            #print("Multiple entities separated by a space are present in "+i+" and have more than 2 digits (offset 10 for more than 2 digits)", "yellow")
                            output_score_df.loc[space_idx,i]=output_score_df.loc[space_idx][i].add(10)
                            
                # find those numerical entities with commas instead of decimal points (e.g.: 48900,20)
                num_series_1=pd.Series([],dtype=pd.StringDtype())
                num_series_1=df[i].apply(lambda x: str(x).replace(",","")) 
                num_bool_list_1=list(num_series_1.apply(str.isdigit))
                # get their indices and penalize those cells by 10
                com_idx=[x for x in range(len(num_bool_list_1)) if ((num_bool_list_1[x]==True)&(type(df[i][x])==str))]
                
                if com_idx:
                    output_score_df.loc[com_idx,i]=output_score_df.loc[com_idx][i].sub(10).add(50)                

                    #print_output("There are entities with commas instead of decimal points in "+i+" (-10; offset 50 for object)", "yellow")
                    
                             
                # check if string numerical entity is being wrongfully penalised by 10
                entity_idx=[]
                for j in range(len(df[i])):
                    if ((type(df[i].iloc[j])==str) and (df[i].iloc[j].isdigit()==True)):
                        entity_idx.append(j)
                
                # get their indices and offset their penalties for "comma instead of decimal" module
                if  entity_idx:
                    output_score_df.loc[entity_idx,i]=output_score_df.loc[entity_idx][i].add(10)
                
                    #print_output("String numerical entities found in "+i+" (offset 10 for comma instead of decimal point module)", "green")
                    
                
                # check if negative string numerical entity is being wrongfully penalised by 50
                # Kartik: Added another AND condition to check if '-' is in 1st position of numerical string
                neg_entity_idx=[]
                for j in range(len(df[i])):
                    if ((type(df[i].iloc[j])==str) and (df[i].iloc[j].replace(".","").replace("-","").isdigit()==True) and (("-" in df[i].iloc[j])==True) and (df[i].iloc[j].find('-')==0)):
                        neg_entity_idx.append(j)
                
                # get their indices and offset their penalties for being object datatype
                if neg_entity_idx:
                    output_score_df.loc[neg_entity_idx,i]=output_score_df.loc[neg_entity_idx][i].add(50)
                    #print("Negative string numerical entities found in "+i+" (offset 50 for object datatype)", "green")
                    
                                
                # find those numerical entities which have decimal points (e.g.: 48900.20)    
                # We consider only entries with single period.
                num_series_2=pd.Series([],dtype=pd.StringDtype())
                #num_series_2=num_series_1.apply(lambda x: str(x).replace(".",""))
                num_series_2=df[i].apply(lambda x: str(x).replace(".",""))
                num_series_2_ct_periods = df[i].apply(lambda x: str(x).count('.'))
                num_series_2_valid_period = df[i].apply(fnPeriod)
                num_bool_list_2=list(num_series_2.apply(str.isdigit))
                # get their indices and offset their penalties for being object datatype
                comdec_idx=[x for x in range(len(num_bool_list_2)) if ((num_bool_list_2[x]==True)&(type(df[i][x])==str)&("." in num_series_1[x])&(num_series_2_ct_periods[x]==True)&(num_series_2_valid_period[x]==True))]

                if comdec_idx:
                    output_score_df.loc[comdec_idx,i]=output_score_df.loc[comdec_idx][i].add(50)
                    #print("Commas Decimal pts "+i+" (offset 50 for object datatype)", "green")
                    
                                        
           
    
    # ********************************************** Row and Column Scores ************************************************
    
    # row and column scores are calculated by considering averages of cell scores
   
    column_score_df = output_score_df.sum(axis=0).astype(float).div(output_score_df.shape[0]).astype(float).round(2)
    
    #output_score_df.loc['Column_Score']=output_score_df.sum(axis=0).astype(float).div(output_score_df.shape[0]).astype(float).round(2)
    output_score_df["Row"]=output_score_df.sum(axis=1).div(output_score_df.shape[1]).round(2)
    #composite_score=output_score_df.loc["Column_Score","Row_Score"]
    composite_score = column_score_df.mean()
    output_score_df = output_score_df.set_index(original_df.index)
    for i in output_score_df.columns:
        output_score_df=output_score_df.rename(columns={i:i+"_score"})
    
    return output_score_df, column_score_df, composite_score, original_df

#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################