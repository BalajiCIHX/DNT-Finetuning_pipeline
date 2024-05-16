def header_disambiguition(dict_,dict_values,merged_df,f_header,key_val,location_of,columns_to_be_extracted,pagerule):
    import re
    import nltk

    try:
        
        from nltk.util import ngrams
        from nltk.tokenize import word_tokenize
        from nltk.util import ngrams
    except (NameError,LookupError):

        nltk.download('punkt')

        from nltk.util import ngrams
        from nltk.tokenize import word_tokenize
        from nltk.util import ngrams

    
    if f_header==1:
        merged_df_cols__=merged_df.columns
        merged_df_cols__=[str(col).strip().strip('.').strip().lower().replace("patient amount","nan").replace("payer amount","nan_1") for col in merged_df_cols__]

        merged_df.columns=merged_df_cols__
        def get_key(val,dict_):
            for i in dict_.keys():
                dict_val_k=dict_[i]

                dict_val_k = [str(item).lower() for item in dict_val_k]
                if val in dict_val_k:

                    return i
            return "key doesn't exist"

        columns_to_be_extracted=[]
        keys_present=[]
        location_of={}
        multigram_to_skip_loc=[]
        
        for i in merged_df_cols__:
            if isinstance(i,str):
                ###if direct hit to dictionary
                if i in dict_values:
                    
                    columns_to_be_extracted.append(i.strip())
                    keys_present.append(get_key(i,dict_))
                    location_of[i]=list(merged_df_cols__).index(i)+1
                    pagerule.append(f'Direct hit of column {i} at {list(merged_df_cols__).index(i)+1}')
                #### if bigram found merged
                elif i not in dict_values and len(i.split(" "))==2:
                    multigram_to_skip_loc1=[]
                    o,p=i.split(' ')
                    o=o.strip()
                    p=p.strip()

                    unique_keywords=dict_['Unit_price']+dict_['After_discount_amount']+\
                                        dict_['Before_discount_amount']+dict_['Discount']

                    ####function to get n numbers of grams provided the token########
                    def get_ngrams(text, n ):
                        n_grams = ngrams(word_tokenize(text), n)
                        return [ ' '.join(grams) for grams in n_grams]
                    grams_vals=10
                    all_vals_=[]
                    main_vals_=[]
                    for y in range(1,grams_vals):
                        lsp=get_ngrams(i,y)
                        for u in lsp:
                            if len(i)>1:
                                all_vals_.append(u)

                        main_vals_.append(all_vals_)

                    ###all combitions of grams are stored in a list in a sorted manner (by len)
                   
                    all_vals_=list(set([item for sublist in main_vals_ for item in sublist]))
                    all_vals_=sorted(all_vals_, key=len,reverse=True) 
                    all_vals_=[e.strip() for e in all_vals_]
                    all_vals_=[e.lower() for e in all_vals_]


                    ##########################

                    ##storing unique_keywords (all values from unit_price, before discount amount and after discount amount)
                    unique_keywords=dict_['Unit_price']+dict_['Quantity']+dict_['Before_discount_amount']+dict_['After_discount_amount']+dict_['Discount']
                    unique_keywords=[zi for zi in unique_keywords if zi!='nan']
                    ##initializing flag =-1
                    flag=-1
                    ##checking if particular is merged with any of (unit_price,before discount amount and after discount amount)
                    ## if yes breaking needs to done else no need of breaking (we are okay if garbage is coming with particulars
                    ##we can clean it afterwards, its not affecting our process)
                    if len(set(dict_['Particular']).intersection(set(all_vals_)))>0:
                        for xi in all_vals_:
                            if xi in unique_keywords and xi!='no':
                                ###put flag=1 if breaking has to done
                                flag=1
                                break

                            else:
                                ###put flag=0 if no breaking required
                                flag=0

                ###########################################
                    ###if breaking has to be done perform following procedure
                    ###below code takes O(first token) and p(second token) and checks if match found
                    ##if yes --append location, columns_to_be_extracted and keypresent
                    if flag==1:
                        # pagerule.append(f'Column names merged {o} {p}')
                        if o in dict_values:
                            
                            location_of[o]=(1,list(merged_df_cols__).index(i)+1)
                            columns_to_be_extracted.append(o.strip())
                            pagerule.append(f'Column names merged {o} {p} at {location_of[o]}')
                            keys_present.append(get_key(i,dict_))
                        if p in dict_values:
                            location_of[p]=(2,list(merged_df_cols__).index(i)+1)
                            columns_to_be_extracted.append(p.strip())
                            pagerule.append(f'Column names merged {o} {p} at {location_of[p]}')
                            keys_present.append(get_key(i,dict_))
                    elif flag==0:
                        ####if breaking is not required perform following procedure
                        ##below code takes directly append i to --locationof,columns_to_be_extracted and keypresent
                        ###logic mentioned below is slightly tricky.....hold on
                       

                        columns_to_be_extracted.append('particular')
                        
                        if 'particular' not in location_of.keys():
                            location_of['particular']=list(merged_df_cols__).index(i)+1
                            
                            keys_present.append('Particular')
                            multigram_to_skip_loc.append(i)
                        else:
                            location_of['particulars']=list(merged_df_cols__).index(i)+1
                                
                                
                    else:
                        if o in dict_values:
                            location_of[o]=(1,list(merged_df_cols__).index(i)+1)
                            columns_to_be_extracted.append(o.strip())
                            keys_present.append(get_key(i,dict_))

                        if p in dict_values:
                            location_of[p]=(2,list(merged_df_cols__).index(i)+1)
                            columns_to_be_extracted.append(p.strip())
                            keys_present.append(get_key(i,dict_))

                #### if trigram found merged

                elif i not in dict_values and len(i.split(" "))>2 or len(i.split("."))>2:

                    ####function to get n numbers of grams provided the token########
                    def get_ngrams(text, n ):
                        n_grams = ngrams(word_tokenize(text), n)
                        return [ ' '.join(grams) for grams in n_grams]
                    grams_vals=10
                    all_vals_=[]
                    main_vals_=[]
                    for y in range(1,grams_vals):
                        lsp=get_ngrams(i,y)
                        for u in lsp:
                            if len(i)>1:
                                all_vals_.append(u)

                        main_vals_.append(all_vals_)
                    ###all combitions of grams are stored in a list in a sorted manner (by len)

                    all_vals_=sorted(all_vals_, key=len,reverse=True)

                     ###lowering the combitions of grams and stripping whitespace

                    ##########################
                    multigram_to_skip_loc=[]
                    ##storing unique_keywords (all values from unit_price, before discount amount and after discount amount)
                    unique_keywords=dict_['Unit_price']+dict_['After_discount_amount']+dict_['Before_discount_amount']+dict_['Discount']
                    unique_keywords=[zi for zi in unique_keywords if zi!='nan']
                    ##initializing flag =-1

                    flag=-1
             ##checking if particular is merged with any of (unit_price,before discount amount and after discount amount)
                    ## if yes breaking needs to done else no need of breaking (we are okay if garbage is coming with particulars
                    ##we can clean it afterwards, its not affecting our process)

                    if len(set([zi.lower() for zi in dict_['Particular']]).intersection(set(all_vals_)))>0:
                        for xi in all_vals_:
                            if xi in unique_keywords and xi!='no':
                                flag=1
                                break


                            else:
                                flag=0
                                #par_name_to_change='servname'
                    #########################################
                    if flag==0:
                         ####if breaking is not required perform following procedure
                        ##below code takes directly append i to --locationof,columns_to_be_extracted and keypresent
                        columns_to_be_extracted.append(i)
                        location_of[i]=list(merged_df_cols__).index(i)+1
                        keys_present.append(i)
                        dict_values.append(i)

                        dict_['Particular'].append(i)
                        
                        multigram_to_skip_loc.append(i)

                    else:
                        ####if multiple grams are merged we will extract first the token which has highest len
                        #and check if it's present in dictionary. If it is present then we will
                        #discard that token from column_name temporary and check presence of other tokens.

                        all_vals_=sorted(all_vals_,key=len,reverse=True)
                        all_vals_improved=[]
                        for b in all_vals_:
                            if b in dict_values:
                                all_vals_improved.append(b)

                        all_vals_improved=[i for i in all_vals_improved if i in dict_values]
                        iz=i
                        for v in all_vals_improved:

                            if len(v.split(" "))>1 and v in dict_values:

                                v=str(v)
                                temp_key=get_key(v,dict_)
                                
                                if temp_key=="Particular":
                                    iz=[re.sub(v,"description",iz )][0]
                                    iz=iz.strip()
                                    all_vals_improved.append("description")

                                elif temp_key=="Quantity":

                                    iz=[re.sub(v,"qty",iz )][0]
                                    iz=iz.strip()
                                    all_vals_improved.append("qty")

                                elif temp_key=="Unit_price":
                                    iz=[re.sub(v,"rate",iz )][0]
                                    iz=iz.strip()
                                    all_vals_improved.append("rate")
                                elif temp_key=="Before_discount_amount":
                                    iz=[re.sub(v,"amount",iz )][0]
                                    iz=iz.strip()
                                    all_vals_improved.append("amount")
                                elif temp_key=="Discount":
                                    iz=[re.sub(v,"discount",iz )][0]
                                    iz=iz.strip()
                                    all_vals_improved.append("discount")
                                else:
                                    v=v.replace('(','').replace(')','')
                                    iz=[re.sub(v,"net",iz )][0]
                                    iz=iz.strip()
                                    all_vals_improved.append("net")


                        ##assigning ith_=i. ith_ is used throughout the process. It stores the remaining tokens
                        #when separated by token
                        all_vals_improved=iz.split(" ")
                        all_vals_improved_=[i for i in all_vals_improved  if len(i)>1]
                        all_vals_improved=all_vals_improved_

                        all_vals_improved=sorted(all_vals_improved,key=len,reverse=True)

                        for token in all_vals_improved:
                            if token not in columns_to_be_extracted:
                                ##3partition method separate the column_name by token and checks the index
                                temp_list=iz.split(" ")

                                temp_list=[i for i in temp_list if len(i)>1]
                                kl=temp_list.index(token)+1

                                #get the key where token is present
                                columns_to_be_extracted.append(token)
                                keys_present.append(get_key(token,dict_))
                                location_of[token]=(kl,list(merged_df_cols__).index(i)+1)


                else:
                    ###if two grams are merged (eg. particularamount, qtyunitprice)

                    ###checks if startswith token is present in dictionary
                    if i not in dict_values and len(i.split(" "))==1 and i not in " ":

                        if list(filter(i.startswith,dict_values))!=[]:
                            if len(list(filter(i.endswith, dict_values)))>0:
                                location_of[list(filter(i.startswith,dict_values))[0]]=(1,list(merged_df_cols__).index(i)+1)
                                columns_to_be_extracted.append(list(filter(i.startswith, dict_values))[0])

                            else:
                                location_of[i]=list(merged_df_cols__).index(i)+1
                                columns_to_be_extracted.append(i)

                    ##checks if endswith token is present in dictionary
                    if i not in dict_values and len(i.split(" "))==1 and i not in " ":

                        if list(filter(i.endswith,dict_values))!=[]:

                            if len(list(filter(i.startswith, dict_values)))>0:
                                location_of[list(filter(i.endswith,dict_values))[0]]=(2,list(merged_df_cols__).index(i)+1)
                                columns_to_be_extracted.append(list(filter(i.endswith, dict_values))[0])
                            else:
                                location_of[i]=list(merged_df_cols__).index(i)+1
                                columns_to_be_extracted.append(i)



        ###assigning key and ther values 
        key_val={}
        key_val_extrac=[]

        for xi in columns_to_be_extracted:

            try:
                if xi in multigram_to_skip_loc or xi in multigram_to_skip_loc1:
                    key_val_extrac.append(xi)
            except NameError:
                key_val.setdefault(get_key(xi,dict_), [])
                key_val[get_key(xi,dict_)].append(xi)

                
            else:
                key_val.setdefault(get_key(xi,dict_), [])
                key_val[get_key(xi,dict_)].append(xi)
       
    
    return key_val,location_of,columns_to_be_extracted,merged_df,pagerule


#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################

#######################################################################################################################