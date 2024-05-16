def table_type_classification(y,model,vectorizer,pagerule):
    import logging
    """
    To classify tables into summary, Detail and others based on ML model - Linear SVC
    
    Model Features
     + text from tables (including the column names)
     + Number of rows
     + Number of columns
     + Word/Number ratio
    
    Vectorizer 
     + TFIDF - word/ character level (3 grams)
    """
    ign_items=['mics', 'cataract','description','particulars',"aphop210011057","dialysis","chemotherapy","cholecystectomy",
               "hemorrhoidectomy","l.s.c.s","disposals",'package','lft','pahcreaticography','haematology']
    summary_items=["patient name bill amount","bill no & date amt"]
   
    from scipy.sparse import hstack
    import re
    import gc
    df_act=y.copy()
    df_act=df_act[[col for col in df_act.columns if str(col) not in ['bb_rowwise','con_rowwise']]]
    row_num,col_num=df_act.shape
    l1=[str(x).lower().strip() for x in df_act.columns.tolist() if len(str(x).lower().strip()) not in [''," ",'nan','unnamed']]
    l2=[str(x).strip() for val in df_act.values.tolist() for x in val if len(str(x).lower().strip()) not in [''," ",'nan','unnamed']]
    text = " ".join(l1+ l2)
    def clean_me(text):
        text=text.lower()
        only_text=re.sub(r'[^A-Za-z ]','',text)
        wor=len(only_text.split())
        num=len(re.sub(r'[^0-9 ]','',text).split())
        if wor!=0:
            ratio=round(num/wor,2)
        else:
            ratio=0
        return [text],ratio
    try:
        text,ratio = clean_me(text)
    except UnboundLocalError:
        text,ratio=[' '],""

    features_train_text = vectorizer.transform(text)
    stacked_features=hstack((features_train_text,[col_num],[row_num],[ratio]))
    result=model.predict(stacked_features)
    result=result[0]
    gc.collect()
    if result=='D' and len(l2)!=0:
        op='Detail'
    elif result=='O' or len(l2)==0:
        op='Others'
        pagerule.append(f'Table marked as Others {len(l2)}')
    elif result == 'S' or len(l2)!=0:
        op="Summary"
    else:
        op='NHI'
    if result=="O" and len(l2)>0 and any(x in text[0] for x in summary_items):
        op="Summary"
        logging.info("Modified Others to Summary due to rules")
        pagerule.append('Table moved to summary due to items matched')

    if result=="O" and len(l2)>0 and (any(x in text[0] for x in ign_items) or len(df_act.columns)>4):
        op="Detail"
        logging.info("Modified Others to Detail due to rules")
        pagerule.append('Table moved to others due to items matched')

    del df_act,result
    gc.collect()
    
    return op,pagerule


#####################################################################
 # Copyright(C), 2023 IHX Private Limited. All Rights Reserved
 # Unauthorized copying of this file, via any medium is 
 # strictly prohibited 
 #
 # Proprietary and confidential
 # email: care@ihx.in
#####################################################################