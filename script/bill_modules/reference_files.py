def header_dictionary(reference_path):
        
    import os
    import pandas as pd
    import numpy as np

    import joblib
    import yaml
    import json
    
    dict_=json.load(open(reference_path+'/final_dictionary.json','r'))
    dict_values=[item for val in dict_.values() for item in val]
    
    post_processing_dict=pd.read_csv(reference_path+"/post_processing_dictionary.csv")
    
    ppdict_pm=pd.read_csv(reference_path+"/perfect_match.csv")
    
    ppdict_par=pd.read_csv(reference_path+"/partial_match.csv")

    model = joblib.load(reference_path+'/model.pickle')
    vectorizer_text = joblib.load(reference_path+'/text_vect.pickle')
    
    model_invoice = joblib.load(reference_path+'/model_invoice.pickle')
    vectorizer_text_invoice = joblib.load(reference_path+'/vectorizer_text_invoice.pickle')
    
    model_aws=joblib.load(reference_path+'/model_aws.pickle')
    vectorizer_aws=joblib.load(reference_path+'/vectorizer_aws.pickle')
    
    clf=joblib.load(reference_path+'/Level2_clf_part1')
    clf_second=joblib.load(reference_path+'/Level2_clf_part2')
    tfidf_word=joblib.load(reference_path+'/L2tfidf_word')
    tfidf_char=joblib.load(reference_path+'/L2tfidf_char')
    nme_clf=joblib.load(reference_path+'/nme_clf')
    nme_tfidf_word=joblib.load(reference_path+'/nme_tfidf_word')
    nme_tfidf_char=joblib.load(reference_path+'/nme_tfidf_char')
    config=yaml.safe_load(open(reference_path+'/config.yaml', 'r'))

       
    return dict_,dict_values,post_processing_dict,model,vectorizer_text,model_invoice,vectorizer_text_invoice,model_aws,vectorizer_aws,clf,clf_second,tfidf_word,tfidf_char,nme_clf,nme_tfidf_word,nme_tfidf_char,ppdict_pm,ppdict_par,config
