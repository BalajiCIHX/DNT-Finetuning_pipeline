def bill_digit_module_(list_of_images,hospital_id,claim,pocr,root_path,txn_id):
   
    from bill_modules.ocr_dontcall import ocr
    from bill_modules.reference_files import header_dictionary
    from bill_modules.header_identification import header_identification
    from bill_modules.headersless_extraction import Headerless_extraction_module
    from bill_modules.header_disambiguation import header_disambiguition
    from bill_modules.data_disambiguation import data_disambiguation
    from bill_modules.table_type_classification import table_type_classification
    from bill_modules.tentacles import hospital_specific_logic
    from bill_modules.pre_processing import preprocessing_
    from bill_modules.confidence_score_map import conf_map
    from bill_modules.output_prep import output_prep
    from bill_modules.utils import get_hospital_id,remove_unnecessary_columns,sort_tables,polishdf,is_unique,call_ocr_matchmaker,prepare_rules,particulars_correction,ignore_omm
    from bill_modules.rescue_rules import rescue_rules
    from bill_modules.sub_heading_clearing import sub_heading_clearing_module
    from bill_modules.post_processing import Post_processing
    
    from datetime import datetime
    
    import os
    import logging
    import pandas as pd
    import gc
    start=datetime.now()
    rule_dict={}
    hospital_df=pd.read_csv(os.environ['CONFIG_DIR']+"/reference_files/Hospitals_OCR_Engine.csv")
    hospital_id,ocr_type_bill,hospital_name=get_hospital_id(str(hospital_id).split('.')[0],hospital_df)
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(module)s] %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info(f"Hospital Name = {hospital_name} , Hospital Id = {hospital_id} , Ocr Type By Mapping = {ocr_type_bill}")
    dict_,dict_values,post_processing_dict,model,vectorizer_text,model_invoice,vectorizer_text_invoice,\
        model_aws,vectorizer_aws,clf,clf_second,tfidf_word,tfidf_char,nme_clf,\
            nme_tfidf_word,nme_tfidf_char,ppdict_pm,ppdict_par,config=header_dictionary(os.environ["CONFIG_DIR"]+"/reference_files")
    
    logging.info("Reference Files Loaded")

    ocr_mapper={"azure_layout":ocr.azure_layout,"azure_invoice":ocr.azure_invoice,"aws_textract":ocr.aws_textract,"blm_layout":ocr.blm}
    model_mapper={"azure_layout":(model,vectorizer_text),"aws_textract":(model_aws,vectorizer_aws),"azure_invoice":(model_invoice,vectorizer_text_invoice),
                  "blm_layout":(model,vectorizer_text)}
    ocr_response,pages,head,docr_list=[],[],[],[]

    all_pdfs=pd.DataFrame()
    page_nos=0
    ocr_selection={}
    ocr_start=datetime.now()
    logging.info(f'OCR Started at {ocr_start}')
    
    logging.info(f'Number of pages in the claim {len(list_of_images)}')
    ommflag=ignore_omm(hospital_id)
    ocr_rules=[]
    ocr_rules.append(f'OCR Started at {ocr_start}')
    
    # for image in list_of_images:
        
#         if ommflag==True:
#             ocrm_start=datetime.now()
#             res=call_ocr_matchmaker(image)
#             ocrm_end=datetime.now()
#             time_ocrm=(ocrm_end-ocrm_start).total_seconds()
#             logging.info(f'----OCR MatchMaker Timetaken for {image} {time_ocrm}----')
#             ocr_rules.append(f'OCR MatchMaker Timetaken for {image} {time_ocrm}')
            
#             if isinstance(res,dict):
#                 if 'ocr' in res.keys():
#                     ocr_type=res['ocr']
#                     if ocr_type=='blm_layout':
#                         logging.info('OCR type made to azure as blm is down')
#                         ocr_type="azure_layout"
#                     ocr_rules.append(f'OCR MatchMaker Timetaken for {ocr_type} {image}')
                    
#                     logging.info(f'OCR Type Matchmaker {ocr_type} {image}')
#                 else:
#                     logging.info(f'Unexpected response from omm -{res}')
#                     ocr_type=ocr_type_bill

#             else:
#                 logging.info(f'OCR Matchmaker failed for {image}')
#                 logging.info(f'{res}')
#                 ocr_type=ocr_type_bill
#         else:
#             logging.info(f"Bypass OMM as hospital id - {hospital_id}")
#             ocr_type=ocr_type_bill
#         logging.info(f'Final OCR Type {ocr_type}')
        # ocr_type='azure_layout'
        # val=ocr_mapper[ocr_type](ocr(image))
        # raw_output,processed_output=val
#         if processed_output['tables']==[] and ocr_type in ["azure_layout","azure_invoice"] and (len(list_of_images)==1 or list_of_images.index(image) != -1):
#             ocr_type="aws_textract"
#             logging.info(f'calling Dynamic ocr for page {image} {list_of_images.index(image)}')
#             docr_list.append(image)
            
#             raw_output,processed_output=ocr_mapper[ocr_type](ocr(image))
#             ocr_rules.append(f'Dynamic OCR called for {image}')
#         pages.append(processed_output['tables'])
#         ocr_selection[image]=ocr_type

        
#         ocr_response.append({image:raw_output})
    # ppocrr=[]
    # ppp=[i.split('_',1)[-1] for i in list_of_images]
    # for pageidd in ppp:
    #     for p_key,p_value in pocr.items():
    #         if pageidd in p_key:
    #             ppocrr.append(p_value)
    # print("WE are HERE!!!")
    # ppocrr = []
    # # list_of_keys = [list(key.keys())[0].split('/')[-1] for key in pocr]
    # keys = {}
    # for key in pocr:
    #     keys[list(key.keys())[0].split('/')[-1]] = list(key.keys())[0]

    # # print(keys)
    # # print(list_of_images)
    # for pageidd in list_of_images:
    #     if pageidd in keys:
    #         for idx in range(len(pocr)):
    #             if keys[pageidd] in pocr[idx].keys():
    #                 ppocrr.append(pocr[idx][keys[pageidd]])
    #                 break
            # ppocrr.append(pocr[0][keys[pageidd]])
            # print("IFDDD")
        # else:
            # print("ELSEDDDDD")

    # print(ppocrr[0])
    # ppocrr = pocr

    # for ppo in pocr:
    #     print(ppo)
    ppo = pocr
    indd = 0
    # print(list_of_images[indd])
    if type(ppo)==list:
        ppo=ppo[0]
    if 'DocumentMetadata' in ppo.keys():
        ocr_type='aws_textract'
        _,processed_output=ocr_mapper[ocr_type](ocr(root_path + claim + '/' + txn_id + '-' + list_of_images[indd], ocr_type, ppo))
        pages.append(processed_output['tables'])
        ocr_selection[list_of_images[indd]]=ocr_type
        # print('aws')

    else:
        if "page_number" in ppo.keys():
            ocr_type='azure_layout'
            _,processed_output=ocr_mapper[ocr_type](ocr(root_path + claim + '/' + txn_id + '-' + list_of_images[indd], ocr_type, ppo))
            pages.append(processed_output['tables'])
            ocr_selection[list_of_images[indd]]=ocr_type
            # print("layout")
    ocr_response=[pocr]
    ocr_end=datetime.now()
    logging.info(f'OCR Ends at {ocr_end}')
    ocr_rules.append(f'OCR Ends at {ocr_end}')
    page_cnt=0
    rule_dict['OcrRules']=ocr_rules

    for indice,tables in enumerate(pages):
        pagerule=[]
        tables=[conf_map(table) for table in tables]
        tables=[remove_unnecessary_columns(table) for table in tables]
        if len(tables)>1:
            try:
                tables=sort_tables(tables)
            except Exception as e:
                logging.info(f'Exception while sorting tables {str(e)}')
                pagerule.append(f'Exception while sorting tables {str(e)}')
        tables1=[]
        mdl,vectorizer=model_mapper[ocr_selection[list_of_images[indice]]]
        page_cnt+=1
        table_type_dict={}
        cnt=0
        for table in tables:
            op,pagerule=table_type_classification(table,mdl,vectorizer,pagerule)
            if op!="Others":
                tables1.append(table)
                table_type_dict[cnt]=op
                cnt+=1
                
        tables=tables1
        del tables1
        cnt=0
        logging.info(f"Number of tables in page {page_cnt} is {len(tables)}")
        for df in tables:
            df,pagerule=preprocessing_(df,hospital_id,pagerule)
            logging.info(f"Number of lines after pre processing {len(df)} page {page_cnt} table {cnt}")
            pagerule.append(f"Number of lines after pre processing {len(df)} page {page_cnt} table {cnt}")

            df=hospital_specific_logic(df,hospital_id,table_type_dict[cnt])
            logging.info(f"Number of lines after hospital specific logic {len(df)} page {page_cnt} table {cnt}")
            pagerule.append(f"Number of lines after hospital specific logic {len(df)} page {page_cnt} table {cnt}")

            df,f_header,head,pagerule=header_identification(dict_values,df,head,pagerule)
            logging.info(f"Number of lines after header identification {len(df)} page {page_cnt} table {cnt}")
            pagerule.append(f"Number of lines after header identification {len(df)} page {page_cnt} table {cnt}")

            df_=df.copy()
            df,key_val,columns_to_be_extracted,location_of,pagerule,pc_flag=Headerless_extraction_module(df,f_header,pagerule)
            pagerule.append(f"Number of lines after headerless extraction {len(df)} page {page_cnt} table {cnt}")
            logging.info(f"Number of lines after headerless extraction {len(df)} page {page_cnt} table {cnt}")

            key_val,location_of,columns_to_be_extracted,df,pagerule=header_disambiguition(dict_,dict_values,df,f_header,key_val,location_of,columns_to_be_extracted,pagerule)
            
            dfa,df,pagerule=data_disambiguation(key_val,location_of,columns_to_be_extracted,df,f_header,dict_,dict_values,pagerule)
            logging.info(f"Number of lines after data disambiguation {len(df)} page {page_cnt} table {cnt}")
            pagerule.append(f"Number of lines after data disambiguation {len(df)} page {page_cnt} table {cnt}")

            dfa=polishdf(dfa)
            coverage,coverage_,pagerule=rescue_rules(dfa,df,table_type_dict[cnt],f_header,pc_flag,pagerule)
            if (coverage==0 or coverage_==0) and f_header==1:
                f_header=0
                logging.info(f"Rescue steps active for page {page_cnt} table {cnt}")
                pagerule.append(f"Rescue steps active for page {page_cnt} table {cnt}")
                df,key_val,columns_to_be_extracted,location_of,pagerule,pc_flag=Headerless_extraction_module(df,f_header,pagerule)

                dfa,df,pagerule=data_disambiguation(key_val,location_of,columns_to_be_extracted,df,f_header,dict_,dict_values,pagerule)
            if f_header==1:
                dfa=particulars_correction(df,dfa)
            else:
                dfa=particulars_correction(df_,dfa)

            flag_subtotal=False
            if len(dfa)>0:
                if (dfa['Unit_price'] == dfa['Unit_price'][dfa.index[0]]).all() and (dfa['Quantity'] == dfa['Quantity'][dfa.index[0]]).all():
                    flag_subtotal=True
            if str(hospital_id) in ["500703","175642"] and flag_subtotal==False:
                condition1 = (dfa['Unit_price'] != "VNA") & (dfa['Quantity'] == "VNA") & (dfa['Before_discount_amount'] == "VNA") & (dfa['After_discount_amount'] == "VNA")
                condition2 = (dfa['Before_discount_amount'] != "VNA") & (dfa['Quantity'] == "VNA") & (dfa['Unit_price'] == "VNA") & (dfa['After_discount_amount'] == "VNA")
                dfa = dfa[~(condition1 | condition2)]
            df=dfa
            # print(dfa.columns)
            dfa.to_csv(f'/home/balajic/workdir/EXPERIMENTS/donut/apollo/data/test_{claim}-{txn_id}-{list_of_images[indice]}.csv',index=False)
            del dfa
            #---------calling sub heading clearing module------------------11
            df,pagerule=sub_heading_clearing_module(df,table_type_dict[cnt],hospital_id,pagerule)
            pagerule.append(f"Rescue steps active for page {page_cnt} table {cnt}")
            logging.info(f"Number of lines after subheading cleaning {len(df)}")
            #---------calling post processing module------------------12
            final_df,pagerule=Post_processing(df,post_processing_dict,ppdict_pm,ppdict_par,pagerule)
            logging.info(f"Number of lines after post processing {len(final_df)}")
            pagerule.append(f"Number of lines after post processing {len(final_df)}")
            
            if is_unique(final_df['Unit_price'])==is_unique(final_df['Quantity'])==True:
                final_df['Unit_price']=final_df['Before_discount_amount']
                final_df['Quantity']='1'
            final_df['Page_no']=list_of_images[page_nos]

            final_df['Table_type']=table_type_dict[cnt]
            final_df['Header prst']=f_header
            all_pdfs=pd.concat([all_pdfs,final_df],axis=0)
            cnt+=1
        page_nos+=1
        rule_dict[page_nos]=pagerule        
    df,rule_dict=output_prep(all_pdfs,ocr_type_bill,hospital_name,hospital_id,clf,clf_second,tfidf_word,tfidf_char,nme_clf,nme_tfidf_word,nme_tfidf_char,config,rule_dict,ocr_selection)
    mongo_dict=prepare_rules(rule_dict,df,start,list_of_images,ocr_start,ocr_end,hospital_id,hospital_name,ocr_selection,docr_list)
     
    gc.collect()
    return df,mongo_dict,ocr_response

        
    
