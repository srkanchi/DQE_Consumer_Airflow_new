#############################
#### Jean Wu, 20211002
#### - send graphql API call, based on different query
#### - send dqe API call with graphql data result
#### - save dqe result back to dynamoDB
##############################
#### TODO
#### - find a better way to determine doc type on fly
#### - teams msessage if a test completed or failed
#### - finish protocol
#### - finish trial
#### - test on airflow
#### - run one time load once all the rules are finalized
#### - SSL handshake issue
############################
import time, json, os, copy, sys
import psycopg2
from datetime import timedelta
import pandas as pd
from fstvault.vault import FstVault
from dynamo_db import *
from  get_tpt_id import call_graphql_api,call_dqe
import datetime
import s3fs
from teams_notification import send_teams_message
###########################
#### main function entry point
##########################
if __name__ == "__main__" :

    ## total time: start
    time1 = time.time()
    ## set up variables
    table_name = 'dqe_results_dev'
    ## set up env
    env = {
        #'DATA_SOURCE': 'JSON',
        'DATA_SOURCE': 'GRAPHQLAPI',
    }

    ## run on local or server or no connection needed
    exe_env = 'server' ## 'server', 'local', 'skip'
    localport = 9090 ## if connecting database locally, change port number here
    ## run on test mode or not
    startind = 0
    endind = 10
    test_mode = 0
    test_year = 2021
    backfill_data_path = "./data/2022_protocols_20211025_subset.csv"
    out_dateformat = "%m%d%Y_%H%M%S"
    saves3flag = 1 ## save s3 bucket
    bucketName = 's3://dqe-consumer-airflow-s3/'
    ###########################
    ## get secretes from vault using fstvault - postgres
    ## get insert tpt_id_key and field_testing_id from dqe insert tabel
    ###########################
    postgresConnection = ''
    # if exe_env == 'server':
    #     v = FstVault(aws_auth=True, aws_fort_knox_role='fst-apc-engineering-team-ecstaskexecutionrole', aws_arn=None) 
    #     config = v.read_secret('data-systems/database/postgres-scout')
    #     postgresConnection= psycopg2.connect(user=config['user'], host=config['host'], port=int(config['port']),
    #         password=config['password'], database=config['dbname'])
    #     cursor = postgresConnection.cursor()
    if exe_env == 'server':
        config = {'user': 'fstapcdevteam', 'host': 'scoutdb-postgres.ci2higiaxfal.us-east-1.rds.amazonaws.com', 'port': 5432, \
                    'dbname': 'scoutdb' , 'password': 'FstApc2021FSTAPC2021'}
        postgresConnection= psycopg2.connect(user=config['user'], host=config['host'], port=int(config['port']),
            password=config['password'], database=config['dbname'])
        cursor = postgresConnection.cursor()        
    elif exe_env == 'local':
        postgresConnection = psycopg2.connect(user='fstapcdevteam', host='localhost', port=localport, password='FstApc2021FSTAPC2021', database='scoutdb')
        cursor = postgresConnection.cursor()
    elif exe_env == 'skip':
        print('**** Spip connection to database ****')
    ## if database connection
    if postgresConnection:
        ## build cursor
        cursor = postgresConnection.cursor()

    ## get time set up
    time_now = datetime.datetime.utcnow()
    starttime = time_now - timedelta(hours = 18)
    endtime = starttime + timedelta(hours = 6.5)
    print('**** timeing *******')
    print("*****[time_now, starttime, endtime]*****")
    print([time_now, starttime, endtime])

    ###########################
    ## get test tpt id key dict
    ###########################
    tpt_ids_dict = {}
    tpt_id_key_list = []
    no_dqe_result_list = []
    no_graphql_result_list = []
    no_all_dqe_result_list = []
    error_dynamodb_list = []
    total_time_logs = []
    if test_mode == 0:
        ## get result from insert tabel
        sql_st1 = "select distinct tpt_id_key , create_date from dqe_consumer.insert_tpt_id where create_date >= '{}' and create_date <= '{}'".format(starttime.strftime('%Y-%m-%d %H:%M:%S'), endtime.strftime('%Y-%m-%d %H:%M:%S'))
        # sql_st1 = """select distinct tpt_id_key , create_date from dqe_consumer.insert_tpt_id where tpt_id_key = 'IA22WLDV2NGX'"""
        print("sql_st1:\n", sql_st1)
        cursor.execute(sql_st1)
        tpt_ids_dict = {row[0]: {'tpt_id_key': row[0], 'dqe_insert_timestamp': row[1], 'doc_type': None, 'year': None} for row in cursor.fetchall()}
        tpt_id_key_list = sorted(list(tpt_ids_dict))
        print("n of Valid tpt_id_key:", len(tpt_ids_dict))
        ## key contains tpt_id_key and val contains timestamp, testing
        for key in tpt_id_key_list:
            print('*** result from database *****')
            print([key, tpt_ids_dict[key]])
        
        ## get result from scout_internal
        n_tpt_ids = len(tpt_ids_dict)
        if n_tpt_ids == 0:
            print('**** NO updated tpt_id_key list from dqe_consumer')
            sys.exit()
        sql_st2 = """SELECT DISTINCT ft.tpt_id_key, document_type.decode1 as doc_type, ft.field_year as year
            from scout_internal.field_testing ft
            left join scout_internal.master_code document_type on document_type.code_id = ft.field_testing_type_code_id
            left join scout_internal.master_code status on status.code_id = ft.status_code_id
            where tpt_id_key in {}""".format(tuple(tpt_id_key_list))
        print("sql_st2:\n", sql_st2)
        cursor.execute(sql_st2)
        for row in cursor.fetchall():
            tpt_ids_dict[row[0]]['doc_type'] = row[1]
            tpt_ids_dict[row[0]]['year'] = int(row[2])
            # print(tpt_ids_dict[row[0]])
        cursor.close() 
        postgresConnection.close()

    elif test_mode == 1:
        tpt_ids_dict = {'SP21USAHGBUHG1': {'tpt_id_key': 'SP21USAHGBUHG1', 'doc_type': 'Trial', 'year': 2021, 'dqe_insert_timestamp': 2021}}
        tpt_id_key_list = sorted(list(tpt_ids_dict))
    ## test from databse, with a quick test
    elif test_mode == 2:
        #yyyy-mm-dd hh:mm:ss
        sql_st1 = "select distinct tpt_id_key, create_date from dqe_consumer.insert_tpt_id limit 10"
        cursor.execute(sql_st1)
        tpt_ids = cursor.fetchall()
        tpt_ids_dict = dict(tpt_ids)
        tpt_id_key_list = sorted(list(tpt_ids_dict))
        print("n of Valid tpt_id_key: ", len(tpt_ids_dict))
        ## key contains tpt_id_key and val contains timestamp 
        for key,val in tpt_ids_dict.items():
            print('*** result from database *****')
            print([key,val.year])
        cursor.close() 
        postgresConnection.close()
    elif test_mode == 3:
        ## one time backfill for all TD        
        td_csv = pd.read_csv(backfill_data_path)
        tpt_id_key_list = sorted(td_csv['tpt_id_key'].to_list())
        tpt_ids_dict = {x: test_year for x in tpt_id_key_list}

    #################################
    ## set up counter
    ## loop through all items in dict
    #################################
    counter = 0
    for tpt_id_key in tpt_id_key_list:

        ## start time for 1 item
        time5 = time.time()
        print('\n\n**** Loop through each item ******', str(counter))
        # print('**** Loop through each item ******', str(counter + startind))
        print([tpt_id_key, tpt_ids_dict[tpt_id_key]])     
        doc_type = tpt_ids_dict[tpt_id_key]['doc_type']
        if doc_type == 'Trial Description':
        	doc_type = 'TD'
        ## should check doc type somewhere
        ## get TD result, if length is 12, or doc type = TD
        ## STEP 1 - CALL GRAPHQL API
        ## STEP 2 - DATA QUALITY ENGINE API
        ## start time for 1 item in graphql and dqe api
        time3 = time.time()
        dqe_total_result = {}
        if doc_type == 'TD' and len(tpt_id_key) == 12:
            print('***** This is a TD test ******')
            ###############
            ## get TD-0
            ###############
            graphql_opt = 'TD_0'
            td_0_data = {}
            input_dqe = {}
            td_0_dqe_result = {}
            td_0_data = call_graphql_api(tpt_id_key, graphql_opt)
            
            try: 
            ## check if graphql returns result from TD_0
                if td_0_data and 'trialDescriptions' in td_0_data['data'] and td_0_data['data']['trialDescriptions']:
                    input_dqe = {'data': td_0_data['data'], 'tests': []}
                    input_dqe['tests'] = [ 
                                { "TDCompleteness_0": {
                                    "attributeMapping": "/home/ubuntu/DataQualityEngine/tests/Files/attribute_mapping_0.py",
                                    "weights": "/home/ubuntu/DataQualityEngine/tests/Files/completeness_TD_V1_0_0.csv",
                                    "version": "completeness_TD_V1_0_0"
                                    }
                                }
                    ]
                    ## add rule name nd version to the result for dqe db 
                    td_0_dqe_result = call_dqe(input_dqe, tpt_id_key)
                    ## pass the td0 result to the total result

                    ## dqe_result td 0                 
                    if td_0_dqe_result:
                        dqe_total_result['TDCompleteness_0'] = copy.deepcopy(td_0_dqe_result[0]['TDCompleteness_0'])
                    else:
                        dqe_total_result['TDCompleteness_0'] = {}
                        no_dqe_result_list += [tpt_id_key + ',' + 'TDCompleteness_0']
                        print('*** NO DQE RESULT-TDCompleteness_0:', tpt_id_key)
                        send_teams_message(team_web_hook_url, " NO DQE RESULT-TDCompleteness_0  for " + str(tpt_id_key)  ) 

                else:
                    no_graphql_result_list += [tpt_id_key + ',' + 'TDCompleteness_0']
                    print('*** NO graphql RESULT-TDCompleteness_0:', tpt_id_key)
                    send_teams_message(team_web_hook_url, " NO graphql RESULT-TDCompleteness_0  for "  + str(tpt_id_key)  )

                ##completed td 0     
                print('***** completed TDCompleteness_0 ****')
                send_teams_message(team_web_hook_url, "Completed TD Completeness 0" + str(tpt_id_key) )
 
            except Exception as e:
                print(e) 
                send_teams_message(team_web_hook_url, "Error at TD Completeness 0 section " + e )
            
            
            time.sleep(10)
            
            
            ###############
            ## get TD-1
            ###############
            graphql_opt = 'TD_1'
            td_1_data = {}
            input_dqe = {}
            td_1_dqe_result = {}
            td_1_data = call_graphql_api(tpt_id_key, graphql_opt)
            
            
            try: 
                ## check if graphql returns result from TD_1
                if td_1_data and 'trialDescriptions' in td_1_data['data'] and td_1_data['data']['trialDescriptions']:
                    input_dqe = {'data': td_1_data['data'], 'tests': []}
                    input_dqe['tests'] = [ 
                                { "TDCompleteness_1": {
                                    "attributeMapping": "/home/ubuntu/DataQualityEngine/tests/Files/attribute_mapping_TD_1.py",
                                    "weights": "/home/ubuntu/DataQualityEngine/tests/Files/completeness_TD_V1_0_0.csv",
                                    "version": "completeness_TD_V1_0_0"
                                    }
                                }
                    ]
                    ## add rule name nd version to the result for dqe db 
                    td_1_dqe_result = call_dqe(input_dqe, tpt_id_key)
                    ## pass the td1 result to the total result
                    if td_1_dqe_result:
                        dqe_total_result['TDCompleteness_1'] = copy.deepcopy(td_1_dqe_result[0]['TDCompleteness_1'])
                    else:
                        dqe_total_result['TDCompleteness_1'] = {}
                        no_dqe_result_list += [tpt_id_key + ',' + 'TDCompleteness_1']
                        print('*** NO DQE RESULT-TDCompleteness_1:', tpt_id_key)
                        send_teams_message(team_web_hook_url, " NO DQE RESULT-TDCompleteness_1 for " + str(tpt_id_key)  ) 


                else:
                    no_graphql_result_list += [tpt_id_key + ',' + 'TDCompleteness_1']
                    print('*** NO graphql RESULT-TDCompleteness_1:', tpt_id_key)  
                    send_teams_message(team_web_hook_url, " NO graphql RESULT-TDCompleteness_ 1  for "  + str(tpt_id_key)  )
                
                ## completed td 1
                print('***** completed TDCompleteness_1 ****')
                send_teams_message(team_web_hook_url, " Completed TD Completeness 1 " + str(tpt_id_key) )

            except Exception as e:
                print(e)
                send_teams_message(team_web_hook_url, "Error at TD Completeness 1 section " + e )   

            
            time.sleep(10)


        ## get protocol result
        elif doc_type == 'Protocol' and len(tpt_id_key) == 10:
            print('***** This is a protocol test ******')
            graphql_opt = 'Protocol'
            protocol_data = {}
            input_dqe = {}
            protocol_dqe_result = {}
            protocol_data = call_graphql_api(tpt_id_key, graphql_opt)
            
            
            try: 
            ## check if graphql returns result from TD_1
                if protocol_data and 'protocols' in protocol_data['data'] and protocol_data['data']['protocols']:
                    input_dqe = {'data': protocol_data['data'], 'tests': []}
                    input_dqe['tests'] = [ { 
                            "ProtocolCompleteness": {
                                        "attributeMapping": "/home/ubuntu/DataQualityEngine/tests/Files/attribute_mapping_Protocol.py",
                                        "weights": "/home/ubuntu/DataQualityEngine/tests/Files/completeness_Protocol_V1_0_0.csv",
                                        "version": "completeness_Protocol_V1_0_0" 
                                    }
                            }
                    ]
                    ## add rule name nd version to the result for dqe db 
                    protocol_dqe_result = call_dqe(input_dqe, tpt_id_key)
                    ## pass the td1 result to the total result
                    if protocol_dqe_result:
                        dqe_total_result['ProtocolCompleteness'] = copy.deepcopy(protocol_dqe_result[0]['ProtocolCompleteness'])
                    else:
                        dqe_total_result['ProtocolCompleteness'] = {}
                        no_dqe_result_list += [tpt_id_key + ',' + 'ProtocolCompleteness']
                        print('*** NO DQE RESULT-ProtocolCompleteness:', tpt_id_key)
                        send_teams_message(team_web_hook_url, " NO DQE RESULT-ProtocolCompleteness  for "  + str(tpt_id_key)  )

                else:
                    no_graphql_result_list += [tpt_id_key + ',' + 'ProtocolCompleteness']
                    print('*** NO graphql RESULT-ProtocolCompleteness:', tpt_id_key)
                    send_teams_message(team_web_hook_url, " NO graphql result Protocol Completeness  for "  + str(tpt_id_key)  )

                ## completed protocol     
                print('***** completed ProtocolCompleteness ****')
                send_teams_message(team_web_hook_url, " Completed  Protocol Completeness " + str(tpt_id_key) )
                
        
            except Exception as e:
                print(e)
                send_teams_message(team_web_hook_url, "Error at Protocol section " + e )   

          
            time.sleep(10)


        ## get trial result
        elif doc_type == 'Trial' and len(tpt_id_key) == 14:
            print('***** This is a trial test ******')
            graphql_opt = 'Trial'
            doc_type = 'Trial'
            trial_data = {}
            input_dqe = {}
            trial_dqe_result = {}
            trial_data = call_graphql_api(tpt_id_key, graphql_opt)
            
            
            try: 
            ## check if graphql returns result from TD_1
                if trial_data and 'fieldtrials' in trial_data['data'] and trial_data['data']['fieldtrials']:
                    input_dqe = {'data': trial_data['data'], 'tests': []}
                    input_dqe['tests'] = [ 
                                {
                                    "TrialCompleteness": {
                                    "attributeMapping": "/home/ubuntu/DataQualityEngine/tests/Files/attribute_mapping_trial.py",
                                    "weights": "/home/ubuntu/DataQualityEngine/tests/Files/completeness_Trial_V1_0_0.csv",
                                    "version": "completeness_Trial_V1_0_0"
                                    }
                                }
                    ]
                    ## add rule name nd version to the result for dqe db 
                    trial_dqe_result = call_dqe(input_dqe, tpt_id_key)
                    ## pass the td1 result to the total result
                    if trial_dqe_result:
                        dqe_total_result['TrialCompleteness'] = copy.deepcopy(trial_dqe_result[0]['TrialCompleteness'])
                    else:
                        dqe_total_result['TrialCompleteness'] = {}
                        no_dqe_result_list += [tpt_id_key + ',' + 'TrialCompleteness']
                        print('*** NO DQE RESULT-TrialCompleteness:', tpt_id_key)
                        send_teams_message(team_web_hook_url, "  NO DQE RESULT-Trial  Completeness for " + str(tpt_id_key) )

                else:
                    no_graphql_result_list += [tpt_id_key + ',' + 'TrialCompleteness']
                    print('*** NO graphql RESULT-TrialCompleteness:', tpt_id_key)
                    send_teams_message(team_web_hook_url, "  NO graphql RESULT-Trial  Completeness for " + str(tpt_id_key))
                
                ## completed Trial Completeness 
                print('***** completed TrialCompleteness ****')
                send_teams_message(team_web_hook_url, " Completed  Trial Completeness " + str(tpt_id_key))

            except Exception as e:
                print(e)
                send_teams_message(team_web_hook_url, "Error at Trial section " + e )   
            
            time.sleep(10)
        else:
            print()


        ## get end time for 1 item for graphql and deq result
        time4 = time.time() - time3
        print('1 interation time for graphql and dqe result:', str(time4))


        ## STEP3: STORING TO DYNAMODB 
        ## if dqe_total result valid
        if dqe_total_result:
            if test_mode == 1 or test_mode == 3:
                cur_year = test_year
            elif test_mode == 0 or test_mode == 2:
                cur_year = tpt_ids_dict[tpt_id_key]['year']
            ## get current timestamp
            curr_timestamp = what_time_is_it()            
            error_flag, response = put_one_dqe_item(tpt_id_key, cur_year, dqe_total_result, curr_timestamp, doc_type, table_name)
            # print("error_flag print ",error_flag) 
            print("Response print ",response) 
            print(len(response)) 
            print("FINISH pushing result to DQE database. Counter: " , counter)
            # input('***end of pusing result***')
            print("FINISH pushing result to DQE database. Counter: " , counter)
            send_teams_message(team_web_hook_url, " FINISH pushing result to DQE database. Counter:  " + counter)
            error_dynamodb_list += [tpt_id_key + ',' + str(error_flag)]
        else:
            print('****** NO DQE result created for this tpt_id_key ******', str(tpt_id_key))
            no_all_dqe_result_list += [tpt_id_key]
            send_teams_message(team_web_hook_url, '****** NO DQE result created for this tpt_id_key  ' + str(tpt_id_key))
       
        ## increase counter
        counter = counter + 1
        time.sleep(5)
        ## get total time for 1 item
        time6 = time.time() - time5
        print('Total time for 1 item: ', str(time6))
        total_time_logs += [tpt_id_key + ',' + str(time6)]

    ## save no dqe result list
    print('***** Summary of logs *************')
    print('no_all_dqe_result_list')
    print(no_all_dqe_result_list)
    print('no_dqe_result_list_')
    print(no_dqe_result_list)
    print('no_graphql_result_list')
    print(no_graphql_result_list)
    if exe_env == 'local' or exe_env == 'skip' and saves3flag == 0:
        output_dir = './result/exp_' + datetime.datetime.utcnow().strftime(out_dateformat)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        outfn0 = output_dir + '/no_all_dqe_result_list.csv'
        outfn1 = output_dir + '/no_dqe_result_list.csv'
        outfn2 = output_dir + '/no_graphql_result_list.csv'
        outfn3 = output_dir + '/error_dynamodb_list.csv'
        outfn4 = output_dir + '/total_time_logs.csv'
        with open(outfn0, 'w') as f1:
            f1.write('\n'.join(no_all_dqe_result_list))
        with open(outfn1, 'w') as f1:
            f1.write('\n'.join(no_dqe_result_list))
        with open(outfn2, 'w') as f1:
            f1.write('\n'.join(no_graphql_result_list))
        with open(outfn3, 'w') as f1:
            f1.write('\n'.join(error_dynamodb_list))
        with open(outfn4, 'w') as f1:
            f1.write('\n'.join(total_time_logs))            
    elif saves3flag == 1:
        ## save file to s3 bucket
        s3 = s3fs.S3FileSystem(anon=False)
        subfolder = 'logs/run_' + datetime.datetime.utcnow().strftime(out_dateformat)
        infn1 = bucketName + subfolder + '/no_all_dqe_result_list.csv' # 's3://fst-data-profile-test1/' + 'scout/test_json.txt'
        infn2 = bucketName + subfolder + '/no_dqe_result_list_.csv'
        infn3 = bucketName + subfolder +'/no_graphql_result_list.csv'
        infn4 = bucketName + subfolder +'/error_dynamodb_list.csv'
        infn5 = bucketName + subfolder +'/total_time_logs.csv'
        with s3.open(infn1, 'w') as f1:
            f1.write('\n'.join(no_all_dqe_result_list))
        with s3.open(infn2, 'w') as f1:
            f1.write('\n'.join(no_dqe_result_list))
        with s3.open(infn3, 'w') as f1:
            f1.write('\n'.join(no_graphql_result_list))
        with s3.open(infn4, 'w') as f1:
            f1.write('\n'.join(error_dynamodb_list))
        with s3.open(infn5, 'w') as f1:
            f1.write('\n'.join(total_time_logs))
            
    send_teams_message(team_web_hook_url, "Logs saved to S3 bucket ")
    
    ## STEP 4 - TIME CHECK 
    time2 = time.time() 
    total_time = time2 - time1
    print('***********************')
    print("Total time: " + str(total_time) + '. Total counter: ' + str(counter))   
    send_teams_message(team_web_hook_url, 'Total time: ' + str(total_time) + " Total counter : " + str(counter))


   
