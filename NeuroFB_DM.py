# coding: utf-8
import pandas as pd
import numpy as np
import csv
import json
import re
import glob

instrumentDF_dict = {}
instrument_dict = {}
codebook = {}
json_nfb=[]

def codeBook_Dictionary():
    #Create a dictionary of dictionaries for codebook keys and corresponding value
    #Codebook reading
    codebook_lines = []
    with open('/Users/md35727/anaconda3/platform/CB.csv') as csvfile:
        csvreader = csv.reader(csvfile, dialect='excel')
        for row in csvreader:
            codebook_lines.append(row)
    
    current_key = ''
    for line_vals in codebook_lines:
        if 'Instrument' in line_vals[0]:
            continue  
        if line_vals[3]:
            if line_vals[3] in codebook:
                raise ValueError('Key {0} already exists in dictionary!'.format(line_vals[3]))
            codebook[line_vals[3]] = {}
            current_key = line_vals[3]
            
        if line_vals[1]:
            codebook[current_key]["Question Label"] = line_vals[1]
   
        if not current_key:
            raise ValueError("Trying to set value with empty key")
        codebook[current_key][line_vals[5]] = line_vals[4]
             
    #Instrument keys readin from codebook
    current_instrmt = ''
    previous_instrmt = ''
    for line_vals in codebook_lines:
        if 'Instrument' in line_vals[0]:
            continue
        previous_instrmt = ''    
        if line_vals[0]:
            if line_vals[0] in instrument_dict:
                raise ValueError('Error')
            current_instrmt = line_vals[0]
            instrument_dict.setdefault(current_instrmt, [])
        while(current_instrmt!=previous_instrmt):
            if line_vals[3]:
                instrument_dict[current_instrmt].append(line_vals[3])
            previous_instrmt = current_instrmt

#recursive if processing multiple assessment files
def file_Clean(file_name, recursive):

    #List of possible visit_ids
    visitID_list = ['V1', 'V1REP', 'V1REP_2', 'V2', 'VA', 'VA_ALG', 
                    'V2REP', 'V2REP_2', 'VA-REP', 'V3', 'V4', 'V5']
    
    if(recursive):
        for file in glob.glob(file_name):
            clean_additionals(file)
            print('FINISHED '+file)
    else:
        print('FILE {0} START'.format(file_name))
        nfb = pd.read_csv(file_name, low_memory=False, skipinitialspace=True)
        column_descriptions = nfb.iloc[0].values
        nfb = nfb.loc[1:,]
        
        #Filling QUEST_90-93 columns with QUEST_ADD_90-93 from part3
        '''if 'part3' in file_name:
            nfb['V2REP_QUEST_90'] = nfb['V2REP_QUEST_ADD_90'].copy()
            nfb['V2REP_QUEST_91'] = nfb['V2REP_QUEST_ADD_91'].copy()
            nfb['V2REP_QUEST_92'] = nfb['V2REP_QUEST_ADD_92'].copy()
            nfb['V2REP_QUEST_93'] = nfb['V2REP_QUEST_ADD_93'].copy()
            nfb['V2_QUEST_90'] = nfb['V2_QUEST_ADD_90'].copy()
            nfb['V2_QUEST_91'] = nfb['V2_QUEST_ADD_91'].copy()
            nfb['V2_QUEST_92'] = nfb['V2_QUEST_ADD_92'].copy()
            nfb['V2_QUEST_93'] = nfb['V2_QUEST_ADD_93'].copy()
            nfb.drop(['V2REP_QUEST_ADD_90', 'V2REP_QUEST_ADD_91', 'V2REP_QUEST_ADD_92', 'V2REP_QUEST_ADD_93',
              'V2_QUEST_ADD_90', 'V2_QUEST_ADD_91', 'V2_QUEST_ADD_92', 'V2_QUEST_ADD_93'], axis=1, inplace=True)'''
           
        nfb.set_index('queried_ursi', inplace=True)
        column_description_dict = dict(zip(nfb.columns.get_values(), column_descriptions.T))
        
        #Remove Nan keys and values without associated question/description
        remove_keys = [] 
        for key, value in column_description_dict.items(): 
            if(type(value) == float or type(value) == np.float_):
                if(np.isnan(value)): 
                    remove_keys.append(key) 
        
        for i in range(len(remove_keys)):
            if remove_keys[i] in column_description_dict:
                del column_description_dict[remove_keys[i]]
        
        #Unique list using visitID_list
        new_cols = ['queried_ursi', 'visit_id']
        processed = []
        for c in nfb.columns:
            for visit in (sorted(visitID_list, key=len, reverse=True)):
                if(visit+'_' in c) and (c not in processed):
                    col = c.replace(visit+'_', '')
                    if(col[0].isdigit()):
                        col = col[col.find('_')+1:]
                    new_cols.append(col)
                    processed.append(c)
        unique_list = [x for i, x in enumerate(new_cols) if new_cols.index(x) == i] #or set(new_cols)
        
        ursi_visit_df = pd.DataFrame(columns=unique_list)
        
        #Dataframe restructuring WITH codebook converting
        row_counter = -1
        target_index_counter = -1
        previous_visit = ''
        
        for i in range(len(nfb.index)):
            previous_visit = ''
            row_counter+=1
            for c in nfb.columns.sort_values():
                for key in unique_list:
                    if key in c:
                        visit_col, nfb_3_col = get_VisitID_ColName(visitID_list, c)
                        if (previous_visit != visit_col):
                            target_index_counter+=1
                            ursi_visit_df.at[target_index_counter, 'visit_id'] = visit_col
                            ursi_visit_df.at[target_index_counter, 'queried_ursi'] = nfb.index[row_counter]
                            previous_visit = visit_col
                        val = nfb.loc[nfb.index[row_counter],c]
                        ursi_visit_df.at[target_index_counter, nfb_3_col] = convertToCodebook(key, val)
    
        #Drop nan rows of dataframe
        ursi_visit_df = ursi_visit_df.drop(drop_NanRows(ursi_visit_df))
        nfb_processed = ursi_visit_df.reset_index(drop=True)
        nfb_processed = nfb_processed.dropna(axis=1, how='all')
        
        #create dictionary according to instrument/assessment
        current_instrument, previous_instrument, instrument_id = '', '', ''
        
        for c in nfb_processed.columns.sort_values(): 
            if(c != 'queried_ursi' and c != 'visit_id'):
                current_instrument = c[:(c.find('_'))] 
                instrument_id = get_instrumentID(c, False)
            if (previous_instrument != current_instrument):
                print(current_instrument)
                if instrument_id in instrumentDF_dict.keys():
                    instrumentDF_dict[instrument_id] = pd.concat([instrumentDF_dict[instrument_id], add_to_instrumentDF(nfb_processed, current_instrument, instrument_id)])
                else:
                    instrumentDF_dict[instrument_id] = add_to_instrumentDF(nfb_processed, current_instrument, instrument_id)
                previous_instrument = current_instrument
        
        
        print('FILE {} COMPLETE'.format(file_name))
def drop_NanRows(df):
    drop_row = True
    drop_list = []
    for i in range(len(df.index)):
        row = df.iloc[i]
        drop_row = True
        for f in range(len(row)-2):
            if(type(row[f+2]) == float or type(row[f+2]) == np.float64):
                if not(np.isnan(row[f+2])):
                    drop_row = False
            elif(type(row[f+2]) == str and row[f+2].lower() != 'nan' and row[f+2] != '~<condSkipped>~'):
                drop_row = False
            else:
                drop_row = False
        if(drop_row):
            drop_list.append(i)
    return drop_list

#Check if string is a number and returns float/int if it is
def convert_StringtoNumber(s):
    try:
        possible_float = float(s)
    except ValueError:
        if("!WithErrors!" in s):
            return convert_StringtoNumber(s.split("!WithErrors!")[0])
        return convert_StringtoUTCTime(s)
    
    if(possible_float.is_integer()):
        return int(possible_float)
    else:
        return possible_float
    
#check if string is time or date and convert to UTC format    
def convert_StringtoUTCTime(s):
    r = re.compile('.*:.*:.* .*')
    a = re.compile('.*:.*')
    d = re.compile('.*/.*/.*')
    if r.match(s):
        hour, minute, second = '', '', ''
        if(type(convert_StringtoNumber(s.split(':')[0])) != str):
            minute = s.split(':')[1]
            second = (s.split(':')[2]).split(' ')[0]
            if(s[s.rfind(' ')+1:] == "AM"):            
                hour = s.split(':')[0]
                if(hour == '12'):
                    hour = '00'
                if(len(hour) == 1):
                    hour = '0'+hour
            elif(s[s.rfind(' ')+1:] == "PM"):
                hour = s.split(':')[0]
                hour = str(convert_StringtoNumber(s.split(':')[0])+12)
                if(hour == '24'):
                    hour = '12'
        UTC_time = '1970-01-01T'+hour+':'+minute+':'+second+'.00Z'
        return UTC_time
    elif a.match(s):
        hour, minute = '', ''
        if(type(convert_StringtoNumber(s.split(':')[0])) != str):
            minute = s.split(':')[1]
            if(convert_StringtoNumber(s.split(':')[0]) < 10):            
                hour = '0'+s.split(':')[0]
            elif(convert_StringtoNumber(s.split(':')[0]) >= 10):
                hour = s.split(':')[0]
        UTC_time = '1970-01-01T'+hour+':'+minute+':00.00Z'
        return UTC_time   
    elif d.match(s):
        year, month, day = '', '', ''
        if(type(convert_StringtoNumber(s.split('/')[0])) != str):
            year = s.split('/')[2]
            if(convert_StringtoNumber(s.split('/')[1]) < 10):            
                day = '0'+s.split('/')[1]
            elif(convert_StringtoNumber(s.split('/')[1]) >= 10):
                day = s.split('/')[1]
            if(convert_StringtoNumber(s.split('/')[0]) < 10):            
                month = '0'+s.split('/')[0]
            elif(convert_StringtoNumber(s.split('/')[0]) >= 10):
                month = s.split('/')[0]
        UTC_time = '20'+year+'-'+month+'-'+day+'T00:00:00.00Z'
        return UTC_time
    else:
        return s

#Convert values in dataframe into corresponding values in the codebook according to column name
def convertToCodebook(k, value):
    if(type(value)==int and not np.isnan(value)):
        value = str(value)
    if(type(value)==str and value.lower()!='nan'):
        sub_key = k[:k.rfind('_')]
        if k in codebook.keys():
            if value in codebook[k]:
                return value+'- '+codebook[k][value]
            elif value not in codebook[k]:
                return value 
        elif sub_key in codebook.keys():
            if value in codebook[sub_key]:
                return value+'- '+codebook[sub_key][value]
            elif value not in codebook[sub_key]:
                return value
        else:
            return value
    elif((type(value) == float or type(value) == np.float64) and np.isnan(value)):
        return value

def get_instrumentID(column_name, var_id):
    instr_id = ''
    found = False
    if not var_id:
        for inst, variables in instrument_dict.items():
            for val in variables:
                if(column_name == val):
                    instr_id = inst
                    found = True
    if (not found) or var_id:
        if((column_name[column_name.rfind('_', 0, column_name.rfind('_'))+1:][0]).isdigit()): #if first character after the last '_' is a digit
            instr_id = column_name[:(column_name.rfind('_', 0, column_name.rfind('_')))]
        else:
            instr_id = column_name[:(column_name.rfind('_'))]
        for inst, variables in instrument_dict.items():             
            if (instr_id == get_instrumentID(variables[0], not var_id)):
                instr_id = inst
    return instr_id   

#Split column name to return visit_id and final column name
def get_VisitID_ColName(vID_list, column_name):
    ncol, visit = '', ''
    for v in (sorted(vID_list, key=len, reverse=True)):
        if(v+'_' in column_name):
            ncol = column_name.replace(v+'_', '')
            visit = v
            if(ncol[0].isdigit()):
                ncol = ncol[ncol.find('_')+1:]
                visit = column_name[:column_name.find(ncol)-1]        
    return visit, ncol

#Returns dataframe with instrument string input
def add_to_instrumentDF(nfb, instrmt, instrmt_id):
    #get column names
    df_columns = ['queried_ursi','visit_id']
    for c in nfb.columns:
        if instrmt in c:
            df_columns.append(c)
    df = pd.DataFrame(columns=df_columns)
    
    #add to dataframe
    for i in range(len(nfb.index)):
        for c in df_columns:
            if instrmt in c:
                df.at[i, 'queried_ursi'] = nfb.queried_ursi[i]
                df.at[i, 'visit_id'] = nfb.visit_id[i]
                df.at[i, c] = convert_StringtoNumber(nfb.loc[i, c])
    #Drop nan rows
    df = df.drop(drop_NanRows(df))
    df['instrument'] = instrmt_id
    df = df[df.queried_ursi != 'queried_ursi']
    df.rename(columns = {'visit_id': instrmt+'_Visit_ID'}, inplace = True)
    
    return df
    
def clean_additionals(file_name):
    nfb = pd.read_csv(file_name, header=1, dtype=str, low_memory=False, skipinitialspace=True)

    nfb.rename(columns = {'ID':'queried_ursi'}, inplace = True)
    nfb.rename(columns = {'VISIT':'Visit_ID'}, inplace = True)

    #add instrument id prefix to all columns besides ursi
    instr_id = get_instrumentID(nfb.columns[6], True)
    rename_col = {}
    for col in nfb.columns:
        if(col != 'queried_ursi' and instr_id not in col):
            rename_col[col] = instr_id+'_'+col;

    nfb.rename(columns = rename_col, inplace=True)
    
    #convert values in codebook values
    for r in range(len(nfb.index)):
        for c in nfb.columns:
            val = nfb.loc[r,c]
            nfb.loc[r,c] = convert_StringtoNumber(convertToCodebook(c, val)) 

    #Drop nan rows of dataframe
    nfb = nfb.drop(drop_NanRows(nfb))
    nfb = nfb.reset_index(drop=True)
    nfb = nfb.dropna(axis=1, how='all')
    
    #add to instrumentDF
    instrument_id = get_instrumentID(nfb.columns[6], False)
    
    #Drop nan rows
    nfb = nfb.drop(drop_NanRows(nfb))
    nfb['instrument'] = instrument_id
    
    if instrument_id in instrumentDF_dict.keys():
        instrumentDF_dict[instrument_id] = instrumentDF_dict[instrument_id].append(nfb)
    else:
        instrumentDF_dict[instrument_id] = nfb
           
def df_QuestionLabels(instrument_id):
    label_cols = {}
    df = instrumentDF_dict[instrument_id]
    for c in df.columns:
        label_cols[c] = df[c].iloc[-1]
    df = df.rename(index=str, columns=label_cols)
    df = df.drop(['0'])
    #df['queried_ursi'] = df.index
    return df
    
def compatibleJson(instrument_df):
    df = instrument_df.replace(np.nan, 'NaN', regex=True)
    df = df.replace('~<condSkipped>~', 'NaN', regex=True)
    df = df.replace('~<userSkipped>~', 'NaN', regex=True)
    nfb_dict = df.to_dict(orient='records')
    for row in nfb_dict:
        new_row = {}
        del_rows = []
        entry_type = row['instrument'].replace(" ", "_")
        entry_type = entry_type.replace("/", "_")
        entry_type = entry_type.replace(":", "_")
        entry_type = entry_type.replace("(", "-")
        entry_type = entry_type.replace(")", "-")
        entry_type = entry_type.replace(",", "_")
        new_row['entry_type'] = entry_type
        del_rows.append('instrument')
#        new_row['visit_id'] = row['visit_id']
#        del_rows.append('visit_id')
        new_row['name'] = 'sub-'+row['queried_ursi']+'_'+entry_type
        new_row['subject_id'] = row['queried_ursi']
        del_rows.append('queried_ursi')
#        for key in row.keys():
#            if 'SUB_STUDY_DAY_LAG' in key:
#                new_row['sub_study_day_lag'] = row[key]
#                del_rows.append(key)
#            elif '_DAY_LAG' in key:
#                new_row['day_lag'] = row[key]
#                del_rows.append(key)
#            elif '_SUB_STUDY' in key:
#                new_row['sub_study'] = row[key]
#                del_rows.append(key)
#            elif '_SUB_TYPE' in key:
#                new_row['sub_type'] = row[key]
#                del_rows.append(key)
                
        for item in del_rows:
            del row[item]
        new_row['metrics'] = row
        json_nfb.append(new_row)    

def dataframeToJson(instrument_df):
    df = instrument_df.replace(np.nan, 'NaN', regex=True)
    df = df.replace('~<condSkipped>~', 'NaN', regex=True)
    df = df.replace('~<userSkipped>~', 'NaN', regex=True)
    nfb_dict = df.to_dict(orient='records')   
    new_nfb=[] 
    for row in nfb_dict:
        new_row = {}
        del_rows = []
        entry_type = row['instrument'].replace(" ", "_")
        entry_type = entry_type.replace("/", "_")
        entry_type = entry_type.replace(":", "_")
        entry_type = entry_type.replace("(", "-")
        entry_type = entry_type.replace(")", "-")
        entry_type = entry_type.replace(",", "_")
        new_row['entry_type'] = entry_type
        del_rows.append('instrument')
        new_row['name'] = 'sub-'+row['queried_ursi']+'_'+entry_type
        new_row['subject_id'] = row['queried_ursi']
        del_rows.append('queried_ursi')
#        for key in row.keys():
#            if 'visit_id' == key:
#                new_row['visit_id'] = row['visit_id']
#                del_rows.append('visit_id')
#            if 'SUB_STUDY_DAY_LAG' in key:
#                new_row['sub_study_day_lag'] = row[key]
#                del_rows.append(key)
#            elif '_DAY_LAG' in key:
#                new_row['day_lag'] = row[key]
#                del_rows.append(key)
#            elif '_SUB_STUDY' in key:
#                new_row['sub_study'] = row[key]
#                del_rows.append(key)
#            elif '_SUB_TYPE' in key:
#                new_row['sub_type'] = row[key]
#                del_rows.append(key)

        for item in del_rows:
            del row[item]
        new_row['metrics'] = row
        new_nfb.append(new_row)
    return new_nfb

def add_Scans(file_name):
    nfb = pd.read_csv(file_name, low_memory=False, skipinitialspace=True)
    for i in range(len(nfb.index)):
        s = nfb.loc[i, 'Subject']
        nfb.loc[i, 'Subject'] = s[s.find('-')+1:s.find('_')]
    nfb.rename(columns = {'Subject':'queried_ursi'}, inplace = True)
    nfb.rename(columns = {'NumFD_greater_than_0.50':'NumFD_greater_than_1/2'}, inplace = True)
    nfb.rename(columns = {'PercentFD_greater_than_0.50':'PercentFD_greater_than_1/2'}, inplace = True)
    nfb['instrument'] = 'SCAN'
    instrumentDF_dict['SCAN'] = nfb
    
def create_settings_json():
    settings = {}
    public = {}
    public["startup_json"] = "http://www.json-generator.com/api/json/get/bUKySRRVnS?indent=2"
    public["metric_labels"] = "http://www.json-generator.com/api/json/get/bTwzelTuBe?indent=2"
    public["use_url_data"] = False
    public["load_if_empty"] = True
    public["use_custom"] = True
    public["needs_consent"] = False
    fields = [{
            "function_name": "get_qc_viewer",
            "id": "name",
            "name": "Subject"
          }]  
    
    modules = []
    for key in instrumentDF_dict.keys():
        mod = {}
        entry_type = instrumentDF_dict[key].iloc[1]['instrument'].replace(" ", "_")
        entry_type = entry_type.replace("/", "_")
        entry_type = entry_type.replace(":", "_")
        entry_type = entry_type.replace("(", "-")
        entry_type = entry_type.replace(")", "-")
        entry_type = entry_type.replace(",", "_")
        mod["name"] = entry_type
        mod["entry_type"] = entry_type
        mod["fields"] = fields
#        metric_names = list(instrumentDF_dict[key].columns)
#        metric_names.remove("queried_ursi")
#        if ("visit_id" in metric_names):
#            metric_names.remove("visit_id")
#        metric_names.remove("instrument")
#        mod["metric_names"] = metric_names
        mod["graph_type"] = "histogram"
        mod["staticURL"] = "https://dxugxjm290185.cloudfront.net/hbn/"
        mod["usePeerJS"] = False
        mod["logPainter"] = False
        mod["logContours"] = False
        mod["logPoints"] = True
        mod["qc_options"] = {}
        modules.append(mod)
    public["modules"] = modules
    settings["public"] = public
    #print(settings)
    with open("/Users/md35727/mindcontrol_kesh/test_settings.json", 'w') as f:
        json.dump(settings, f, indent=2)

codeBook_Dictionary()
#file_Clean('/Users/md35727/Projects/NKI_data/NeuroFB_part1.csv', False)
#file_Clean('/Users/md35727/Projects/NKI_data/NeuroFB_part2.csv', False)
#file_Clean('/Users/md35727/Projects/NKI_data/NeuroFB_part3.csv', False)
#file_Clean('/Users/md35727/Projects/NKI_data/NeuroFB_part4.csv', False)
#file_Clean('/Users/md35727/Projects/NKI_data/NeuroFB_part5.csv', False)
#file_Clean('/Users/md35727/Projects/NKI_data/NeuroFB_part6.csv', False)
#add_Scans('all_pow_params.csv')
#file_Clean('/Users/md35727/Projects/NKI_data/assessment_data/*.csv', True)
    

#for key in instrumentDF_dict.keys():
#    name_key = key.replace(" ", "_")
#    name_key = name_key.replace(":", "_")
#    name_key = name_key.replace("/", "_")
#    name_key = name_key.replace("(", "-")
#    name_key = name_key.replace(")", "-")
#    name_key = name_key.replace(",", "_")
#    with open(name_key+'.json', 'w') as f:
#        json.dump(dataframeToJson(instrumentDF_dict[key]), f, indent=2)
#        
#json_cb = {}
#for key in codebook.keys():
#    if('Question Label' in codebook[key].keys()): 
#        json_cb[key] = codebook[key]["Question Label"]
     
#create_settings_json()