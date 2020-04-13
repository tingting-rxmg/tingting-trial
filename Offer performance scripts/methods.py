#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import datetime
from collections import OrderedDict
from address_suffixes import address_suffixes
import os
import string
from pandas import ExcelWriter
from xlsxwriter.utility import xl_col_to_name
import xlsxwriter


# In[ ]:


# Segment ID used in both new and old message name structures. 
SEGlist = [
    'OA','O150','O180','O90','O60','O45','O30','O21','O15','O14','O10','O7','O1','O5','O120','O3','O160','O31','O365',
    'C','C30','C15','C14','C7','C17','C21',
    'A90','A60','A30','A21','A15','A14','A7','A10',
    'M','M5','M5.O21','M5.O30','M4','M3.O21','M10.O30','M10.O21','M10','MI','MD','M1','M3','MI2',
    'TEST','T','T1',
    'CO30','CO1','CM','CO1',
    'W','WRMUP',
    'ACTIVE', 
    'NO30',
    'M5.O14','M3.O14',
]

# Drop ID used in old message name structure: add new ID into this list before running error check.
Droplist = [
    '0','1','2','3','4','5','6','7','8','9','10','11',
    '1A','1B','2A','2B','3A','3B','4A','4B','5A','5B','6A','6B','7A','7B','8A','8B','9A','9B','10A','10B','11A','11B',
    'M','M1','M2','M3','M4','TEST','TEST2','T1',
    'P','W','MI','MA','E','RD','OT','CT','I','PI','MG',
    'MI1','MI2',
]

# Drop ID used in new message name structure: add new ID into this list before running error check.
DroplistV2 = [
    'P','W','MI','MA','E','RD','OT','CT','I','PI','MG','ST','T',
]

# # Creative type used in new message name structure.
# creativelist = [
#     'HTML',
#     'CC',
#     'HF.HR.GN"Nepka 2, Nepka 1"4372135','HF.HR.FR"Nepka 2, Nepka 1"437287',
# ]

SUBJlist = []


# In[ ]:


# def getFolderNames(file):
#     '''
    
#     '''
#     path = os.path.dirname(file)
#     folder_name = os.path.basename(path)
#     fullTime = folder_name.split('-',1)[1]
#     endTime = folder_name.split('-')[2]
#     prefix = folder_name.split('-')[0]
#     return folder_name, fullTime, endTime, prefix


# In[ ]:


findReplaceDict = OrderedDict([
    # populuate this dictionary with find and replace values
    # use OrderedDict unless running Python 3.7+
    (r"^.*?[\._]\d_(.*?)_\d+_.*$",  r"\1"),  # get just the subject
    (r"%%.*?%%",  ""),  # remove template strings
    (r"^['|']s",  ""),  # beginning 's
    (r" ['|']s",  ""),  # hanging 's
    (r"'",  "'"),  # standard apostrophe
    (r"-",  ""),  # hyphen
    (r"^,",  ""),  # hanging commas
    (r"\.$",  ""),  # ending periods
    # remove formatted date
    (r"(?i)(January|February|March|April|May|June|July|August|September|October|November|December) \d+, \d+",  ""),
    (r"(?i)(January|February|March|April|May|June|July|August|September|October|November|December)( \d+)?(st|nd|rd|th)?",  ""),
    (f"(?i)[REDACTED] {'|'.join(address_suffixes.split())}",  ""),  # remove street suffix
    (r"\[REDACTED\]",  ""),  # remove [REDACTED]
    # remove street suffixes, add more between pipes
    (r"^\s+",  ""),  # beginning spaces
    (r"\s+$",  ""),  # trailing spaces
    (r"\s{2,}",  " "),  # remove multiple spaces, should be last run
])


def replaceCol(dfColumn, regex, value=r'\1'):
    '''
    Regex find and replace. Use `(?i)` flag in regex for case insensitivity, otherwise use pd.Series.str.replace
    dfColumn - the pd.DataFrame column for the regex replace
    regex - regular expression string
    value - "replace with" value; defaults to first capture group
    '''
    dfColumn.replace(
        regex=True,
        inplace=True,
        to_replace=regex,
        value=value
    )


# In[ ]:


def SubjectBreaker(bronto,domainNames):
    '''
    break message name down to get more features, eg: date, dropID, subject line, campaign ID,...
    '''
    bronto['Message'] = bronto['Message'].str.lstrip(' ').str.rstrip(' ')
    j = 1
    for i in ['date_pt','drop','subject line','Campaign ID','rest_']:
        bronto.insert(0, i, bronto.Message)
#         replaceCol(bronto[i], regex=r"(^.*?)_(\d+[\w]?)_(.*?)_(\d+)_(.*)$", value=r"\%s" % j)
#         replaceCol(bronto[i], regex=r"(^.*?)_(.*?)_(.*?)_(\d+)_(.*)$", value=r"\%s" % j)
        replaceCol(bronto[i], regex=r"(^.*?)_(.*?)_(.*?)(?=_\d{4})_(\d+)_(.*)$", value=r"\%s" % j)
        j = j+1 
    
    #get domain
    bronto['domain'] = bronto.rest_.str.split('_',3).str[2]
    bronto['domain'] = bronto.domain.astype('category').cat.rename_categories(domainNames)

    return bronto


# In[ ]:


def OfficeSplit(masterFile,domainHeaderName):
    '''
    Catogorize all the messages in ESPs to Irvine and Venice office
    Output: 
        IrvineFile: messages dropped by Irvine office
        VeniceFile: messages dropped by Venice office
    '''
    IrvineFile = masterFile[masterFile[f'{domainHeaderName}'].isin(
        [
          'apply-portal.net',
            'mortgage-assisting.com',
            'fha-guide.com',
            'app-portal.net',
            'thepaleo.net',
            'house-goals.com',
            'yourupdatereport.com',
            'thefhacapital.com',
        ]
    )]
#     df1[~df1.index.isin(df2.index)]
    VeniceFile = masterFile[~masterFile.index.isin(IrvineFile.index)]
    return IrvineFile,VeniceFile

def RevenueSplit(masterFile):
    '''
    Catogorize all the messages in revenue file to Irvine and Venice office
    Output: 
        IrvineRevFile: messages dropped by Irvine office
        VeniceRevFile: messages dropped by Venice office
    '''
    IrvineRevFile = masterFile[masterFile.data_provider.isin([
        'PMG.RF',
        'WC.RF',
        'UPSD.RF',
        'PMG.DEBT',
        'LXCN.PA',
        'LPG.RF',
        'LPG.FHA',
        'AP.I',
        'SC.RF',
        'SC.FHA'
    ])]
    VeniceRevFile = masterFile[~masterFile.index.isin(IrvineRevFile.index)]
    return IrvineRevFile, VeniceRevFile


# In[ ]:


def rest_Breaker(df):
    '''
    In SubjectBreaker function, we got one feature called rest_. This funtion further breaks rest_ down to more features
    Output: 
        df: dataframe with more features(cols). 
    '''
    df['esp'] = df['rest_'].str.split('_',7).str[3]
    df['sa'] = df['rest_'].str.split('_',7).str[4]
    df['pubid'] = df['rest_'].str.split('_',7).str[5]
    df['creativeType'] = df['rest_'].str.split('_',7).str[6]
    
    co = ['esp', 'sa', 'pubid', 'creativeType']
    df[co] = df[co].where(df['drop'].str.contains(".C"))
    df.esp = np.where(df.esp.isnull(), df.ESP, df.esp)
    df['Content ID'] = np.where(df['Content ID'].isnull(), df['creativeType'], df['Content ID'])
    return df


# In[ ]:


def namingErrorV2(df,DPDSDomainESPSAPUBlist,domainNames,eaDomains):
    '''
    Checks error for messages with NEW naming sturcture 
    (eg:12.7.19_P.1.S.1_Reconsolidate your debt now to save the most money_5030_PMG.DEBT_O30_4_BR_AP2_460641_HTML_22NOV19)
    Output: 
        errorDF: table of error report. Check "Irvine & Venice Combined Files Days Error Messages by Drops (new structure)" for example. 
    '''
    
    # 12.7.19_P.1.S.1_Reconsolidate your debt now to save the most money_5030_PMG.DEBT_O30_4_BR_AP2_460641_HTML_22NOV19
    
    df = df[['Message']]
    df = SubjectBreaker(df,domainNames)
    df['domain'] = df['domain'].astype(str)
    df['domain_ID'] = df.rest_.str.split('_',3).str[2]
    df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
    
    df['dropID'] = df['drop'].str.split('.',3).str[0]
    df['dropNumber'] = df['drop'].str.split('.',3).str[1]
    df['splitID'] = df['drop'].str.split('.',3).str[2]
    df['splitNumber'] = df['drop'].str.split('.',3).str[3]
    
#     df['dataset'] = df.rest_.str.split('_',7).str[0]
    df['dataset'] = df.rest_.str.split('_',7).str[0].str.rstrip(' .?!-').str.upper().str.replace('AP','AP.I').str.replace('AP.I.I','AP.I').str.replace('LXCN','LXCN.PA').str.replace('LXCN.PA.PA','LXCN.PA').str.replace('I.CARDAP.IP','I.CARDAPP')
    df['openers'] = df.rest_.str.split('_',7).str[1]
    df['esp'] = df.rest_.str.split('_',7).str[3]
    df['sa'] = df.rest_.str.split('_',7).str[4]
    df['pubid'] = df.rest_.str.split('_',7).str[5]
    df['creativeType'] = df.rest_.str.split('_',7).str[6]
    
    
#     df.insert(
#         0, "dataset", 
#         df.rest_.str.split("_", expand = True)[0].str.rstrip(' .?!-').str.upper().str.replace('AP','AP.I').str.replace('AP.I.I','AP.I').str.replace('LXCN','LXCN.PA').str.replace('LXCN.PA.PA','LXCN.PA').str.replace('I.CARDAP.IP','I.CARDAPP'), 
#         allow_duplicates = True
#     )
#     df.insert(
#         1, "openers", df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), allow_duplicates = True
#     )
    df.insert(
        0, "DP.DS_Domain_ESP_SA_PUBID",df.dataset+'_'+df.domain_ID+'_'+df.esp+'_'+df.sa+'_'+df.pubid
    )

    # create error message report
    df.is_copy = False
    df['date_error'] = np.where(
        df.date_brt.isnull(),
        'Invalid date format',
        ''
    )
    df['dropID_error'] = np.where(
        df['dropID'].isin(DroplistV2),
        '',
        'Invalid drop ID: '+df['dropID']
#         f"Invalid drop ID: {df['dropID']}"
    )
    df['dropNumber_error'] = np.where(
        df['dropNumber'].isin([str(x) for x in list(range(100))]),
        '',
        'Invalid drop number: '+df['dropNumber'].astype(str)
#         f"Invalid drop number: {df['dropNumber']}"
    )
    df['splitID_error'] = np.where(
        df['splitID'].isin(['C']),
        '',
        'Invalid split ID: '+df['splitID'].astype(str)
#         f"Invalid split ID: {df['splitID']}"
    )
    df['splitNumber_error'] = np.where(
        df['splitNumber'].isin([str(x) for x in list(range(100))]),
        '',
        'Invalid split number: '+df['splitNumber'].astype(str),
#         f"Invalid split number: {df['splitNumber']}"
    )
    df['campaignID_error'] = np.where(
        df['Campaign ID'].str.len() <= 4,
        '',
        'Invalid campaign ID: '+df['Campaign ID']
    ) # assume the length of campaign id is 4 digits
    
    df['domain_error'] = np.where(
        df.domain.isin(eaDomains),
        '',
        np.where(
            df.domain.isin(list(domainNames.values())),
            'Domain missing in EA:'+df.domain,
            'Invalid domain:'+df.domain
        )
    )
    df['DP.DS_Domain_ESP_SA_PUBID_error'] = np.where(
        df['DP.DS_Domain_ESP_SA_PUBID'].isin(DPDSDomainESPSAPUBlist),
        '',
        'Invalid DP.DS_Domain_ESP_SA_PUBID: '+df['DP.DS_Domain_ESP_SA_PUBID']
    )

    df['segment_error'] = np.where(
        df.openers.isin(SEGlist),
        '',
        'Invalid segment ID: '+df.openers
    )
#     df['creativeType_error'] = np.where(
#         df['creativeType'].isin(creativelist),
#         '',
#         'Invalid creativeType: '+df.creativeType.astype(str)
# #         f"Invalid creative type: {df['creativeType']}"
#     )
    df['pubid_matching_error'] = ''

    errorDF = df[[
        'Message','date_error','dropID_error','dropNumber_error','splitID_error','splitNumber_error',
        'campaignID_error','domain_error','DP.DS_Domain_ESP_SA_PUBID_error','segment_error','pubid_matching_error'
#         'creativeType_error',
    ]]
    errorChecker = errorDF.drop('Message',axis = 1)
    errorDF = errorDF[(errorChecker.values != '').any(1)]
    return errorDF    


# In[ ]:


def namingError(df,DPDSDomainlist,domainNames,eaDomains):
    '''
    Checks error for messages with OLD naming sturcture 
    (eg: 12.7.19_P1_Reconsolidate your debt now to save the most money_5030_PMG.DEBT_O30_4_22NOV19)
    Output: 
        errorDF: table of error report. Check "Irvine & Venice Combined Files Days Error Messages by Drops (old structure)" for example. 
    '''
    df = df[['Message']]
    df = SubjectBreaker(df,domainNames)
    df['domain'] = df['domain'].astype(str)
    df['domain_ID'] = df.rest_.str.split('_',3).str[2]
    df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
    # df[['dataset','openers']]=df.rest_.str.split('_',expand=True)[[0,1]]
    df.insert(
        0, "dataset", 
        df.rest_.str.split("_",1).str[0].str.rstrip(' .?!-').str.upper().str.replace('AP','AP.I').str.replace('AP.I.I','AP.I').str.replace('LXCN','LXCN.PA').str.replace('LXCN.PA.PA','LXCN.PA').str.replace('I.CARDAP.IP','I.CARDAPP'), 
        allow_duplicates = True
    )
    df.insert(
        1, "openers", 
        df.rest_.str.split("_",2).str[1].str.rstrip(' .?!-').str.upper(), 
        allow_duplicates = True
    )
    df.insert(
#         2, "DP.DS/DV_Domain",df.dataset+'_'+df.domain_ID.astype(int).astype(str)
        2, "DP.DS/DV_Domain",df.dataset+'_'+df.domain_ID
    )
    

#     df.insert(0, "DP", df.dataset.str.split(".", expand = True)[0].str.rstrip(' .?!-'), allow_duplicates = True)
#     df.insert(1, "DS", df.dataset.str.split(".", expand = True)[1].str.rstrip(' .?!-'), allow_duplicates = True)

    # create error message report
    df.is_copy = False
    df['date_error'] = np.where(
        df.date_brt.isnull(),
        'Invalid date format',
        ''
    )
    df['dropID_error'] = np.where(
        df['drop'].isin(Droplist),
        '',
        'Invalid drop ID: '+df['drop']
    )
    df['campaignID_error'] = np.where(
        df['Campaign ID'].str.len() <= 4,
        '',
        'Invalid campaign ID: '+df['Campaign ID']
    ) # assume the length of campaign id is 4 digits
    
    df['domain_error'] = np.where(
        df.domain.isin(eaDomains),
        '',
        np.where(
            df.domain.isin(list(domainNames.values())),
            'Domain missing in EA:'+df.domain,
            'Invalid domain:'+df.domain
        )
    )
#     df['domain_error'] = np.where(
#         df.domain.isin(list(domainNames.values())),
#         '',
#         'Invalid domain: '+df.domain.astype(str)
#     )
    df['DP.DS/DV_Domain'] = np.where(
        df['DP.DS/DV_Domain'].isin(DPDSDomainlist),
        '',
        'Invalid DP.DS/DV_Domain: '+df['DP.DS/DV_Domain']
    )

    df['segment_error'] = np.where(
        df.openers.isin(SEGlist),
        '',
        'Invalid segment ID: '+df.openers
    )
#     df['subject_error'] = np.where(
#         df['subject line'].isin(SUBJlist),
#         '',
#         'Invalid subject line'
#     )

    errorDF = df[['Message','date_error','dropID_error',
                    'campaignID_error','domain_error','DP.DS/DV_Domain','segment_error']]
    errorChecker = errorDF.drop('Message',axis = 1)
    errorDF = errorDF[(errorChecker.values != '').any(1)]
    return errorDF


# In[ ]:


def error_report(sender,revenue,fullTime,df_new, dup_full, missing_full,DPDSDomainlist,domainNames,eaDomains):
    """
    For messages with OLD naming structure, get dataframes created previously to create error message report, missing message report and print a brief summary.
    Output:
        errorBrt: error message report for Bronto
        errorIte: error message report for Iterable
        errorTM: error message report for TM
        errorrev: error message report for Revenue
        notInRev: messages that are missing from revenue raw file
        notInSender: messages that are missing from all ESP raw files
        dupSender: duplicate messages in ESP raw files
        dupRev: duplicate messages in revenue raw file
    """
    # split sender data by ESP
    Bronto = sender[sender.ESP == 'Bronto']
    Iterable = sender[sender.ESP == 'Iterable']
    TailoredMail = sender[sender.ESP == 'Tailored Mail']
    
    # Message naming error df
    errorBrt = namingError(Bronto, DPDSDomainlist, domainNames, eaDomains)
    errorIte = namingError(Iterable, DPDSDomainlist, domainNames, eaDomains)
    errorTM = namingError(TailoredMail, DPDSDomainlist, domainNames, eaDomains)
    errorrev = namingError(revenue,DPDSDomainlist,domainNames,eaDomains)
    
    # Missing messages df
    checkBrt = Bronto[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    checkIte = Iterable[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    checkTM = TailoredMail[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    
    checkSender = sender[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    checkRev = revenue[['Message','date_pt','rest_','drop','Campaign ID']]
    notInSender = pd.merge(checkRev,checkSender,how='left', on = ['date_pt','rest_','drop','Campaign ID'], indicator = True)
    notInSender = notInSender[notInSender._merge == 'left_only']

    notInRev = pd.merge(checkSender,checkRev,how='left', on = ['date_pt','rest_','drop','Campaign ID'], indicator = True)
    notInRev = notInRev[notInRev._merge == 'left_only']

    intersect = pd.merge(checkSender,checkRev,how='inner', on = ['date_pt','rest_','drop','Campaign ID'], indicator = True)
    
    # Duplicate message names 
    dupSender = checkSender.loc[checkSender.duplicated([
        'date_pt',
        'rest_',
        'drop',
        'Campaign ID'
    ], keep = False)]
    dupRev = checkRev.loc[checkRev.duplicated([
        'date_pt',
        'rest_',
        'drop',
        'Campaign ID'
    ], keep = False)]
    
    # brief summary of all types of errors.
    print("\n================ Report ===============\n")
    print("Date range:",fullTime)
    print("\n-------------------------------------\n")
    print("Total Bronto drops:",len(checkBrt))
    print("Total Iterable drops:",len(checkIte))
    print("Total Tailored Mail drops:",len(checkTM))
    print("Total Revenue drops:",len(checkRev))
    print("\n-------------------------------------\n")
    print("ESPs non-unique dops:",len(dupSender))
    print("Revenue non-unique dops:",len(dupRev))
    print("\n-------------------------------------\n")
    print("When matching ESPs with Revenue report:")
    print("Drops in ESPs not in Revenue file:",len(notInRev))
    print("Drops in Revenue not in ESPs file:",len(notInSender))
    print("Drops in both ESPs and Revenue files:", len(intersect))
    print("\n-------------------------------------\n")
    print("Error messages in Bronto file:",len(errorBrt))
    print("Error messages in Iterable file:",len(errorIte))
    print("Error messages in TM file:",len(errorTM))
    print("Error messages in Revenue file:",len(errorrev))
    print("\n-------------------------------------\n")
    print("When matching ESPs with EA:")
    print("# of one-to-one matches:",len(df_new))
    print("# of one-to-many matches:",len(dup_full.drop_duplicates('index_b',keep='first')))
    print("Drops in ESPs not in EA file:",len(missing_full))
    print("\n========== Files are attached ==========\n")
    
    return errorBrt, errorIte, errorTM, errorrev, notInRev, notInSender,dupSender, dupRev


# In[1]:


def error_reportV2(sender,revenue,fullTime,df_new, dup_full, missing_full,DPDSDomainESPSAPUBlist,domainNames,eaDomains):
    """
    For messages with NEW naming structure, get dataframes created previously to create error message report, missing message report and print a brief summary.
    Output:
        errorBrt: error message report for Bronto
        errorIte: error message report for Iterable
        errorTM: error message report for TM
        errorrev: error message report for Revenue
        notInRev: messages that are missing from revenue raw file
        notInSender: messages that are missing from all ESP raw files
        dupSender: duplicate messages in ESP raw files
        dupRev: duplicate messages in revenue raw file
    """
    # split sender data by ESP
    Bronto = sender[sender.ESP == 'Bronto']
    Iterable = sender[sender.ESP == 'Iterable']
    TailoredMail = sender[sender.ESP == 'Tailored Mail']
    
    # Message naming error df
    errorBrt = namingErrorV2(Bronto, DPDSDomainESPSAPUBlist, domainNames, eaDomains)
    errorIte = namingErrorV2(Iterable, DPDSDomainESPSAPUBlist, domainNames, eaDomains)
    errorTM = namingErrorV2(TailoredMail, DPDSDomainESPSAPUBlist, domainNames, eaDomains)
    errorrev = namingErrorV2(revenue,DPDSDomainESPSAPUBlist,domainNames,eaDomains)
    
    ## add an extra pubid check for revenue: there is a affiliate ID column in revenue raw file, here is to check if this affiliate ID in revenue is the same as the pubid in message name.
    revenue['pubid'] = revenue.rest_.str.split('_',7).str[5]
    revenue['pubid'] = pd.to_numeric(revenue['pubid'], errors = 'coerce')
    pubidCheckRev = revenue[[
        'Message','pubid','Affiliate ID'
    ]]
    pubidCheckRev['pubid_matching_error'] = np.where(
        pubidCheckRev['pubid'] == pubidCheckRev['Affiliate ID'],
        '',
        'pubid & affiliate id not matching'
    )
    
    columns_add = [
        'date_error','dropID_error','dropNumber_error','splitID_error','splitNumber_error',
        'campaignID_error','domain_error','DP.DS_Domain_ESP_SA_PUBID_error','segment_error',
    ]
    for col in columns_add:
        pubidCheckRev[f'{col}'] = ''
        
    pubidCheckRev = pubidCheckRev.reindex(columns = errorrev.columns)
    errorCheckerPubid = pubidCheckRev.drop('Message',axis = 1)
    pubidCheckRev = pubidCheckRev[(errorCheckerPubid.values != '').any(1)]
    ## combine two parts of revenue error reports to make the full rev error report.
    errorrev = pd.concat([errorrev,pubidCheckRev])
    
    
    # Missing messages df
    checkBrt = Bronto[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    checkIte = Iterable[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    checkTM = TailoredMail[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    
    checkSender = sender[['Message','date_pt','rest_','drop','Campaign ID','ESP']]
    checkRev = revenue[['Message','date_pt','rest_','drop','Campaign ID']]
    notInSender = pd.merge(checkRev,checkSender,how='left', on = ['date_pt','rest_','drop','Campaign ID'], indicator = True)
    notInSender = notInSender[notInSender._merge == 'left_only']

    notInRev = pd.merge(checkSender,checkRev,how='left', on = ['date_pt','rest_','drop','Campaign ID'], indicator = True)
    notInRev = notInRev[notInRev._merge == 'left_only']

    intersect = pd.merge(checkSender,checkRev,how='inner', on = ['date_pt','rest_','drop','Campaign ID'], indicator = True)
    
    # Duplicate message names 
    dupSender = checkSender.loc[checkSender.duplicated([
        'date_pt',
        'rest_',
        'drop',
        'Campaign ID'
    ], keep = False)]
    dupRev = checkRev.loc[checkRev.duplicated([
        'date_pt',
        'rest_',
        'drop',
        'Campaign ID'
    ], keep = False)]
    
    
    print("\n================ Report ===============\n")
    print("Date range:",fullTime)
    print("\n-------------------------------------\n")
    print("Total Bronto drops:",len(checkBrt))
    print("Total Iterable drops:",len(checkIte))
    print("Total Tailored Mail drops:",len(checkTM))
    print("Total Revenue drops:",len(checkRev))
    print("\n-------------------------------------\n")
    print("ESPs non-unique dops:",len(dupSender))
    print("Revenue non-unique dops:",len(dupRev))
    print("\n-------------------------------------\n")
    print("When matching ESPs with Revenue report:")
    print("Drops in ESPs not in Revenue file:",len(notInRev))
    print("Drops in Revenue not in ESPs file:",len(notInSender))
    print("Drops in both ESPs and Revenue files:", len(intersect))
    print("\n-------------------------------------\n")
    print("Error messages in Bronto file:",len(errorBrt))
    print("Error messages in Iterable file:",len(errorIte))
    print("Error messages in TM file:",len(errorTM))
    print("Error messages in Revenue file:",len(errorrev))
    print("\n-------------------------------------\n")
    print("When matching ESPs with EA:")
    print("# of one-to-one matches:",len(df_new))
    print("# of one-to-many matches:",len(dup_full.drop_duplicates('index_b',keep='first')))
    print("Drops in ESPs not in EA file:",len(missing_full))
    print("\n========== Files are attached ==========\n")
    
    return errorBrt, errorIte, errorTM, errorrev, notInRev, notInSender,dupSender, dupRev


# In[ ]:


def error_report_saver(folder_name,prefix,version,fullTime,errorBrt, errorIte, errorTM,errorrev,notInRev, notInSender,dupSender, dupRev, missing_full, dup_fullRxmg):
    '''
    Write error message report, missing message report to excel. 
    Output:
        Irvine & Venice Combined Files Days Error Messages by Drops (new structure).xlsx: new message name error report
        Irvine & Venice Combined Files Days Error Messages by Drops (old structure).xlsx: old message name error report
        Irvine & Venice Combined Files Missing Drops (new structure).xlsx: missing message report
        Irvine & Venice Combined Files Missing Drops (old structure).xlsx: missing message report
    '''
    writer = pd.ExcelWriter(f'/Users/tingting/Desktop/{folder_name}/{prefix} Days Error Messages by Drops {version}-{fullTime}.xlsx', engine = 'xlsxwriter')
    workbook = writer.book
    writer.sheets['Error Messages by Drop-Bronto'] = workbook.add_worksheet('Error Messages by Drop-Bronto')
    writer.sheets['Error Messages by Drop-Iterable'] = workbook.add_worksheet('Error Messages by Drop-Iterable')
    writer.sheets['Error Messages by Drop-TM'] = workbook.add_worksheet('Error Messages by Drop-TM')
    writer.sheets['Error Messages by Drop-Revenue'] = workbook.add_worksheet('Error Messages by Drop-Revenue')
    writer.sheets['Non-unique Messages-ESP'] = workbook.add_worksheet('Non-unique Messages-ESP')
    writer.sheets['Non-unique Messages-Revenue'] = workbook.add_worksheet('Non-unique Messages-Revenue')
    
    # add subaccount col
    errorBrt.to_excel(writer, sheet_name = 'Error Messages by Drop-Bronto',index = False)
    
    errorIte.to_excel(writer, sheet_name = 'Error Messages by Drop-Iterable',index = False)
    errorTM.to_excel(writer, sheet_name = 'Error Messages by Drop-TM',index = False)
    errorrev.to_excel(writer, sheet_name = 'Error Messages by Drop-Revenue',index = False)
    dupSender.to_excel(writer, sheet_name = 'Non-unique Messages-ESP',index = False)
    dupRev.to_excel(writer, sheet_name = 'Non-unique Messages-Revenue',index = False)
    writer.save()


    writer = pd.ExcelWriter(f'/Users/tingting/Desktop/{folder_name}/{prefix} Missing Drops {version}-{fullTime}.xlsx', engine = 'xlsxwriter')
    workbook = writer.book
    writer.sheets['Drops Missing in Revenue'] = workbook.add_worksheet('Drops Missing in Revenue')
    writer.sheets['Drops Missing in ESPs'] = workbook.add_worksheet('Drops Missing in ESPs')

    notInRev[['Message_x','ESP']].to_excel(writer, sheet_name = 'Drops Missing in Revenue',index = False)
    notInSender[['Message_x']].to_excel(writer, sheet_name = 'Drops Missing in ESPs',index = False)
#     missing_full[['Message']].to_excel(writer, sheet_name = 'Drops Missing in EA',index = False)
#     # add duplicate drops in EA
#     dup_fullRxmg[['Message']].to_excel(writer, sheet_name = 'Drops Duplicated in EA', index = False)
    
    
    writer.save()


# In[ ]:


def summary_missing_ea(missing_fullRxmg,dup_fullRxmg, brontoRxmg, DPDSDomainESPSAPUBlist, domainNames, eaDomains):
    '''
    Create a summary for messages don't have inboxing stats, at DP.DS/DV and segment level.
    Output: There are 4 stats for each table:
                All messages: all the messages that are missing inbox stats
                # of duplicates: missing because of duplicate matching 
                # of missing domains: missing because of missing EA domains
                # of missing messages: missing because there is no record in EA
            df2: the above 4 stats for each [DP.DS/DV, segment] combination
            df1: df2 further grouped by DP.DS/DV. 

        
    '''
    missing_fullRxmg['DP.DS'] = missing_fullRxmg['rest_'].str.split("_").str[0]
    missing_fullRxmg['Segment'] = missing_fullRxmg['rest_'].str.split("_").str[1]
    dup_fullRxmg['DP.DS'] = dup_fullRxmg['rest_'].str.split("_").str[0]
    dup_fullRxmg['Segment'] = dup_fullRxmg['rest_'].str.split("_").str[1]

    df = missing_fullRxmg[['Message','DP.DS','Segment']]
    dup = dup_fullRxmg[['Message','DP.DS','Segment']]
    dup['# of duplicates'] = 1
    dup['# of missing domains'] = 0

    eaDomainMiss = namingErrorV2(brontoRxmg, DPDSDomainESPSAPUBlist, domainNames, eaDomains)
    eaDomainMiss = eaDomainMiss[eaDomainMiss.domain_error != ''][['Message']]


    # missing drops not caused by domain missing
    dfmerge = pd.merge(
        df,
        eaDomainMiss,
        how = 'left',
        on = 'Message',
        indicator = True
    )
    #     dfleft = dfmerge[dfmerge._merge == 'left_only']
    dfmerge['# of duplicats'] = 0
    dfmerge['# of missing domains'] = dfmerge._merge.str.replace('left_only','0').str.replace('both','1')
    dfmerge['# of missing domains'] = dfmerge['# of missing domains'].astype(int)
    dfmerge.drop('_merge',axis = 1,inplace = True)


    dfMissingFull = pd.concat([dup,dfmerge])
    dfMissingFull = dfMissingFull.rename(columns = {'Message':'All messages'})

    df2 = dfMissingFull.groupby(['DP.DS','Segment']).agg({
        'All messages':'count',
        '# of duplicates':'sum',
        '# of missing domains':'sum'
    }).reset_index()
    df2['# of missing messages'] = df2['All messages'] - df2['# of duplicates'] - df2['# of missing domains']

    df1 = dfMissingFull.groupby(['DP.DS']).agg({
        'All messages':'count',
        '# of duplicates':'sum',
        '# of missing domains':'sum'
    }).reset_index()
    df1['Segment'] = 'All'
    df1['# of missing messages'] = df1['All messages'] - df1['# of duplicates'] - df1['# of missing domains']
    df1 = df1.reindex(columns=df2.columns).sort_values('All messages',ascending = False)
    return df1, df2


# In[ ]:


def ea_error_report(
    folder_name,prefix,
    missing_fullRxmg, dup_fullRxmg, brontoRxmg, 
    DPDSDomainESPSAPUBlist, domainNames, eaDomains):
    '''
    Write df1 and df2 into excel, format the excel file
    Output: EA missing drops error report.xlsx
    '''
    
    df1, df2 = summary_missing_ea(missing_fullRxmg,dup_fullRxmg, brontoRxmg, DPDSDomainESPSAPUBlist, domainNames, eaDomains)
    
    writer = pd.ExcelWriter(f'/Users/tingting/Desktop/{folder_name}/EA missing drops error report.xlsx', engine = 'xlsxwriter')
    workbook = writer.book
    
    
    # summary table
    yanan = workbook.add_worksheet('Missing EA summary')
    yanan.outline_settings(True,False,False,False)

    row=0
    col=0

    titles=df2.columns

    for i in titles:
        yanan.write(row,col,i)
        col+=1
    row+=1
    col=0
    for i,l in df1.iterrows():
        yanan.write(row,col,l[0])
        col+=1
        yanan.write(row,col,l[1])
        col+=1
        yanan.write(row,col,l[2])
        col+=1
        yanan.write(row,col,l[3])
        col+=1
        yanan.write(row,col,l[4])
        col+=1
        yanan.write(row,col,l[5])
        col=0
        row+=1
        for k,j in df2[df2['DP.DS']==l[0]].iterrows():
            yanan.write(row,col,j[0])
            col+=1
            yanan.write(row,col,j[1])
            col+=1
            yanan.write(row,col,j[2])
            col+=1
            yanan.write(row,col,j[3])
            col+=1
            yanan.write(row,col,j[4])
            col+=1
            yanan.write(row,col,j[5])
            yanan.set_row(row,None,None,{'level':1,'hidden':True,'collapsed':True})
            col=0
            row+=1
    
    # missing EA drop list
    missing_fullRxmg[['Message']].to_excel(writer, sheet_name = 'Drops Missing in EA',index = False)
    # duplicates EA drop list
    dup_fullRxmg[['Message']].to_excel(writer, sheet_name = 'Drops Duplicated in EA', index = False)
    workbook.close()


# In[ ]:


def combine_bronto_ea(bronto, ea):
    '''
    Inner join Bronto and Email Analyst DataFrames
    bronto - pandas.DataFrame processed by data.bronto
    ea - pandas.DataFrame processed by data.ea
    
    Output:
        df_new: ESP drops that are one to one matched with EA reports
        dup_full: ESP drops that have duplicate matches 
        missing_full: ESP drops that can't be matched with EA reports
    '''
    # add date plus one column
    bronto['date_plus_one'] = bronto.date_brt + datetime.timedelta(days = 1)
    
    # delivered on the same day
    combined_1 = bronto.merge(ea, how = 'inner', 
                                            left_on = ['date_brt','sub_clean', 'domain'],
                                            right_on = ['date','sub_clean', 'sender_domain'])
    
    # delivered on next day
    combined_2 = bronto.merge(ea, how = 'inner', 
                                            left_on = ['date_plus_one','sub_clean','domain'],
                                            right_on = ['date','sub_clean','sender_domain'])
    combined_full = pd.concat([combined_2, combined_1],ignore_index=True)

    
    #  get unmatched drops
    non_na = combined_2['index_b'].tolist() + combined_1['index_b'].tolist()
    complete_full = bronto.index.isin(non_na)
    missing_full = bronto[~complete_full]

      
    # get duplicated drops
    dup_brt = combined_full.loc[combined_full.duplicated('index_b', keep = False)]
    dup_ibr = combined_full.loc[combined_full.duplicated('index_i', keep = False)]

    dup_full = pd.concat([dup_brt, dup_ibr], ignore_index = True).drop_duplicates(keep = 'first')

    # get perfectly matched data without duplicated in any file
    df_new = pd.concat([combined_full,dup_full]).drop_duplicates(keep=False)
    df_new.drop('openers',axis=1,inplace=True)
    

    print("\n-------EA match Bronto report---------\n")
    print("# of duplicated drops in brt:", dup_full.drop_duplicates('index_b',keep='first').shape)
    
    print("# of good drops in brt:", df_new.shape)
    
    print("# of unmatches in brt:", missing_full.shape)
   

    if len(dup_full.drop_duplicates('index_b',keep='first')) + len(df_new) + len(missing_full) == len(bronto):
        print("Pass row number check of brt and merging tables")
    else: 
        print("Row number error of merging table")
    print("\n---------------------------------------\n")
        
    return df_new, dup_full, missing_full


# In[ ]:


def addRevenue(brontoRxmg, revenueRxmg, df_newRxmg):
    '''
    Notice:
        1. We use revenue file as the source of truth. Meaning that any messages that are in ESP files but not in revenue files will be eliminated from the output.
        2. ['sent','ccontact lost','contact loss rate','CTR'] are only in ESP dataframe; revenue stats are only in revenue file; inbox stats are only in EA file
        3. combine the 3 dataframes to get the master stats. 
        4. ['delivered','opens','clicks'] are in both revenue and esp files, but we want to use those in esp file, because those are more up to date.
    Output: 
        merged: a dataframe having all messages from revenue file and all stats.    
    '''
    # add contact loss to craig's reporting

    brontoRxmg = brontoRxmg.drop('domain',axis=1)
    # revenueRxmg = revenueRxmg.drop('subject line',axis=1)

    contact = brontoRxmg[[
        'date_pt', 'rest_', 'drop','Campaign ID',
        'Contacts Lost','Contact Loss Rate','Click Through Rate','Sent','ESP','subject line',
        'Delivered','Opens','Open Rate','Clicks','Click Rate'
    ]]
    
    
    # add inbox stats
    merged1 = pd.merge(
        contact, 
        df_newRxmg.drop([
            'ESP','domain','subject line','Delivered','Opens','Open Rate','Clicks','Click Rate'
        ],axis=1), 
        how = 'left', 
        on = ['date_pt', 'rest_', 'drop','Campaign ID'], 
#         indicator = True
    )
    
    # add revenue
    merged = pd.merge(
        revenueRxmg,
        merged1,
        how= 'left',
        on=['date_pt', 'rest_', 'drop','Campaign ID'],
        indicator = True
    ).drop(['index_craig'],axis = 1)
    
    #_x is from revenue, _y is from esp
    merged['Delivered_y'].fillna(merged['Delivered_x'],inplace = True)
    merged['Opens_y'].fillna(merged['Opens_x'],inplace = True)
    merged['Open Rate_y'].fillna(merged['Open Rate_x'],inplace = True)
    merged['Clicks_y'].fillna(merged['Clicks_x'],inplace = True)
    merged['Click Rate_y'].fillna(merged['Click Rate_x'],inplace = True)
    merged['subject line_y'].fillna(merged['subject line_x'],inplace = True)
    
    # _y is from esp; _x is from revenue
    merged = merged.rename(columns={
        'Contacts Lost_x':'Contacts Lost',
        'Contact Loss Rate_x':'Contact Loss Rate',
        'Click Through Rate_x':'Click Through Rate',
        'Sent_x':'Sent',
        'Delivered_y':'Delivered',
        'Opens_y':'Opens',
        'Open Rate_y':'Open Rate',
        'Clicks_y':'Clicks',
        'Click Rate_y':'Click Rate',
        'subject line_y':'subject line_esp',
        'subject line_x':'subject line_rev',
    })

    return merged


# In[1]:


def consolidated_report(combine):
    '''
    Input:
        combine: dataframe get from last function (addRevenue)
    Output:
        master: the same thing as the input table combine
        weekly_drops: select some features out of combine df, will be write into excel: raw_no_format_02.02.20 - 03.03.20.xlsx
        combine: select some features and format them as requsted, will be write into excel: Venice and Irvine Reporting - Consolidated Bronto SA Drop Stats_02.25.20-03.03.20.xlsx
    '''
    master = combine.copy()
    # add adjusted clicks and adjusted click rate
    combine['Adjusted Clicks'] = combine['Clicks'] - combine['Contacts Lost']
    combine['Adjusted Clicks Rate'] = combine['Adjusted Clicks']/combine['Opens']
    combine['ESP'] = combine['ESP'].replace(np.nan, "Missing")
    
    # ================== file without format (yanan use only)=====================
    attr = ['Message_x','Date','drop', 'Sent','Delivered', 'Opens', 'Open Rate', 'Clicks', 'Click Rate',
            'Contacts Lost','Contact Loss Rate','Adjusted Clicks','Adjusted Clicks Rate','Offer Name', 
            'Campaign ID', 'Link Used in Mailing', 'Affiliate ID', 'Sub ID','Content ID', #'Unnamed: 11', 
            'Revenue', 'RPC', 'Revenue CPM (eCPM)', 'Conversions', 'Cost CPM', 'Cost per send', 'Net Revenue', 
            'Margin', 'subject line_rev','subject line_esp', 'data_provider','openers' ,'aol_inbox_percent', 'actual_aol_volume', 
            'google_inbox_percent', 'actual_google_volume', 'yahoo_inbox_percent', 'actual_yahoo_volume',
            'outlook_inbox_percent', 'actual_outlook_volume', 'global_isps_inbox_percent',
            'actual_global_isps_volume', 'actual_overall_volume', 'overall_inbox_percent', 
            'aol_action_volume', 'google_action_volume', 'yahoo_action_volume', 
            'outlook_action_volume', 'global_isps_action_volume', 'overall_action_volume','ESP','domain','rest_']
    weekly_drops = combine[attr].rename(columns={'Message_x':'Message'})
    
    
    # ================= formatted file (google sheet 2020) ================================
#     combine["DP.DS/DV_PubID"] = combine['data_provider']+'_'+combine['data_provider']
    attr = ['Campaign ID','Date',  'data_provider', 'openers', 'Message_x', 'Sent', 'Delivered', 'Opens', 'Open Rate', 'Clicks',
        'Click Rate', 'Contacts Lost','Adjusted Clicks','Adjusted Clicks Rate','Offer Name', 'Campaign ID', 'Link Used in Mailing', 'Affiliate ID', 'Sub ID','Content ID', #'Unnamed: 11',
        'Revenue', 'RPC', 'Revenue CPM (eCPM)', 'Conversions', 'Cost CPM', 'Cost per send', 'Net Revenue', 
         'overall_inbox_percent', 'aol_inbox_percent','google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent',
        'global_isps_inbox_percent','Click Through Rate','drop','ESP','rest_']
    
    combine = combine[attr]
    combine = combine.rename(columns={
        'Message_x':'Message','Campaign ID':'Offer ID',
        #'Unnamed: 11':np.nan,
    })
    combine.insert(2, "Helper", np.nan)
    combine.insert(28,"Vertical", np.nan)
    combine.insert(29,"Network", np.nan)
    
    
    return combine, weekly_drops, master
    


# In[1]:


def addVertical(monthly_drops,mappingDF):
    '''
    Add source vertical and vertical ID (originally exist in EMIT) to master stats (called weekly_drops in script)
    Output:
        monthlydropsCredit: master stats with two more columns (vertical and vert ID) added
    '''
    creditDPDS = mappingDF[[
        'Vertical','Vertical ID','DP.DS or DP.DV if multiple sources using samePubID',
    ]].drop_duplicates(subset = 'DP.DS or DP.DV if multiple sources using samePubID', keep = 'first')

    monthlydropsCredit = monthly_drops.copy()
    monthlydropsCredit['data_provider'] = monthlydropsCredit['data_provider'].str.upper()
    monthlydropsCredit = pd.merge(
        monthlydropsCredit,
        creditDPDS,
        how = 'left',
        left_on = 'data_provider',
        right_on = 'DP.DS or DP.DV if multiple sources using samePubID',
        indicator = True
    )
    monthlydropsCredit.rename(columns = {'Vertical':'Source_Vertical'},inplace = True)
    return monthlydropsCredit


# In[ ]:


def add_col(smry1):
    smry1['Open rate'] = smry1['Opens']/smry1['Delivered']
    smry1['Click rate'] = smry1['Clicks']/smry1['Opens']
    smry1['Adjusted Click Rate'] = smry1['Adjusted Clicks']/smry1['Opens']
    smry1['EPC'] = smry1['Revenue']/smry1['Clicks']
    smry1['eCPM'] = smry1['Revenue']*1000/smry1['Delivered']
    smry1['% matched drops'] = smry1['number of matched']/smry1['number of total drops']
    for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
        smry1['%s_inbox_percent' % j] = smry1['%s_action_volume' % j] / smry1['actual_%s_volume' % j]
    return smry1


def row_style(row):
    if row['Offer Name'] == 'Total':
        return pd.Series('background-color: yellow', row.index)
    else:
        return pd.Series('', row.index)
    
    

def irvine_report_main(sub_drops, offers):
    '''
    Create offer performance reports
    Output:
        rk_offer: message stats grouped by campaign ID -> campaign ID level offer performance report
        s_ver_1: message stats grouped by campaign ID and Vertical -> Vertical level performance report
        third_final: a report combined campaign ID and vertical level report. vertical level report is used as a break down for campaignID level report
    Example: check "overall offer performance", "overall vertical performance" and "break down" in "Irvine - Offer & Vertical Performance by Vertical-02.02.20 - 03.03.20.xlsx"
    '''

    # aggregate stats for messages on campaign ID
    smry1 = sub_drops.groupby(['Campaign ID']).agg({'overall_inbox_percent':'count','Message':'count',
                                                    'Sent':'sum',
                                                   'Delivered':'sum','Opens':'sum','Clicks':'sum','Adjusted Clicks':'sum','Revenue':'sum',
                                                   'actual_aol_volume':'sum','actual_google_volume':'sum',
                                                   'actual_yahoo_volume':'sum','actual_outlook_volume':'sum',
                                                   'actual_global_isps_volume':'sum','actual_overall_volume':'sum',
                                                   'aol_action_volume':'sum','google_action_volume':'sum',
                                                   'yahoo_action_volume':'sum','outlook_action_volume':'sum',
                                                   'global_isps_action_volume':'sum','overall_action_volume':'sum',
                                                   'Date':'max',
                                                   }).reset_index(drop = False)

    smry1 = smry1.rename(columns={'overall_inbox_percent':'number of matched','Message':'number of total drops','Date':'Last seen'})
    smry1 = add_col(smry1)
    attr = ['Hitpath Offer ID', 'Offer Name', 'Vertical', 'Operational Status', 'Advertiser Name', 'Payout', 'Payout Type', 'Custom Creative Allowed','Budget','Cap', 'Day Restrictions','Additional Notes']
    smry2 = offers[attr]
    smry2['live']=np.where(
        smry2['Operational Status'] == 'Live',
        1,
        0
    )
    
    rk_offer = pd.merge(smry1, smry2, how = 'left', left_on = 'Campaign ID', right_on = 'Hitpath Offer ID')
    
    # further aggregate stats on Vertical 
    s_ver_1 = rk_offer.groupby(['Vertical']).agg({'number of matched':'sum', 'number of total drops':'sum','Sent':'sum',
                                                  'Delivered':'sum','Opens':'sum','Clicks':'sum','Adjusted Clicks':'sum','Revenue':'sum','Offer Name':'count','live':'sum',
                                                   'actual_aol_volume':'sum','actual_google_volume':'sum',
                                                   'actual_yahoo_volume':'sum','actual_outlook_volume':'sum',
                                                   'actual_global_isps_volume':'sum','actual_overall_volume':'sum',
                                                   'aol_action_volume':'sum','google_action_volume':'sum',
                                                   'yahoo_action_volume':'sum','outlook_action_volume':'sum',
                                                   'global_isps_action_volume':'sum','overall_action_volume':'sum',
                                                   'Last seen':'max',
                                                 }).reset_index(drop = False)
    s_ver_1 = add_col(s_ver_1)
    s_ver_1 = s_ver_1.rename(columns={'Offer Name':'Number of Offers', 'live':'Number of Live Offers'})
    att_rk = ['Hitpath Offer ID', 'Offer Name', 'Vertical', 'Operational Status', 'Advertiser Name', 'Payout', 'Payout Type','Custom Creative Allowed','Budget','Cap', 'Day Restrictions','Additional Notes',
            'Campaign ID', 'Sent','Delivered', 'Opens','Open rate', 'Clicks', 'Click rate','Adjusted Clicks','Adjusted Click Rate','Revenue',  'EPC', 'eCPM',
            'aol_inbox_percent', 'google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent', 'global_isps_inbox_percent', 'overall_inbox_percent',
             'number of matched', 'number of total drops','% matched drops','Last seen',]
    rk_offer = rk_offer[att_rk]
    att_sver = ['Vertical', 'Number of Offers', 'Number of Live Offers','Sent','Delivered', 'Opens','Open rate', 'Clicks', 'Click rate','Adjusted Clicks','Adjusted Click Rate','Revenue',  'EPC', 'eCPM',
               'aol_inbox_percent', 'google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent', 'global_isps_inbox_percent', 'overall_inbox_percent',
               'number of matched', 'number of total drops','% matched drops','Last seen',]
    s_ver_1 = s_ver_1[att_sver]
    
    # on the basis of previous two dataframes (rk_offer, s_ver_1), we combined these two tables together. Each row in s_ver_1 is a *vertical summary* of the rows in rk_offer that have the same vertical.
    header_list = list(rk_offer)
    add_cols = s_ver_1.reindex(columns = header_list)
    add_cols['Offer Name'] = 'Total'
    third = pd.concat([rk_offer, add_cols]).sort_values(by = ['Vertical', 'Hitpath Offer ID'])
    third_final = third.reset_index().drop('index', axis = 1).style.apply(row_style, axis=1) 
    return rk_offer, s_ver_1, third_final


# In[ ]:


def offerNotSeen(offers,rk_offer_full,rk_offer_oa,columns):
    '''
    Create the "offers not seen" tab in "Irvine - Offer & Vertical Performance by Vertical-02.02.20 - 03.03.20.xlsx" report
    Output:
        offersNotSeen: the dataframe shows the offers that have not been sent to this Office or vertical or segment yet.
    '''
    IrvVenMergeOffer = pd.merge(
        offers[[
            'Hitpath Offer ID','Offer Name','Vertical','Operational Status','Advertiser Name','Payout',
            'Payout Type','Cap','Day Restrictions','Additional Notes'
        ]],
        rk_offer_full,
        how = 'outer',
        on = [
            'Hitpath Offer ID','Offer Name','Vertical','Operational Status','Advertiser Name','Payout',
            'Payout Type','Cap','Day Restrictions','Additional Notes'
        ],
        indicator = True
    )
    IrvVenMergeOffer = IrvVenMergeOffer.reindex(columns = columns)
    
    offersNotSeen = pd.merge(
        IrvVenMergeOffer,
        rk_offer_oa[['Campaign ID']],
        how = 'left',
        on = 'Campaign ID', 
        indicator = True
    )
    offersNotSeen = offersNotSeen[offersNotSeen._merge == 'left_only'].drop('_merge', axis = 1).sort_values('eCPM', ascending = False)
    return offersNotSeen


# In[1]:


def generate_rxmg_report(writer,IrvVenWeeklyDrops,selected_drop,criteria,segments,offers):
    """
    Note:
        1. IrvVenWeeklyDrops: this df is only used to get "offer not seen" tab, stats in this tab is the combined stats of both offices.
        2. selected_drop: this df is used to produce all other tabs. Stats used in other tabs are Venice or Irvine office only.
    Function:
        This function saves the dataframes created by irvine_report_main to excel. 
    Output:
        Excel report: Irvine - Offer & Vertical Performance by Vertical-02.25.20-03.03.20.xlsx
    """
    # df needed forehand 
    rk_offer_full, s_ver_1_full, third_final_full = irvine_report_main(IrvVenWeeklyDrops, offers)
    columns = list(rk_offer_full.columns)
    
    # add offer and vertical performance sheet
    rk_offer_oa, s_ver_1_oa, third_final_oa = irvine_report_main(selected_drop, offers)
    rk_offer_oa.to_excel(writer, 'overall offer performance',index = False)
    s_ver_1_oa.to_excel(writer, 'overall vertical performance',index = False)
    third_final_oa.to_excel(writer, 'break down',index = False)
    
    # add "offers not seen" sheet
    not_seen = offerNotSeen(offers,rk_offer_full,rk_offer_oa,columns)
    not_seen.to_excel(writer, 'offers not seen',index = False)
    
    # format % and $ sign
    format(rk_offer_oa, writer, 'overall offer performance')
    format(s_ver_1_oa, writer, 'overall vertical performance')
    format(not_seen, writer, 'offers not seen')
    
    # break the report down by certain criteria (segment or vertical), and save to excel files.
    for seg in segments:
        sub_drops = selected_drop.loc[selected_drop[f'{criteria}'] == seg]
        rk_offer, s_ver_1, third_final = irvine_report_main(sub_drops, offers)
        rk_offer.to_excel(writer, f'{seg} offer performance',index = False)
        s_ver_1.to_excel(writer,  f'{seg} vert performance',index = False)
        third_final.to_excel(writer, f'{seg} break down',index = False)

        # offers haven't been sent
        not_seen = offerNotSeen(offers,rk_offer_full,rk_offer,columns)
        not_seen.to_excel(writer, f'{seg} offers not seen',index = False)
        
        # format % and $ sign
        format(rk_offer, writer, f'{seg} offer performance')
        format(s_ver_1, writer, f'{seg} vert performance')
        format(not_seen, writer, f'{seg} offers not seen')
                
    # add "consolidated stats" for this pubID
    selected_drop.to_excel(writer, 'consolidated stats', index = False)

    return writer


# In[ ]:



def set_column(df: 'dataframe', worksheet: 'a pd.Excelwriter sheet', cols: list, format: 'excel format to use', col_width: int = None) -> None:
    """ sets column by index, the column's position in the dataframe """
    idx = [df.columns.get_loc(c) for c in cols if c in df]

    for i in idx:
        # set the column width and format
        col = xl_col_to_name(i)
        worksheet.set_column(
            f'{col}:{col}',
            col_width,
            format)

def format(df: 'affiliate revenue stats dataframe', writer: str, sheetname: str, ) -> 'pd.ExcelWriter':
    '''
    formats affiliate revenue stats excel worksheet with currency and percentage formatting
    '''

    # get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets[sheetname]

    # add format cells written
    pct_fmt = workbook.add_format({
        'num_format': '0.0%',
    })

    money_fmt = workbook.add_format({
        'num_format': '$#,##0.00',
    })

    int_fmt = workbook.add_format({
        'num_format': '#,##0',
    })

    pct_cols = df.filter(regex='Rate|Margin|rate|percent|CLR')
    money_cols = df.filter(regex='Revenue|RPC|Cost|CPM|EPC')
    int_cols = df.filter(regex='Conversions')
    

    set_column(df, worksheet, pct_cols, pct_fmt)
    set_column(df, worksheet, money_cols, money_fmt)
    set_column(df, worksheet, int_cols, int_fmt)

    return writer


# In[ ]:


def subjectLinePerformance(weekly_drops,offers):
    """
    Create subject line performance report
    Output:
       groupMaster: check "subject line performance_COMBINED_02.25.20-03.03.20.xlsx" 
    """
    groupMaster = weekly_drops.groupby(['Campaign ID','subject line_rev']).agg({
        'Message':'count',
        'openers':lambda x:list(np.unique(x)),
        'Sent':'sum','Delivered':'sum','Opens':'sum','Clicks':'sum','Adjusted Clicks':'sum','Revenue':'sum',
        'actual_aol_volume':'sum','actual_google_volume':'sum',
        'actual_yahoo_volume':'sum','actual_outlook_volume':'sum',
        'actual_global_isps_volume':'sum','actual_overall_volume':'sum',
        'aol_action_volume':'sum','google_action_volume':'sum',
        'yahoo_action_volume':'sum','outlook_action_volume':'sum',
        'global_isps_action_volume':'sum','overall_action_volume':'sum',
        'Contacts Lost':'sum',
        'Date':'max',
    }).reset_index(drop = False)

    # re-calculate rate cols
    groupMaster['Open rate'] = groupMaster['Opens']/groupMaster['Delivered']
    groupMaster['Click rate'] = groupMaster['Clicks']/groupMaster['Opens']
    groupMaster['Adjusted Click Rate'] = groupMaster['Adjusted Clicks']/groupMaster['Opens']
    groupMaster['EPC'] = groupMaster['Revenue']/groupMaster['Clicks']
    groupMaster['eCPM'] = groupMaster['Revenue']*1000/groupMaster['Delivered']
    groupMaster['Contact loss rate'] = groupMaster['Contacts Lost']/groupMaster['Delivered']
    for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
        groupMaster['%s_inbox_percent' % j] = groupMaster['%s_action_volume' % j] / groupMaster['actual_%s_volume' % j]

    groupMaster=groupMaster.rename(columns={'Date':'Last seen','Message':'Number of drops','openers':'Segment List'})


    # add offer info
    groupMaster = pd.merge(groupMaster, offers[['Hitpath Offer ID','Offer Name','Vertical']],
                          how = 'left',
                          left_on = 'Campaign ID',
                          right_on = 'Hitpath Offer ID').drop('Hitpath Offer ID',axis = 1)
    
    # select cols to keep
    groupMaster=groupMaster[['Last seen','Campaign ID','Offer Name','Vertical','subject line_rev',
                             'Segment List','Number of drops','Sent','Delivered', 'Opens', 'Open rate','Clicks','Click rate',
                             'Contacts Lost',
                             'Contact loss rate',
                             'Adjusted Clicks',
                             'Adjusted Click Rate',
                             'Revenue','EPC', 'eCPM',
                             'google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent',
                             'global_isps_inbox_percent', 'overall_inbox_percent',
                            ]]
    return groupMaster


# In[ ]:


def productionAnalysis(weekly_drops,offers,folder_name,subfolder,prefix,fullTime,reportName):
    """
    Create production analysis report
    Output:
       Offer_Vertical Production Analysis-COMBINED-02.02.20 - 03.03.20.xlsx 
    """
    o30WeeklyDrops = weekly_drops[weekly_drops.openers == 'O30']
    o30weeklyDrops_offer, s_ver_1, third_final = irvine_report_main(o30WeeklyDrops, offers)

    o30writer = pd.ExcelWriter(f'/Users/tingting/Desktop/{folder_name}/{subfolder}/Offer_Vertical Production Analysis-{reportName}-{fullTime}.xlsx', engine = 'xlsxwriter')
    for vert in list(o30weeklyDrops_offer.Vertical.dropna().unique()):
        creditTable = o30weeklyDrops_offer[o30weeklyDrops_offer.Vertical == vert].sort_values(['Revenue'],ascending = False)
        creditRatio = creditTable[[
            'Hitpath Offer ID','Offer Name','Vertical','Operational Status','Advertiser Name','Payout',
            'Payout Type','Budget','Cap','Day Restrictions','Campaign ID',
        ]]

    #     print(vert)
        for colHeader in ['Delivered','Opens','Open rate','Clicks','Click rate','Adjusted Clicks','Adjusted Click Rate','Revenue','EPC','eCPM']:
            creditRatio[colHeader] = (creditTable[colHeader]-creditTable.iloc[0][colHeader])/creditTable.iloc[0][colHeader]

    #     print('done')   

        creditTable.to_excel(o30writer,sheet_name=f'{vert}',startrow=0 , startcol=0)   
        creditRatio.to_excel(o30writer,sheet_name=f'{vert}',startrow=len(creditTable)+3, startcol=0) 
        
        
    o30writer.save()


# In[1]:


def schedulebyContentID(weekly_dropsIrvine):
    """
    create content split report
    Output:
        schedulMaster: check "Content Split Report_02.02.20 - 03.03.20.xlsx"
    """
    weekly_dropsIrvine['Margin'] = weekly_dropsIrvine.Margin.replace('no data',np.nan).astype(float)
    scheduleMaster = weekly_dropsIrvine[weekly_dropsIrvine['Content ID']!='HTML']
    scheduleMaster = scheduleMaster[weekly_dropsIrvine['Content ID']!='CC']
    schedulMaster = scheduleMaster.groupby(['Campaign ID','Date','data_provider','openers','Content ID']).agg({
        'Message':'; '.join,
        'Sent':'sum',
        'Delivered':'sum',
        'Opens':'sum',
        'Clicks':'sum',
        'Contacts Lost':'sum',
        'Adjusted Clicks':'sum',
        'Offer Name':'max',
        'Link Used in Mailing':'max',
        'Affiliate ID':'max',
        'Sub ID':'; '.join, #errors when there are NA values
        'Revenue':'sum',
        'Cost CPM':'max',
        'Conversions': 'sum',
        'Cost per send':'sum',
        'Net Revenue':'sum',
        'Margin':'mean',
        'actual_aol_volume':'sum',
        'actual_google_volume':'sum',
        'actual_yahoo_volume':'sum',
        'actual_outlook_volume':'sum',
        'actual_global_isps_volume':'sum',
        'actual_overall_volume':'sum',
        'aol_action_volume':'sum',
        'google_action_volume':'sum',
        'yahoo_action_volume':'sum',
        'outlook_action_volume':'sum',
        'global_isps_action_volume':'sum',
        'overall_action_volume':'sum',
#         'esp':'; '.join,
#         'sa':'; '.join,
        'Affiliate ID':'first',

    }).reset_index(drop=False).sort_values(['Date'])

    schedulMaster['Open Rate'] = schedulMaster['Opens']/schedulMaster['Delivered']
    schedulMaster['Click Rate'] = schedulMaster['Clicks']/schedulMaster['Opens']
    schedulMaster['Adjusted Click Rate'] = schedulMaster['Adjusted Clicks']/schedulMaster['Opens']
    schedulMaster['RPC'] = schedulMaster['Revenue']/schedulMaster['Clicks']
    schedulMaster['Revenue CPM (eCPM)'] = schedulMaster['Revenue']*1000/schedulMaster['Delivered']
    schedulMaster['CLR'] = schedulMaster['Contacts Lost']/schedulMaster['Opens']
    for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
        schedulMaster['%s_inbox_percent' % j] = schedulMaster['%s_action_volume' % j] / schedulMaster['actual_%s_volume' % j]

    attr = ['Campaign ID','Date',  'data_provider', 'openers', 'Message','Sent', 'Delivered', 'Opens', 'Open Rate', 'Clicks',
            'Click Rate', 'Contacts Lost','CLR','Adjusted Clicks','Adjusted Click Rate','Offer Name', 'Campaign ID', 'Link Used in Mailing', 'Affiliate ID', 'Sub ID',
            'Revenue', 'RPC', 'Revenue CPM (eCPM)', 'Conversions', 'Cost CPM', 'Cost per send', 'Net Revenue', 'Margin',
             'overall_inbox_percent', 'aol_inbox_percent','google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent',
            'global_isps_inbox_percent',
#             'esp',
#             'sa',
            'Affiliate ID',
            'Content ID']

    schedulMaster = schedulMaster[attr]
    schedulMaster = schedulMaster.rename(columns={'Campaign ID':'Offer ID'})
    schedulMaster.insert(2, "Helper", np.nan)
#     schedulMaster.insert(16,"Sub ID",np.nan)
    schedulMaster.insert(21,"Unnamed: 15",np.nan)
    schedulMaster.insert(30,"Vertical", np.nan)
    schedulMaster.insert(31,"Network", np.nan)
    
    return schedulMaster


# In[ ]:


# def schedulingmaster(weekly_dropsIrvine):
#     """
#     Combine stats for one split message drop in Bronto and Iterable. 
#     """
#     weekly_dropsIrvine['Margin'] = weekly_dropsIrvine.Margin.replace('no data',np.nan).astype(float)
#     schedulMaster = weekly_dropsIrvine.groupby(['Campaign ID','Date','data_provider','openers']).agg({
#         'Message':'; '.join,
#         'Sent':'sum',
#         'Delivered':'sum',
#         'Opens':'sum',
#         'Clicks':'sum',
#         'Contacts Lost':'sum',
#         'Adjusted Clicks':'sum',
#         'Offer Name':'max',
#     #     'Campaign ID':'max',
#         'Link Used in Mailing':'max',
#         'Affiliate ID':'max',
#         'Sub ID':'; '.join, #errors!!!!?????
#         'Revenue':'sum',
#         'Cost CPM':'max',
#         'Cost per send':'sum',
#         'Net Revenue':'sum',
#         'Margin':'mean',
#         'actual_aol_volume':'sum',
#         'actual_google_volume':'sum',
#         'actual_yahoo_volume':'sum',
#         'actual_outlook_volume':'sum',
#         'actual_global_isps_volume':'sum',
#         'actual_overall_volume':'sum',
#         'aol_action_volume':'sum',
#         'google_action_volume':'sum',
#         'yahoo_action_volume':'sum',
#         'outlook_action_volume':'sum',
#         'global_isps_action_volume':'sum',
#         'overall_action_volume':'sum',
#         'drop':'; '.join,
#         'ESP':';' .join,

#     }).reset_index(drop=False)

#     schedulMaster['Open Rate'] = schedulMaster['Opens']/schedulMaster['Delivered']
#     schedulMaster['Click Rate'] = schedulMaster['Clicks']/schedulMaster['Opens']
#     schedulMaster['Adjusted Click Rate'] = schedulMaster['Adjusted Clicks']/schedulMaster['Opens']
#     schedulMaster['RPC'] = schedulMaster['Revenue']/schedulMaster['Clicks']
#     schedulMaster['Revenue CPM (eCPM)'] = schedulMaster['Revenue']*1000/schedulMaster['Delivered']
#     for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
#         schedulMaster['%s_inbox_percent' % j] = schedulMaster['%s_action_volume' % j] / schedulMaster['actual_%s_volume' % j]

#     attr = ['Campaign ID','Date',  'data_provider', 'openers', 'Message','Sent', 'Delivered', 'Opens', 'Open Rate', 'Clicks',
#             'Click Rate', 'Contacts Lost','Adjusted Clicks','Adjusted Click Rate','Offer Name', 'Campaign ID', 'Link Used in Mailing', 'Affiliate ID', 'Sub ID',
#             'Revenue', 'RPC', 'Revenue CPM (eCPM)', 'Cost CPM', 'Cost per send', 'Net Revenue', 'Margin',
#              'overall_inbox_percent', 'aol_inbox_percent','google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent',
#             'global_isps_inbox_percent','drop','ESP']

#     schedulMaster = schedulMaster[attr]
#     schedulMaster = schedulMaster.rename(columns={'Campaign ID':'Offer ID'})
#     schedulMaster.insert(2, "Helper", np.nan)
# #     schedulMaster.insert(16,"Sub ID",np.nan)
#     schedulMaster.insert(20,"Unnamed: 15",np.nan)
#     schedulMaster.insert(28,"Vertical", np.nan)
#     schedulMaster.insert(29,"Network", np.nan)
    
#     return schedulMaster


# In[ ]:

# def revenueSummation(full_drops, offers):
    
#     # revenue summary campaign id level
#     full_drops['week_number'] = full_drops.Date.dt.week
#     revSumDF = full_drops.groupby(['Campaign ID','week_number']).aggregate({'Revenue':'sum'}).reset_index(drop = False)
#     revSumIDview = revSumDF.pivot_table(
#         'Revenue',index = 'Campaign ID',columns='week_number',
#         aggfunc=np.sum, margins=True
#     )
    
#     # revenue summary advertiser level
#     offersAdv = offers[['Hitpath Offer ID','Advertiser Name']]
#     fullDropsAddMonthAdv = pd.merge(
#         full_drops, offersAdv,
#         how = 'left',
#         left_on = 'Campaign ID',
#         right_on = 'Hitpath Offer ID'
#     )
#     fullDropsAddMonthAdv['month'] = fullDropsAddMonthAdv.Date.dt.month
#     revSumAdvView = fullDropsAddMonthAdv.groupby(['month','week_number','Advertiser Name']).aggregate({'Revenue':'sum'})
#     revSumAdvView = revSumAdvView.pivot_table(
#         'Revenue',
#         index = ['month','week_number'],
#         columns = 'Advertiser Name'
#     )
#     return revSumIDview,revSumAdvView


# In[ ]:


def offerSummaryByDatasets(full_drops, offers,folder_nameIrvine):
    '''
    create Offer Summary Stats By Dataset report
    Output: check Offer Summary Stats By Dataset.xlsx
    '''
    o30Full = full_drops[full_drops.openers == 'O30']
    masterAddVertical = pd.merge(offers[['Hitpath Offer ID','Vertical','Payout']],o30Full,how='left', 
                                 left_on = 'Hitpath Offer ID', right_on = 'Campaign ID')
    pivotTable = ExcelWriter(f'/Users/tingting/Desktop/{folder_nameIrvine}/Irvine Reports/Offer Summary Stats By Dataset.xlsx')

    for data in list(masterAddVertical.data_provider.dropna().unique()):
        subDrops = masterAddVertical[masterAddVertical.data_provider == data]
        summaryStats = subDrops.assign(
            result1 = np.where(subDrops['Contacts Lost']>0, subDrops.Delivered, 0)
        ).groupby(['Campaign ID','Vertical','Payout'], as_index = False).agg({
            'Sent':'sum',
            'Delivered':'sum',
            'Revenue':'sum',
            'Opens':'sum',
            'Open Rate':['mean','std'],
            'Clicks':'sum',
            'Click Rate':['mean','std'],
            'Adjusted Clicks':'sum',
            'Adjusted Click Rate':['mean','std'],
            'result1':'sum',
            'Contacts Lost':'sum',
            'Message':'count'
        })


        # rename column names
        d = {
            'Sentsum':'Total Sent',
            'Deliveredsum':'Total Delivered',
            'Revenuesum':'Total Revenue',
            'Openssum':'Sum Opens',
            'Open Ratemean':'Average Open Rate by Offer',
            'Open Ratestd':'StdDev Open Rate',
            'Clickssum':'Total Production Clicks',
            'Click Ratemean':'Average Click Rate by offer',
            'Click Ratestd':'StdDev Click Rate by Offer',
            'Adjusted Clickssum':'Total Adjusted Clicks',
            'Adjusted Click Ratemean':'Average Adjusted Click Rate by offer',
            'Adjusted Click Ratestd':'StdDev Adjusted Click Rate by Offer',
            'result1sum':'Sent (Contacts Lost Purposes)',
            'Contacts Lostsum':'Sum Contacts Lost',
            'Messagecount':'Total Drops',   
        }
        summaryStats.columns = summaryStats.columns.map(''.join)
        summaryStats = summaryStats.reset_index().rename(columns=d)


        # add calculated columns
        summaryStats['Calculated Avg Historical eCPM AP'] = summaryStats['Total Revenue']*1000/summaryStats['Total Delivered']
        summaryStats['Contact Lost Rate'] = summaryStats['Sum Contacts Lost']/summaryStats['Sent (Contacts Lost Purposes)']
        summaryStats['EPC'] = summaryStats['Total Revenue']/summaryStats['Total Production Clicks']
        summaryStats['Click Through Rate'] = summaryStats['Average Open Rate by Offer']*summaryStats['Average Click Rate by offer']

        # rearrange columns
        header_list = [
            'Campaign ID', 'Vertical', 'Total Sent', 'Total Delivered','Total Revenue', 
            'Sum Opens', 'Average Open Rate by Offer', 'StdDev Open Rate',
            'Average Click Rate by offer', 'Total Production Clicks', 'StdDev Click Rate by Offer', 
            'Total Adjusted Clicks','Average Adjusted Click Rate by offer','StdDev Adjusted Click Rate by Offer',
            'Median Offer eCPM StdDev', 'Calculated Avg Historical eCPM AP',
            'Sent (Contacts Lost Purposes)', 'Sum Contacts Lost',
            'Contact Lost Rate', 'EPC', 'Click Through Rate', 'Conversions','Total Drops','Payout',
        ]
        summaryStats = summaryStats.reindex(columns = header_list)

        # data type and formatting
        summaryStats['Campaign ID'] = summaryStats['Campaign ID'].astype(int)
        
        #save in excel file
        summaryStats.to_excel(pivotTable,f'{data}', index = False)
        
        format(summaryStats,pivotTable,f'{data}')
    pivotTable.save()
    
    


# In[ ]:


# def GroupBySubaccount(weekly_drops):
#     '''
    
#     '''
#     weeklyDropsSubUse = weekly_drops.copy()
#     weeklyDropsSubUse = weeklyDropsSubUse[weeklyDropsSubUse.Date >= '2019-08-19']
#     weeklyDropsSubUse['ESP'] = weeklyDropsSubUse['Sub ID'].str.split("_",expand = True)[0]
#     weeklyDropsSubUse['Sub_account'] = weeklyDropsSubUse['Sub ID'].str.split("_",expand = True)[1]
#     weeklyDropsSubUse['ESP_SA'] = weeklyDropsSubUse[['ESP','Sub_account']].apply(lambda x:'_'.join(x),axis = 1)

#     # weeklyDropsSubUse

#     subaccountWise = weeklyDropsSubUse.groupby(['ESP','Sub_account'], as_index = False).agg({
#         'Message':'count',
#         'Sent':'sum',
#         'Delivered':'sum',
#         'Opens':'sum',
#         'Clicks':'sum',
#         'Adjusted Clicks':'sum',
#         'Revenue':'sum',
#     })
#     subaccountWise['Open_rate'] = subaccountWise['Opens']/subaccountWise['Delivered']
#     subaccountWise['Click_rate'] = subaccountWise['Clicks']/subaccountWise['Opens']
#     subaccountWise['Adjusted Clicks_rate'] = subaccountWise['Adjusted Clicks']/subaccountWise['Opens']
#     subaccountWise['ECPM'] = subaccountWise['Revenue']*1000/subaccountWise['Delivered']
#     subaccountWise['EPC'] = subaccountWise['Revenue']/subaccountWise['Clicks']

#     subaccountWise = subaccountWise.rename(columns = {'Message':'# Drops'})
#     colHeaders = ['ESP','Sub_account','# Drops','Sent','Delivered','Opens','Open_rate','Clicks','Click_rate','Adjusted Clicks','Adjusted Clicks_rate','Revenue','ECPM','EPC','Notes']
#     subaccountWise = subaccountWise.reindex(columns = colHeaders)
#     return weeklyDropsSubUse, subaccountWise

