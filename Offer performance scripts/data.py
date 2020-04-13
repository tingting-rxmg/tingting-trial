#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from pandas import ExcelWriter
import datetime
import os 
import sys
import methods


# In[2]:


pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 500)


# In[3]:


def border_msg(msg):
    row = len(msg)
    h = ''.join(['+'] + ['-' *row] + ['+'])
    result= h + '\n'"|"+msg+"|"'\n' + h
    print(result)


# In[ ]:


def offers(file):
    '''
    Read in raw offer sheet
    file - eg. "/Users/yanangao/Desktop/Irvine & Venice Combined Files/Offer Sheet by Advertiser.csv"
    Return: 
        df: processed offer sheet
    '''
    df = pd.read_csv(
        file,
        encoding='latin-1'
    )
    
    df = df.dropna(subset=['Hitpath Offer ID'])
    df['Hitpath Offer ID'] = df['Hitpath Offer ID'].astype(int).astype(str)
    return df


# In[ ]:


def ea(file):
    '''
    Read in Email Analyst file
    file - eg. "/Users/yanangao/Desktop/Irvine & Venice Combined Files/Dash - Master EA.xlsx"
    returns:
        df: processed EA file (with cleaned subject line, and regrouped by 'date_received', 'sender_domain', 'sub_clean')
    '''
    df = pd.DataFrame(pd.read_excel(file))

    # get subset of ibr data
    df = df.rename(columns = {'overall_inbox_placement':'overall_inbox_percent'})
    attr = ['date_received','sender_domain', 'subject', 'aol_inbox_percent', 'actual_aol_volume', 'google_inbox_percent', 'actual_google_volume',
           'yahoo_inbox_percent', 'actual_yahoo_volume', 'outlook_inbox_percent', 'actual_outlook_volume',
           'global_isps_inbox_percent', 'actual_global_isps_volume', 'actual_overall_volume','overall_inbox_percent', 'overall_weighted_inbox_placement']
    df = df[attr]

    # create columns 
    for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
        df['%s_inbox_percent' % j] = df['%s_inbox_percent' % j] / 100
        df['%s_action_volume' % j] = df['actual_%s_volume' % j] * df['%s_inbox_percent' % j]

    df['overall_weighted_inbox_placement'] = df['overall_weighted_inbox_placement']/100 # could be useless??

    # clean subject line
#     df['sub_clean'] = df['subject'].str.replace('[REDACTED] ', '',regex=False).str.replace(
#         '[REDACTED]', '',regex=False).str.rstrip(' !.?-').str.lstrip(' -,')
    
    df['sub_clean'] = df['subject'].str.replace('[REDACTED]', '',regex=False).str.replace(
        '[REDACTED]', '',regex=False).str.rstrip(' !.?-').str.lstrip(' -,')

#     # create and clean the sub_clean column's subject with functions imported from methods.py: Nicholas' regex function
#     df.insert(2, 'sub_clean', df['subject'])
#     for find, replace in Functions_lib.findReplaceDict.items():
#         Functions_lib.replaceCol(df['sub_clean'], find, replace)   
#     print(f'{len(df)} rows in EA file before combination')

    print("\n--------EA file report--------\n")
    print("EA rows before combination",df.shape)

    df = df.groupby(['date_received', 'sender_domain', 'sub_clean']).agg({'actual_aol_volume':'sum','actual_google_volume':'sum',
                                                                                'actual_yahoo_volume':'sum','actual_outlook_volume':'sum',
                                                                                'actual_global_isps_volume':'sum','actual_overall_volume':'sum',
                                                                                'aol_action_volume':'sum','google_action_volume':'sum',
                                                                                'yahoo_action_volume':'sum','outlook_action_volume':'sum',
                                                                                'global_isps_action_volume':'sum','overall_action_volume':'sum'}).reset_index(drop = False)

    for i in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
        df['%s_inbox_percent' % i] = df['%s_action_volume' % i] / df['actual_%s_volume' % i]

    # create index_i and format date
    df['date'] = pd.to_datetime(df['date_received'], format = '%m/%d/%Y')
    df['index_i'] = df.index
    print("EA rows after combination",df.shape)
    
    return df
    


# In[ ]:


# def ea(file):
#     '''
#     Read in Email Analyst file
#     file - eg. "/Users/yanangao/Desktop/Last 30 Days-5.22.19 -6.15.19/EA_5.22.19-6.15.19.csv"
#     '''
#     df = pd.DataFrame(pd.read_csv(file))

#     # get subset of ibr data
#     df = df.rename(columns = {'overall_inbox_placement':'overall_inbox_percent'})
#     attr = ['date_received','sender_domain', 'subject', 'aol_inbox_percent', 'actual_aol_volume', 'google_inbox_percent', 'actual_google_volume',
#            'yahoo_inbox_percent', 'actual_yahoo_volume', 'outlook_inbox_percent', 'actual_outlook_volume',
#            'global_isps_inbox_percent', 'actual_global_isps_volume', 'actual_overall_volume','overall_inbox_percent', 'overall_weighted_inbox_placement']
#     df = df[attr]

#     # create columns 
#     for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
#         df['%s_inbox_percent' % j] = df['%s_inbox_percent' % j] / 100
#         df['%s_action_volume' % j] = df['actual_%s_volume' % j] * df['%s_inbox_percent' % j]

#     df['overall_weighted_inbox_placement'] = df['overall_weighted_inbox_placement']/100 # could be useless??

#     # clean subject line
# #     df['sub_clean'] = df['subject'].str.replace('[REDACTED] ', '',regex=False).str.replace(
# #         '[REDACTED]', '',regex=False).str.rstrip(' !.?-').str.lstrip(' -,')
    
#     df['sub_clean'] = df['subject'].str.replace('[REDACTED]', '',regex=False).str.replace(
#         '[REDACTED]', '',regex=False).str.rstrip(' !.?-').str.lstrip(' -,')

# #     # create and clean the sub_clean column's subject with functions imported from methods.py: Nicholas' regex function
# #     df.insert(2, 'sub_clean', df['subject'])
# #     for find, replace in Functions_lib.findReplaceDict.items():
# #         Functions_lib.replaceCol(df['sub_clean'], find, replace)   
# #     print(f'{len(df)} rows in EA file before combination')

#     print("\n--------EA file report--------\n")
#     print("EA rows before combination",df.shape)

#     df = df.groupby(['date_received', 'sender_domain', 'sub_clean']).agg({'actual_aol_volume':'sum','actual_google_volume':'sum',
#                                                                                 'actual_yahoo_volume':'sum','actual_outlook_volume':'sum',
#                                                                                 'actual_global_isps_volume':'sum','actual_overall_volume':'sum',
#                                                                                 'aol_action_volume':'sum','google_action_volume':'sum',
#                                                                                 'yahoo_action_volume':'sum','outlook_action_volume':'sum',
#                                                                                 'global_isps_action_volume':'sum','overall_action_volume':'sum'}).reset_index(drop = False)

#     for i in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
#         df['%s_inbox_percent' % i] = df['%s_action_volume' % i] / df['actual_%s_volume' % i]

#     # create index_i and format date
#     df['date'] = pd.to_datetime(df['date_received'], format = '%m/%d/%Y')
#     df['index_i'] = df.index
#     print("EA rows after combination",df.shape)
    
#     return df
    


# In[11]:


def tailoredmail(file,domainNames):
    '''
    Read in TailoredMail raw file
    file - eg. "/Users/yanangao/Desktop/Irvine & Venice Combined Files/Dash - Master Tailored Mail.xlsx"
    returns:
        df: processed TM file (with cleaned subject line)
    '''
    df = pd.read_excel(file)
    df = df[[
        'messageid','emailssent','num_deliveries','num_unique_opens','num_unique_clicks','num_contact_loss',
        'delivery_rate','open_rate','click_rate','click_through_rate','contact_loss_rate',
    ]]
    df = df.rename(columns={
        'messageid':'Message',
        'emailssent':'Sent',
        'num_deliveries':'Delivered',
        'num_unique_opens':'Opens',
        'num_unique_clicks':'Clicks',
        'num_contact_loss':'Contacts Lost',
        'delivery_rate':'Delivery Rate',
        'open_rate':'Open Rate',
        'click_rate':'Click Rate',
        'click_through_rate':'Click Through Rate',
        'contact_loss_rate':'Contact Loss Rate'
    })

    header_list = [
        'Message','Last Edited','Sent','Delivered',
        'Delivery Rate','Opens','Open Rate','Clicks',
        'Click Rate','Click Through Rate','Contacts Lost','Contact Loss Rate'
    ]
    df = df.reindex(columns = header_list)

    # break message column
    df = methods.SubjectBreaker(df,domainNames)
    df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
    df['strmonth'] = df['date_brt'].dt.strftime('%B %-d, %Y')

    #get opener (only for format checking purpose, will delete later)
    df.insert(0, "openers", df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), allow_duplicates = True)


    #clean subject
    df['sub_clean'] = df['subject line'].str.replace("[firstname]’s", '').str.replace(
                                "[firstname]'s", '').str.replace("[firstname],", '').str.replace(
                                '[firstname]:', '').str.replace("[firstname]", '').str.replace(
                                '[firstname]', '').str.replace('[firstname]’s', '').str.replace('[lastname]', '').str.replace(
                                '[city]', '').str.replace('{{state}}', '').str.replace(
                                '[address1]', '').str.replace("{{state_abbrev}}", '').str.replace("{{date}}", '').str.lstrip(' -,').str.rstrip(' .?!-')


    df['sub_clean'] = df.apply(lambda x: x['sub_clean'].replace('[longdate]',x['strmonth']),axis =1)

    # add 1 day 
    df['date_plus_one'] = df.date_brt + datetime.timedelta(days = 1)

    #add index
    #     df['index_b'] = df.index

    # add ESP
    df['ESP']  = 'Tailored Mail'

    #     # add subaccount
    #     df['sub_account'] = np.nan

    print("\n--------Tailored Mail file report--------\n")
    print("# of Tailored Mail drops:",df.shape)
    return df


# In[ ]:


def iterable(file,domainNames):
    '''
    Read in Iterable file,
    file - eg. "/Users/yanangao/Desktop/Irvine & Venice Combined Files/Dash - Master Iterable.xlsx"
    Returns:
        df: processed Iterable file (with subject line cleaned, and column name renamed)
    '''
    df = pd.read_excel(file)
    
    # pre-processing of Iterable data
    df = df[[
        'name','send_size','total_emails_delivered','unique_emails_opens',
        'unique_email_clicks',
    ]]
    df = df.groupby(['name']).agg({
        'send_size':'sum',
        'total_emails_delivered':'sum',
        'unique_emails_opens':'sum',
        'unique_email_clicks':'sum'
    }).reset_index(drop = False)
    df = df.rename(columns={
        'name':'Message',
        'send_size':'Sent',
        'total_emails_delivered':'Delivered',
        'unique_emails_opens':'Opens',
        'unique_email_clicks':'Clicks'
    })
    df['Delivery Rate'] = df['Delivered']/df['Sent']
    df['Open Rate'] = df['Opens']/df['Delivered']
    df['Click Rate'] = df['Clicks']/df['Delivered']
    df['Click Through Rate'] = df['Clicks']/df['Delivered']
    header_list = [
        'Message','Last Edited','Sent','Delivered',
        'Delivery Rate','Opens','Open Rate','Clicks',
        'Click Rate','Click Through Rate','Contacts Lost','Contact Loss Rate'
    ]
    df = df.reindex(columns = header_list)

    # break message column
    df = methods.SubjectBreaker(df,domainNames)
    df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
    df['strmonth'] = df['date_brt'].dt.strftime('%B %-d, %Y')

    #get opener (only for format checking purpose, will delete later)
    df.insert(0, "openers", df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), allow_duplicates = True)


    #clean subject
    df['sub_clean'] = df['subject line'].str.replace("{{firstname}}’s", '').str.replace(
                                "{{firstname}}'s", '').str.replace("{{firstname}},", '').str.replace(
                                '{{firstname}}:', '').str.replace("{{firstname}}", '').str.replace(
                                '{{firstname}}', '').str.replace('{{firstname}}’s', '').str.replace('{{lastname}}', '').str.replace(
                                '{{city}}', '').str.replace('{{state}}', '').str.replace(
                                '{{address1}}', '').str.replace("{{state_abbrev}}", '').str.replace("{{date}}", '').str.lstrip(' -,').str.rstrip(' .?!-')


    df['sub_clean'] = df.apply(lambda x: x['sub_clean'].replace('{{now}}',x['strmonth']),axis =1)

    # add 1 day 
    df['date_plus_one'] = df.date_brt + datetime.timedelta(days = 1)

    #add index
#     df['index_b'] = df.index
    
    # add ESP
    df['ESP']  = 'Iterable'
    
#     # add subaccount
#     df['sub_account'] = np.nan
    
    print("\n--------Iterable file report--------\n")
    print("# of Iterable drops:",df.shape)
    
    
    return df


# In[ ]:


# def iterable(file,domainNames):
#     '''
#     Read in Iterable file,
#     file - eg. "/Users/yanangao/Desktop/Last 30 Days-5.22.19 -6.15.19/iterable_5.22.19-6.15.19.csv"
#     '''
#     df = pd.read_csv(file)
#     # pre-processing of Iterable data
#     df = df[[
#         'Campaign Name','Total Email Sends','Total Emails Delivered','Total Email Opens',
#         'Total Emails Clicked',
#     ]]

#     df = df.groupby(['Campaign Name']).agg({
#         'Total Email Sends':'sum',
#         'Total Emails Delivered':'sum',
#         'Total Email Opens':'sum',
#         'Total Emails Clicked':'sum'
#     }).reset_index(drop = False)

#     df = df.rename(columns={
#         'Campaign Name':'Message',
#         'Total Email Sends':'Sent',
#         'Total Emails Delivered':'Delivered',
#         'Total Email Opens':'Opens',
#         'Total Emails Clicked':'Clicks'
#     })

#     df['Delivery Rate'] = df['Delivered']/df['Sent']
#     df['Open Rate'] = df['Opens']/df['Delivered']
#     df['Click Rate'] = df['Clicks']/df['Delivered']
#     df['Click Through Rate'] = df['Clicks']/df['Delivered']

#     header_list = [
#         'Message','Last Edited','Sent','Delivered',
#         'Delivery Rate','Opens','Open Rate','Clicks',
#         'Click Rate','Click Through Rate','Contacts Lost','Contact Loss Rate'
#     ]
#     df = df.reindex(columns = header_list)

#     # break message column
#     df = methods.SubjectBreaker(df,domainNames)
#     df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
#     df['strmonth'] = df['date_brt'].dt.strftime('%B %-d, %Y')

#     #get opener (only for format checking purpose, will delete later)
#     df.insert(0, "openers", df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), allow_duplicates = True)


#     #clean subject
#     df['sub_clean'] = df['subject line'].str.replace("{{firstname}}’s", '').str.replace(
#                                 "{{firstname}}'s", '').str.replace("{{firstname}},", '').str.replace(
#                                 '{{firstname}}:', '').str.replace("{{firstname}}", '').str.replace(
#                                 '{{firstname}}', '').str.replace('{{firstname}}’s', '').str.replace('{{lastname}}', '').str.replace(
#                                 '{{city}}', '').str.replace('{{state}}', '').str.replace(
#                                 '{{address1}}', '').str.replace("{{state_abbrev}}", '').str.replace("{{date}}", '').str.lstrip(' -,').str.rstrip(' .?!-')


#     df['sub_clean'] = df.apply(lambda x: x['sub_clean'].replace('{{now}}',x['strmonth']),axis =1)

#     # add 1 day 
#     df['date_plus_one'] = df.date_brt + datetime.timedelta(days = 1)

#     #add index
# #     df['index_b'] = df.index
    
#     # add ESP
#     df['ESP']  = 'Iterable'
    
# #     # add subaccount
# #     df['sub_account'] = np.nan
    
#     print("\n--------Iterable file report--------\n")
#     print("# of Iterable drops:",df.shape)
    
    
#     return df


# In[ ]:


def bronto(file,domainNames):
    '''
    Read in bronto file
    file - eg. "/Users/yanangao/Desktop/Irvine & Venice Combined Files/Dash - Master Bronto.xlsx"
    Return:
        df: processed bronto file (with cleaned subject line)
    '''
    df = pd.read_excel(file)
    df = df[[
        'Message','Sent','Delivered','Delivery Rate','Opens','Open Rate','Clicks','Click Rate','Click Through Rate',
        'Contacts Lost','Contact Loss Rate',
    ]]
#     df = df[[
#         'message','sent','delivered','deliver_rate','opens','open_rate','clicks','click_rate','click_through-rate',
#         'contacts_lost','contact_loss_rate',
#     ]]
#     df = df.rename(columns = {
#         'message':'Message',
#         'sent':'Sent',
#         'delivered':'Delivered',
#         'deliver_rate':'Delivery Rate',
#         'Total Email Opens':'Opens',
#         'open_rate':'Open Rate',
#         'clicks':'Clicks',
#         'click_rate':'Click Rate',
#         'click_through-rate':'Click Through Rate',
#         'contacts_lost':'Contacts Lost',
#         'contact_loss_rate':'Contact Loss Rate'
#     })
    
    # break message column
    df = methods.SubjectBreaker(df,domainNames)

    #get datetime
    df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
    # print(df[df.date_brt.isna()][['Message']])
    df['strmonth'] = df['date_brt'].dt.strftime('%B %-d, %Y')

    #get opener (only for format checking purpose, will delete later)
    df.insert(0, "openers", df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), allow_duplicates = True)

    
    #clean subject
    df['sub_clean'] = df['subject line'].str.replace("%%firstname%%’s", '').str.replace(
                                "%%firstname%%'s", '').str.replace("%%firstname%%,", '').str.replace(
                                '%%firstname%%:', '').str.replace("%%firstname%%", '').str.replace(
                                '%%firstname%%', '').str.replace('%%lastname%%’s', '').str.replace('%%lastname%%', '').str.replace(
                                '%%city%%', '').str.replace('%%state%%', '').str.replace(
                                '%%address1%%', '').str.replace("%%state_abbrev%%", '').str.replace("%%date%%", '').str.lstrip(' -,').str.rstrip(' .?!-')
    
    #clean subject 
#     df['sub_clean'] = df['subject line'].str.replace("%%firstname%%’s ", '').str.replace(
#                                 "%%firstname%%'s ", '').str.replace("%%firstname%%, ", '').str.replace(
#                                 '%%firstname%%: ', '').str.replace("%%firstname%% ", '').str.replace(
#                                 '%%firstname%%', '').str.replace('%%lastname%%’s', '').str.replace('%%lastname%%', '').str.replace(
#                                 '%%city%% ', '').str.replace('%%city%%', '').str.replace('%%state%% ', '').str.replace(
#                                 '%%address1%%', '').str.replace("%%state_abbrev%%", '').str.replace("%%date%%", '').str.lstrip(' -,').str.rstrip(' .?!-')
    df['sub_clean'] = df.apply(lambda x: x['sub_clean'].replace('%%!date%%',x['strmonth']),axis =1)


    # add 1 day 
    df['date_plus_one'] = df.date_brt + datetime.timedelta(days = 1)

    #add index
#     df['index_b'] = df.index

    # add ESP
    df['ESP']  = 'Bronto'
    
#     # add sub account
#     df['sub_account'] = 
    
    print("\n--------bronto file report--------\n")
    # df.drop('openers', axis = 1, inplace = True)
    print("# of Bronto drops:",df.shape)
    
    border_msg('Check point I')
    floatCol = df[[
        'Sent','Delivered','Delivery Rate', 'Opens', 'Open Rate',
        'Clicks', 'Click Rate','Click Through Rate', 'Contacts Lost',
        'Contact Loss Rate',
    ]]

    obj_types = {col: set(map(type, floatCol[col])) for col in floatCol.select_dtypes(include=[object])}
    if len(obj_types) == 0:
        print("Passed Bronto column data type checking!")
    else:
        print("Strings found in Bronto float columns!!!")
        print(obj_types)
    
    return df


# In[ ]:


# def bronto(file,domainNames):
#     '''
#     Read in bronto file
#     file - eg. "/Users/yanangao/Desktop/Last 30 Days-5.22.19 -6.15.19/bronto_5.22.19-6.15.19.csv"
#     '''
#     df = pd.read_csv(file)
    
#     # create new open rate/click rate => to get more decimal places.
#     df.drop(['Open Rate','Click Rate','Click Through Rate'],axis=1,inplace=True)
#     df.insert(6,'Open Rate',df.Opens/df.Delivered)
#     df.insert(8,'Click Rate',df.Clicks/df.Opens)
#     df.insert(9,'Click Through Rate',df.Clicks/df.Delivered)

#     # break message column
#     df = methods.SubjectBreaker(df,domainNames)

#     #get datetime
#     df['date_brt'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
#     # print(df[df.date_brt.isna()][['Message']])
#     df['strmonth'] = df['date_brt'].dt.strftime('%B %-d, %Y')

#     #get opener (only for format checking purpose, will delete later)
#     df.insert(0, "openers", df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), allow_duplicates = True)

    
#     #clean subject
#     df['sub_clean'] = df['subject line'].str.replace("%%firstname%%’s", '').str.replace(
#                                 "%%firstname%%'s", '').str.replace("%%firstname%%,", '').str.replace(
#                                 '%%firstname%%:', '').str.replace("%%firstname%%", '').str.replace(
#                                 '%%firstname%%', '').str.replace('%%lastname%%’s', '').str.replace('%%lastname%%', '').str.replace(
#                                 '%%city%%', '').str.replace('%%state%%', '').str.replace(
#                                 '%%address1%%', '').str.replace("%%state_abbrev%%", '').str.replace("%%date%%", '').str.lstrip(' -,').str.rstrip(' .?!-')
    
#     #clean subject 
# #     df['sub_clean'] = df['subject line'].str.replace("%%firstname%%’s ", '').str.replace(
# #                                 "%%firstname%%'s ", '').str.replace("%%firstname%%, ", '').str.replace(
# #                                 '%%firstname%%: ', '').str.replace("%%firstname%% ", '').str.replace(
# #                                 '%%firstname%%', '').str.replace('%%lastname%%’s', '').str.replace('%%lastname%%', '').str.replace(
# #                                 '%%city%% ', '').str.replace('%%city%%', '').str.replace('%%state%% ', '').str.replace(
# #                                 '%%address1%%', '').str.replace("%%state_abbrev%%", '').str.replace("%%date%%", '').str.lstrip(' -,').str.rstrip(' .?!-')
#     df['sub_clean'] = df.apply(lambda x: x['sub_clean'].replace('%%!date%%',x['strmonth']),axis =1)
    
    
#     # df['sub_clean']=df['subject line']
#     # for find, replace in methods.findReplaceDict.items():
#     #     methods.replaceCol(df['sub_clean'],find,replace)


#     # add 1 day 
#     df['date_plus_one'] = df.date_brt + datetime.timedelta(days = 1)

#     #add index
# #     df['index_b'] = df.index

#     # add ESP
#     df['ESP']  = 'Bronto'
    
# #     # add sub account
# #     df['sub_account'] = 
    
#     print("\n--------bronto file report--------\n")
#     # df.drop('openers', axis = 1, inplace = True)
#     print("# of Bronto drops:",df.shape)
    
#     border_msg('Check point I')
#     floatCol = df[[
#         'Sent','Delivered','Delivery Rate', 'Opens', 'Open Rate',
#         'Clicks', 'Click Rate','Click Through Rate', 'Contacts Lost',
#         'Contact Loss Rate',
#     ]]

#     obj_types = {col: set(map(type, floatCol[col])) for col in floatCol.select_dtypes(include=[object])}
#     if len(obj_types) == 0:
#         print("Passed Bronto column data type checking!")
#     else:
#         print("Strings found in Bronto float columns!!!")
#         print(obj_types)
    
#     return df
    


# In[ ]:


def revenue(file,offers,domainNames):
    '''
    Read in revenue raw file from Outsource
    file - eg. '/Users/yanangao/Desktop/Irvine & Venice Combined Files/Master Revenue.csv'
    Return: 
        df: processed revenue file
    '''
    df = pd.read_csv(
        file, 
        encoding = 'latin-1'
    )
    df = df.dropna(subset = ['Message'])
    df = df.rename(columns = {'Campaign ID':'HitPath ID'})
    
    # replace string values (eg. #value?, no data, $revenue...) in revenue cols to nan
    df[[
        "Revenue", "RPC", "Revenue CPM (eCPM)","Conversions","Cost CPM","Cost per send","Net Revenue","Margin"
    ]] = df[[
        "Revenue", "RPC", "Revenue CPM (eCPM)","Conversions","Cost CPM","Cost per send","Net Revenue","Margin"
    ]].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    

    
    df = methods.SubjectBreaker(df,domainNames)

    df.insert(
        0, "data_provider", 
        df.rest_.str.split("_", expand = True)[0].str.rstrip(' .?!-').str.replace('AP','AP.I').str.replace('AP.I.I','AP.I').str.replace('LXCN','LXCN.PA').str.replace('LXCN.PA.PA','LXCN.PA').str.replace('I.CARDAP.IP','I.CARDAPP'), 
        allow_duplicates = True
    )
    df.insert(
        0, "openers", 
        df.rest_.str.split("_", expand = True)[1].str.rstrip(' .?!-').str.upper(), 
        allow_duplicates = True
    )
#     df.insert(
#         0, "",
        
#     )
    df['Date'] = pd.to_datetime(df.date_pt, format = '%m.%d.%y',errors='coerce')
    print("\n-------revenue file report---------\n")
    rowsBeforeMerging = len(df)

    # replace "offer" in revenue report with "offer name" in smartsheet
    offerCol = offers[['Hitpath Offer ID','Offer Name']]
    df = pd.merge(df,offerCol,how='left',left_on='Campaign ID',right_on='Hitpath Offer ID')
    df.drop(['Offer','Hitpath Offer ID'],axis=1,inplace=True)
    
    rowsAfterMerging = len(df)
    border_msg('Check point II')
    if rowsBeforeMerging != rowsAfterMerging:
        print("Duplicated HitPath IDs are found in Offer sheets!!!")
    else:
        print("Passed duplicated HitPath ID checking!")

    df['index_craig'] = df.index

    print("# of drops in Revenue report:",len(df))
    # Functions_lib.FormatChecker(df)
    # print(errorrev[['Message']])
    
    border_msg('Check point III')
    floatCol = df[[
        'Delivered', 'Opens', 'Open Rate',
        'Clicks', 'Click Rate','Revenue', 'RPC',
        'Revenue CPM (eCPM)', "Conversions", 'Cost CPM', 'Cost per send', 'Net Revenue',
        'Margin'
    ]]

    obj_types = {col: set(map(type, floatCol[col])) for col in floatCol.select_dtypes(include=[object])}
    if len(obj_types) == 0:
        print("Passed Revenue column data type checking!")
    else:
        print("Strings found in Revenue float columns!!!")
        print(obj_types)
        
#     border_msg("Check point IV")
    

#     df = df.drop('domain', axis = 1)
    return df


# In[ ]:


def mapping(file):
    '''
    Read in an email map tracking sheet
    df - eg. 'mapping - 12.21.2019.csv'
    '''
    df = pd.read_csv(file)
    df = df[~(df.Status=='INACTIVE')&~(df['DP.DS or DP.DV if multiple sources using samePubID'].isna())]
    df['From Domain ID'] = df['From Domain ID'].astype(int).astype(str)
    df['Revenue Pub ID'] = df['Revenue Pub ID'].astype(int).astype(str)
    df['DP.DS or DP.DV if multiple sources using samePubID'] = df['DP.DS or DP.DV if multiple sources using samePubID'].str.upper()

    
    ## create DP.DS.Domain.ESP.SA.PUBID list
    df['uniqueV2'] = df['DP.DS or DP.DV if multiple sources using samePubID']+"_"+df['From Domain ID']+"_"+df['ESP ID']+"_"+df['ESP Sub Account ID']+"_"+df['Revenue Pub ID']
    DPDSDomainESPSAPUBlist = list(df['uniqueV2'].unique())
    
    ## create DP.DS.Domain list
    df['unique'] = df['DP.DS or DP.DV if multiple sources using samePubID']+"_"+df['From Domain ID']
    DPDSDomainlist = list(df['unique'].unique())
    
    ## creat DP.DS list
    DPDSlist = list(df['DP.DS or DP.DV if multiple sources using samePubID'].unique())
    
    ## create domainNames dictionary
    domianNoDup = df[['From Domain','From Domain ID']].drop_duplicates(keep = 'first')
    domainNames = pd.Series(domianNoDup['From Domain'].values,index=domianNoDup['From Domain ID']).to_dict()
    
    return DPDSDomainESPSAPUBlist, DPDSDomainlist,DPDSlist,domainNames,df


# In[ ]:


# def mapping(file):
#     '''
#     Read in an email map tracking sheet
#     df - eg. 'mapping - 12.21.2019.csv'
#     '''
#     df = pd.read_csv(file)
#     df = df[~(df.Status=='INACTIVE')&~(df['DP.DS or DP.DV if multiple sources using samePubID'].isna())]
#     df['From Domain ID'] = df['From Domain ID'].astype(int).astype(str)
    
#     ## create venice DP.DS.Domain list
#     df['DP.DS or DP.DV if multiple sources using samePubID'] = df['DP.DS or DP.DV if multiple sources using samePubID'].str.upper()
#     df['unique'] = df['DP.DS or DP.DV if multiple sources using samePubID']+"_"+df['From Domain ID']
#     veniceList = list(df['unique'].unique())
#     # add irvine DP.DS.Domain list
#     irvineList = ['AP_1','SC.RF_2','SC.FHA_3','PMG.RF_2','PMG.DEBT_4','WC.RF_6',
#                  'UPSD.RF_2','LXCN_5','LPG.RF_7','LPG.FHA_8',]
#     DPDSDomainlist = veniceList + irvineList
    
#     ## create venice domainNames dictionary
#     domianNoDup = df[['From Domain','From Domain ID']].drop_duplicates(keep = 'first')
#     domainNames = pd.Series(domianNoDup['From Domain'].values,index=domianNoDup['From Domain ID']).to_dict()
#     # add irvine domains
#     IrvineDomainNames = {
#         "1":'apply-portal.net',
#         "2":'mortgage-assisting.com',
#         "3":'fha-guide.com',
#         "4":'app-portal.net',
#         "5":'thepaleo.net',
#         "6":'house-goals.com',
#         "7":'yourupdatereport.com',
#         "8":'thefhacapital.com',
#     }
#     domainNames.update(IrvineDomainNames)
    
#     return DPDSDomainlist,domainNames
    


# In[ ]:


def scheduling(file):
    '''
    Read in an scheduling sheet
    file - eg. 'scheduling - 12.21.2019.csv'
    '''
    df = pd.read_csv(file)
   
    # get date 
    df['Unique ID'] = df['Unique ID'].astype(str)
    df.insert(0,"Campaign ID",df["Unique ID"].str[0:4])
    df.insert(1,"date",df['Unique ID'].str[4:9])

    df.date = pd.to_numeric(df.date)
    df.insert(0,"Date",df.date.fillna(80000).astype(int).apply(lambda x: datetime(*xldate_as_tuple(x,0))))

    # get campaign id and data provider
    # df['Campaign ID']=df['Campaign ID'].astype(str)
    df[['data_provider', 'openers']] = df['Segment'].str.split('_', 1, expand = True)
    
    return df


# In[ ]:




