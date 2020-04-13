#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '1')
get_ipython().run_line_magic('aimport', 'data')
get_ipython().run_line_magic('aimport', 'methods')

import warnings
from functools import wraps
import pandas as pd
import numpy as np
import os
import string
from pandas import ExcelWriter
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
import datetime
import glob
import sys

warnings.filterwarnings('ignore')
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 500)


# ### Pt I. Error checking

# In[2]:


DATADIR = '/Users/yanangao/Desktop/Irvine & Venice Combined Files'
DPDSDomainESPSAPUBlist, DPDSDomainlist,DPDSlist,domainNames,mappingDF = data.mapping(
    os.path.join(
        DATADIR,
        'Email Identifier Mapping Tracker.csv',
    )
)

offers = data.offers(
    os.path.join(
        DATADIR,
        'Offer Sheet by Advertiser.csv'
    )
)

brontoRxmg = data.bronto(
    os.path.join(
        DATADIR,
        'Dash - Master Bronto.xlsx'
    ),
    domainNames
)

iterableRxmg = data.iterable(
    os.path.join(
        DATADIR,
        'Dash - Master Iterable.xlsx'
    ),
    domainNames
)

tailormailRxmg = data.tailoredmail(
    os.path.join(
        DATADIR,
        'Dash - Master Tailored Mail.xlsx'
    ),
    domainNames
)

eaRxmg = data.ea(
    os.path.join(
        DATADIR,
        'Dash - Master EA.xlsx'
    ),
)

revenueRxmg = data.revenue(
    os.path.join(
        DATADIR,
        'Master Revenue.csv'
    ),
    offers,
    domainNames
)

orgRxmg = pd.read_csv(
    os.path.join(
        DATADIR,
        'Master Revenue.csv'
    ),
    encoding = 'latin-1'
)

# Combine ESP: Bronto & Iterable
brontoRxmg = pd.concat([brontoRxmg,
                        iterableRxmg,
                        tailormailRxmg,
                       ]).reset_index()
brontoRxmg['index_b'] = brontoRxmg.index

# get name varialbes
startTimeRxmg = (brontoRxmg.date_brt.max().date() - datetime.timedelta(days = 30)).strftime("%m.%d.%y")
endTimeRxmg = brontoRxmg.date_brt.max().date().strftime("%m.%d.%y")
fullTimeRxmg = " - ".join([startTimeRxmg,endTimeRxmg])
prefixRxmg = "Irvine & Venice Combined Files"
folder_nameRxmg = "Irvine & Venice Combined Files"

# combine EA and Bronto:
df_newRxmg, dup_fullRxmg, missing_fullRxmg = methods.combine_bronto_ea(brontoRxmg, eaRxmg)


# In[3]:


# ARbronto = brontoRxmg[brontoRxmg.Message.str.contains('_W.')]
# ARorg = orgRxmg[orgRxmg.Message.str.contains('_W.')]
# ARrevenue = revenueRxmg[revenueRxmg.Message.str.contains('_W.')]

# brontoRxmg = brontoRxmg[~brontoRxmg.Message.str.contains('_W.')]
# ...


# In[4]:


# make Venice and Irvine sub directories
os.mkdir(f'{DATADIR}/Venice Reports')
os.mkdir(f'{DATADIR}/Irvine Reports')
os.mkdir(f'{DATADIR}/Combined Reports')


# In[7]:


# ================================ Rxmg ERROR REPORT V2 ======================================================= #
# message checking report 
eaDomains = list(eaRxmg.sender_domain.unique())
# brtNewStruc = brontoRxmg[brontoRxmg['date_brt'] >= '2019-12-7']
brtNewStruc = brontoRxmg[(brontoRxmg['Message'].str.count('_')>=11)|(brontoRxmg['drop'].str.contains('.C'))]
# revenueNewStruc = revenueRxmg[revenueRxmg['Date'] >= '2019-12-7']
revenueNewStruc = revenueRxmg[(revenueRxmg['Message'].str.count('_')>=11)|(revenueRxmg['drop'].str.contains('.C'))]
errorBrtRxmg, errorIteRxmg,errorTMRxmg,errorrevRxmg, notInRevRxmg, notInSenderRxmg, dupSenderRxmg, dupRevRxmg = methods.error_reportV2(brtNewStruc,revenueNewStruc,fullTimeRxmg,df_newRxmg, dup_fullRxmg, missing_fullRxmg,DPDSDomainESPSAPUBlist,domainNames,eaDomains)

# save error report
methods.error_report_saver(folder_nameRxmg,prefixRxmg,'(new structure)',fullTimeRxmg,errorBrtRxmg, errorIteRxmg,errorTMRxmg,errorrevRxmg,notInRevRxmg,notInSenderRxmg,dupSenderRxmg, dupRevRxmg,missing_fullRxmg, dup_fullRxmg)

# ================================ Rxmg ERROR REPORT V1 ======================================================= #
# message checking report 
eaDomains = list(eaRxmg.sender_domain.unique())
# brtOldStruc = brontoRxmg[brontoRxmg['date_brt'] < '2019-12-7']
# revenueOldStruc = revenueRxmg[revenueRxmg['Date'] < '2019-12-7']
brtOldStruc = brontoRxmg[(brontoRxmg['Message'].str.count('_')<11)&(~brontoRxmg['drop'].str.contains('.C'))]
revenueOldStruc = revenueRxmg[(revenueRxmg['Message'].str.count('_')<11)&(~revenueRxmg['drop'].str.contains('.C'))]
errorBrtRxmg, errorIteRxmg, errorTMRxmg,errorrevRxmg, notInRevRxmg, notInSenderRxmg, dupSenderRxmg, dupRevRxmg = methods.error_report(brtOldStruc,revenueOldStruc,fullTimeRxmg,df_newRxmg, dup_fullRxmg, missing_fullRxmg,DPDSDomainlist,domainNames,eaDomains)

# save error report
methods.error_report_saver(folder_nameRxmg,prefixRxmg,'(old structure)',fullTimeRxmg,errorBrtRxmg, errorIteRxmg,errorTMRxmg,errorrevRxmg,notInRevRxmg,notInSenderRxmg,dupSenderRxmg, dupRevRxmg,missing_fullRxmg, dup_fullRxmg)

# ea error report
methods.ea_error_report(folder_nameRxmg,prefixRxmg,missing_fullRxmg, dup_fullRxmg, brontoRxmg, DPDSDomainESPSAPUBlist, domainNames, eaDomains)


# ## Pt II. Master stats generating

# In[8]:


# add revenue stats:
combine = methods.addRevenue(brontoRxmg,revenueRxmg,df_newRxmg)
consolidatedDf,monthly_drops,master = methods.consolidated_report(combine)
monthly_drops=methods.rest_Breaker(monthly_drops)

consolidatedDf=methods.rest_Breaker(consolidatedDf)
consolidatedDf.drop(['rest_','ESP'],axis = 1,inplace = True)
consolidatedDf["DP.DS/DV_PubID"] = consolidatedDf['data_provider']+'_'+consolidatedDf['pubid']

# add vertical info:
monthly_drops = methods.addVertical(monthly_drops, mappingDF)
monthly_drops = monthly_drops.drop(['DP.DS or DP.DV if multiple sources using samePubID','_merge'],axis=1)


# In[9]:


weekly_drops = monthly_drops[monthly_drops.Date >= monthly_drops.Date.max()-datetime.timedelta(days = 7)]
monthly_drops.to_excel(f'/Users/yanangao/Desktop/{folder_nameRxmg}/raw_no_format_{fullTimeRxmg}.xlsx', index = False)
StartTimeWeek = weekly_drops.Date.min().strftime('%m.%d.%y')
EndTimeWeek = weekly_drops.Date.max().strftime('%m.%d.%y')

# ================ split monthly_drops by office: Irvine and Venice ==================== #

DPDSIrvine = [
    'PMG.RF','WC.RF','UPSD.RF','PMG.DEBT','LXCN.PA','LPG.RF','LPG.FHA','AP.I','SC.RF','SC.FHA'
]
DPDSVenice = [x for x in DPDSlist if x not in DPDSIrvine]

monthly_dropsVenice = monthly_drops[monthly_drops.data_provider.isin(DPDSVenice)] 
monthly_dropsIrvine = monthly_drops[monthly_drops.data_provider.isin(DPDSIrvine)]

weekly_dropsVenice = weekly_drops[weekly_drops.data_provider.isin(DPDSVenice)] 
weekly_dropsIrvine = weekly_drops[weekly_drops.data_provider.isin(DPDSIrvine)] 


# ### - ESP/SubAccount Report
# - monthly data only
# - on hold!!!!!

# In[10]:


# weeklyDropsSubUse, subaccountWise = methods.GroupBySubaccount(monthly_drops)
# pathList = [f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/ESP-SubAccount Report-{fullTimeRxmg}.xlsx',
#             f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/ESP-SubAccount Report-{fullTimeRxmg}.xlsx']

# for path in pathList:
#     subWiseWriter = ExcelWriter(path)
#     subaccountWise.to_excel(subWiseWriter, 'sub account performance',index = False)
#     methods.format(subaccountWise,subWiseWriter,'sub account performance')

#     subaccountWise['ESP_SA'] = subaccountWise[['ESP','Sub_account']].apply(lambda x:'_'.join(x),axis = 1)
#     for ESPSA in list(subaccountWise.ESP_SA.unique()):
#         subDF = weeklyDropsSubUse[weeklyDropsSubUse.ESP_SA == ESPSA]
#         subDF.to_excel(subWiseWriter,f'{ESPSA}',index = False)
#         methods.format(subDF,subWiseWriter,f'{ESPSA}')

#     subWiseWriter.save()


# ### - Scheduling master stats
# combine gmail and non-gmail message parts, and re-match with EA inboxing stats
# 
# should be written into a function and move to methods

# In[11]:


monthly_drops['strmonth'] = monthly_drops['Date'].dt.strftime('%B %-d, %Y')

#clean Iterable subject
monthly_drops['sub_clean'] = monthly_drops['subject line_esp'].str.replace("{{firstname}}’s", '').str.replace(
                            "{{firstname}}'s", '').str.replace("{{firstname}},", '').str.replace(
                            '{{firstname}}:', '').str.replace("{{firstname}}", '').str.replace(
                            '{{firstname}}', '').str.replace('{{firstname}}’s', '').str.replace('{{lastname}}', '').str.replace(
                            '{{city}}', '').str.replace('{{state}}', '').str.replace(
                            '{{address1}}', '').str.replace("{{state_abbrev}}", '').str.replace("{{date}}", '').str.lstrip(' -,').str.rstrip(' .?!-')



#clean Iterable subject
monthly_drops['sub_clean'] = monthly_drops['sub_clean'].str.replace("%%firstname%%’s", '').str.replace(
                            "%%firstname%%'s", '').str.replace("%%firstname%%,", '').str.replace(
                            '%%firstname%%:', '').str.replace("%%firstname%%", '').str.replace(
                            '%%firstname%%', '').str.replace('%%lastname%%’s', '').str.replace('%%lastname%%', '').str.replace(
                            '%%city%%', '').str.replace('%%state%%', '').str.replace(
                            '%%address1%%', '').str.replace("%%state_abbrev%%", '').str.replace("%%date%%", '').str.lstrip(' -,').str.rstrip(' .?!-')

monthly_drops['sub_clean'] = monthly_drops.apply(lambda x: x['sub_clean'].replace('%%!date%%',x['strmonth']),axis =1)
monthly_drops['sub_clean'] = monthly_drops.apply(lambda x: x['sub_clean'].replace('{{now}}',x['strmonth']),axis =1)





# In[12]:


monthly_dropsNew = monthly_drops[monthly_drops['drop'].str.contains(".C")]
monthly_dropsOld = monthly_drops[~monthly_drops['drop'].str.contains(".C")]

schedulMasterNew = monthly_dropsNew.groupby(['Date','drop','data_provider','Campaign ID']).agg({
#     'Campaign ID':'first',
#     'data_provider':'first',
    'openers':'first',
    'sub_clean':'first',
    'domain':'first',
    'Message':'; '.join,
    'Sent':'sum',
    'Delivered':'sum',
    'Opens':'sum',
    'Clicks':'sum',
    'Contacts Lost':'sum',
    'Adjusted Clicks':'sum',
    'Offer Name':'max',
# #     'Campaign ID':'max',
    'Link Used in Mailing':'max',
    'Affiliate ID':'max',
    'Sub ID':'; '.join, #errors!!!!?????
    'Revenue':'sum',
    'Cost CPM':'max',
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
    'ESP':';' .join,
    'sa':'first',
    'pubid':'first',
    'creativeType':'first',

}).reset_index(drop=False)


schedulMasterOld = monthly_dropsOld.groupby(['Campaign ID','Date','data_provider','openers','sub_clean','domain']).agg({
    'Message':'; '.join,
    'Sent':'sum',
    'Delivered':'sum',
    'Opens':'sum',
    'Clicks':'sum',
    'Contacts Lost':'sum',
    'Adjusted Clicks':'sum',
    'Offer Name':'max',
# #     'Campaign ID':'max',
    'Link Used in Mailing':'max',
    'Affiliate ID':'max',
    'Sub ID':'; '.join, #errors!!!!?????
    'Revenue':'sum',
    'Cost CPM':'max',
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
    'drop':'; '.join,
    'ESP':';' .join,
    'sa':'first',
    'pubid':'first',
    'creativeType':'first',

}).reset_index(drop=False)

schedulMasterOld = schedulMasterOld.reindex(columns = schedulMasterNew.columns)
schedulMaster = pd.concat([schedulMasterOld,schedulMasterNew])

schedulMaster['Open Rate'] = schedulMaster['Opens']/schedulMaster['Delivered']
schedulMaster['Click Rate'] = schedulMaster['Clicks']/schedulMaster['Opens']
schedulMaster['Adjusted Click Rate'] = schedulMaster['Adjusted Clicks']/schedulMaster['Opens']
schedulMaster['RPC'] = schedulMaster['Revenue']/schedulMaster['Clicks']
schedulMaster['Revenue CPM (eCPM)'] = schedulMaster['Revenue']*1000/schedulMaster['Delivered']
for j in ['aol', 'google', 'yahoo', 'outlook', 'global_isps', 'overall']:
    schedulMaster['%s_inbox_percent' % j] = schedulMaster['%s_action_volume' % j] / schedulMaster['actual_%s_volume' % j]


# In[13]:


missingInbox = schedulMaster[schedulMaster.overall_inbox_percent.isna()]
goodInbox = pd.concat([missingInbox,schedulMaster]).drop_duplicates(keep=False)
missingInbox = missingInbox.reset_index()

# add 1 day 
missingInbox['date_plus_one'] = missingInbox.Date + datetime.timedelta(days = 1)

#add index
missingInbox['index_b'] = missingInbox.index
attr = ['overall_inbox_percent','aol_inbox_percent','google_inbox_percent','yahoo_inbox_percent','outlook_inbox_percent','global_isps_inbox_percent']
missingInbox = missingInbox.drop(columns = attr, axis = 1)

eaRxmg = eaRxmg[['date','sender_domain','sub_clean','overall_inbox_percent','aol_inbox_percent','google_inbox_percent','yahoo_inbox_percent','outlook_inbox_percent','global_isps_inbox_percent','index_i']]


# In[14]:


# delivered on the same day
combined_1 = missingInbox.merge(
    eaRxmg, 
    how = 'inner', 
    left_on = ['Date','sub_clean', 'domain'],
    right_on = ['date','sub_clean', 'sender_domain']
)

# delivered on next day
combined_2 = missingInbox.merge(
    eaRxmg, 
    how = 'inner', 
    left_on = ['date_plus_one','sub_clean','domain'],
    right_on = ['date','sub_clean','sender_domain']
)

combined_full = pd.concat([combined_2, combined_1],ignore_index=True)

#  get unmatched drops
non_na = combined_2['index_b'].tolist() + combined_1['index_b'].tolist()
complete_full = missingInbox.index.isin(non_na)

missing_full = missingInbox[~complete_full]
#     return missing_full, complete_full

# get duplicated drops
dup_brt = combined_full.loc[combined_full.duplicated('index_b', keep = False)]
dup_ibr = combined_full.loc[combined_full.duplicated('index_i', keep = False)]

dup_full = pd.concat([dup_brt, dup_ibr], ignore_index = True).drop_duplicates(keep = 'first')

# get perfectly matched data without duplicated in any file
df_new = pd.concat([combined_full,dup_full]).drop_duplicates(keep=False)

# df_new = df_new.loc[df_new['_merge'] == 'left_only']
# df_new.drop('openers',axis=1,inplace=True)


# In[15]:


print("\n-------EA match Bronto report---------\n")
print("# of duplicated drops in brt:", dup_full.drop_duplicates('index_b',keep='first').shape)

print("# of good drops in brt:", df_new.shape)

print("# of unmatches in brt:", missing_full.shape)


if len(dup_full.drop_duplicates('index_b',keep='first')) + len(df_new) + len(missing_full) == len(missingInbox):
    print("Pass row number check of brt and merging tables")
else: 
    print("Row number error of merging table")
print("\n---------------------------------------\n")


# In[16]:


header_list = list(goodInbox)
df_new = df_new.reindex(columns = header_list)
finalSchedule = pd.concat([schedulMaster,df_new]).sort_values('overall_inbox_percent',ascending = False).drop_duplicates('Message',keep='first')


# In[17]:


finalSchedule['send_strategy']=finalSchedule["drop"].str.split('.').str[0]
finalSchedule["Drop_Number"] = finalSchedule["drop"].str.split('.').str[1]
finalSchedule['Split_Variable'] = finalSchedule["drop"].str.split('.').str[2]+'.'+finalSchedule["drop"].str.split('.').str[3]
# ".".join(
#     finalSchedule["drop"].str.split('.').str[2],
#     finalSchedule["drop"].str.split('.').str[3]
# )

attr = ['Campaign ID','Date',  'data_provider', 'openers', 'Message','Sent', 'Delivered', 'Opens', 'Open Rate', 'Clicks',
        'Click Rate', 'Contacts Lost','Adjusted Clicks','Adjusted Click Rate','Offer Name', 'Campaign ID', 'Link Used in Mailing', 'Affiliate ID', 'Sub ID',
        'Revenue', 'RPC', 'Revenue CPM (eCPM)', 'Cost CPM', 'Cost per send', 'Net Revenue', 'Margin',
         'overall_inbox_percent', 'aol_inbox_percent','google_inbox_percent', 'yahoo_inbox_percent', 'outlook_inbox_percent',
        'global_isps_inbox_percent','drop','ESP','sa','pubid','creativeType','send_strategy','Drop_Number','Split_Variable']

finalSchedule = finalSchedule[attr]
finalSchedule = finalSchedule.rename(columns={'Campaign ID':'Offer ID'})
finalSchedule.insert(2, "Helper", np.nan)
#     finalSchedule.insert(16,"Sub ID",np.nan)
finalSchedule.insert(20,"Unnamed: 15",np.nan)
finalSchedule.insert(28,"Vertical", np.nan)
finalSchedule.insert(29,"Network", np.nan)
finalSchedule["DP.DS/DV_PubID"] = finalSchedule['data_provider']+'_'+finalSchedule['pubid']

finalSchedule.replace(np.inf,np.nan,inplace=True)


# In[18]:


finalSchedule.to_excel(f'/Users/yanangao/Desktop/{folder_nameRxmg}/schedule master_{fullTimeRxmg} V2.xlsx', index = False)


# In[19]:


# ===================== schedule master version 1 =================================#
# # scheduleIrvine = methods.schedulingmaster(monthly_dropsIrvine)
# scheduleIrvine = methods.schedulingmaster(monthly_drops)

# scheduleWriter = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/schedule master_{fullTimeRxmg}.xlsx')
# scheduleIrvine.to_excel(scheduleWriter,'Schedule Master',index = False)
# methods.format(scheduleIrvine,scheduleWriter,'Schedule Master')
# scheduleWriter.save()


# ### - Content split report

# In[20]:


scheduleIrvine = methods.schedulebyContentID(monthly_drops)

scheduleWriter = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Content Split Report_{fullTimeRxmg}.xlsx')
scheduleIrvine.to_excel(scheduleWriter,'Schedule Master',index = False)
methods.format(scheduleIrvine,scheduleWriter,'Schedule Master')
scheduleWriter.save()


# ### - Consolidated stats report

# In[21]:


for path in [f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Venice and Irvine Reporting - Consolidated Bronto SA Drop Stats_{StartTimeWeek}-{EndTimeWeek}.xlsx',
            f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Venice and Irvine Reporting - Consolidated Bronto SA Drop Stats_{StartTimeWeek}-{EndTimeWeek}.xlsx']:
    ConsWriter = ExcelWriter(path)
    
#     consolidatedDf = consolidatedDf[consolidatedDf.Date >= consolidatedDf.Date.max()-datetime.timedelta(days = 7)]
    consolidatedDf['data_provider'] = consolidatedDf.data_provider.str.upper()
    consolidatedDf.to_excel(ConsWriter,'Consolidated Stats',index = False)
    methods.format(consolidatedDf,ConsWriter,'Consolidated Stats')
    ConsWriter.save()


# ====================== above: master stats ===============

# ## Pt III. Offer performance reporting 
# ### - Offer performance report
# - Weekly & monthly

# In[22]:


from datetime import datetime 
from xlrd.xldate import xldate_as_tuple

# historical drops: venice + irvine
full_drops = pd.read_excel('/Users/yanangao/Desktop/Bronto w IBR/historical_drops/new historical/Rxmg-Full-Drops.xlsx') # this file is the master file I kept to record all historical drops from july 10th to today.
full_drops['Campaign ID'] = full_drops['Campaign ID'].astype(int).astype(str)


# In[23]:


campaignDrops = full_drops[['Campaign ID']].drop_duplicates(keep = 'first')
campaignOffersheet = offers[['Hitpath Offer ID']]
campaignMissing = pd.merge(
    campaignDrops,campaignOffersheet,
    how = 'left',
    indicator = True,
    left_on = 'Campaign ID',
    right_on = 'Hitpath Offer ID'
)
campaignMissing = campaignMissing[campaignMissing._merge == 'left_only']
campaignMissing.to_csv(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Campaign ID missing in offer sheet.csv', index = False)
campaignMissing.to_csv(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Campaign ID missing in offer sheet.csv', index = False)


# In[24]:


SEGlistVenice = [
    'OA','O150','O180','O90','O60','O45','O30','O21','O15','O14','O10','O7','O1','O5','O120','O3','O160','O31',
    'C','C30','C15','C14','C7','C17','C21',
    'A90','A60','A30','A21','A15','A14','A7','A10',
    'M','M5','M5.O21','M5.O30','M4','M3.O21','M10.O30','M10.O21','M10','MI','MD','M1','M3',
    'TEST','T','T1',
    'CO30','CO1','CM','CO1',
    'W','WRMUP',
    'ACTIVE', 
    'NO30',
    'M5.O14','M3.O14',
]


SEGlistIrvine = [
    'O30','M','O1','T','CO30','CO1','C','CM','W','ACTIVE'
]


# In[25]:


# ================================ VENICE VERSION ======================================================= #

# file 1. Venice report with all historical messages:
writer_full_drops = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/HISTORICAL - Offer & Vertical Performance by Segment- 1.28.19-{endTimeRxmg}.xlsx')
writer_full_drops = methods.generate_rxmg_report(writer_full_drops,full_drops, full_drops,'openers',SEGlistVenice,offers)
writer_full_drops.save()

# monthly reports
writer_monthly_drops_overall = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Venice-Offer & Vertical Performance by Segment-{fullTimeRxmg}.xlsx')
writer_monthly_drops_overall = methods.generate_rxmg_report(writer_monthly_drops_overall,monthly_drops,monthly_dropsVenice, 'openers',SEGlistVenice,offers)
writer_monthly_drops_overall.save()

# monthly reports by vertical
writer_monthly_drops_veniceonly = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Venice - Offer & Vertical Performance by Vertical-{fullTimeRxmg}.xlsx')
writer_monthly_drops_veniceonly = methods.generate_rxmg_report(writer_monthly_drops_veniceonly,monthly_drops,monthly_dropsVenice,'Source_Vertical',list(monthly_dropsVenice.Source_Vertical.unique()),offers)
writer_monthly_drops_veniceonly.save()

# weekly full report
writer_weekly_drops_overall = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Venice-Offer & Vertical Performance by Segment-{StartTimeWeek}-{EndTimeWeek}.xlsx')
writer_weekly_drops_overall = methods.generate_rxmg_report(writer_weekly_drops_overall,weekly_drops,weekly_dropsVenice, 'openers',SEGlistVenice,offers)
writer_weekly_drops_overall.save()

# weekly reports by vertical
writer_weekly_drops_veniceonly = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Venice - Offer & Vertical Performance by Vertical-{StartTimeWeek}-{EndTimeWeek}.xlsx')
writer_weekly_drops_veniceonly = methods.generate_rxmg_report(writer_weekly_drops_veniceonly,weekly_drops,weekly_dropsVenice,'Source_Vertical',list(monthly_dropsVenice.Source_Vertical.unique()),offers)
writer_weekly_drops_veniceonly.save()

# weekly emails broke down by dataset
for affiliate in list(monthly_dropsVenice['Affiliate ID'].dropna().unique()):
        
    drop_by_opener = monthly_drops.loc[monthly_drops['Affiliate ID'] == affiliate]
    dataProvider = drop_by_opener.iloc[0]['data_provider']
    
    writer_weekly_drops = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/Venice-{affiliate}-{dataProvider} DROPS- Offer & Vertical Performance by Segment - {fullTimeRxmg}.xlsx')
    # add original revenue file to each pubid performance sheet
    dropbyPub = orgRxmg[orgRxmg['Affiliate ID'] == affiliate]
    dropbyPub.to_excel(writer_weekly_drops,f'{affiliate} monthly revenue stats', index = False)
    methods.format(dropbyPub,writer_weekly_drops,f'{affiliate} monthly revenue stats')

    # segment report sheets
    writer_weekly_drops = methods.generate_rxmg_report(writer_weekly_drops,monthly_drops,drop_by_opener,'openers',SEGlistVenice,offers)
       
    writer_weekly_drops.save()


# In[26]:


# file 1. Irvine report with all historical messages:
writer_full_drops = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/HISTORICAL - Offer & Vertical Performance by DP.DS- 1.28.19-{endTimeRxmg}.xlsx')
writer_full_drops = methods.generate_rxmg_report(writer_full_drops,full_drops,full_drops,'data_provider',list(full_drops.data_provider.unique()),offers)
writer_full_drops.save()

# file 2-4: Irvine weekly message report
# monthly full report
writer_weekly_drops_overall = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Irvine & Venice -Offer & Vertical Performance by DP.DS-{fullTimeRxmg}.xlsx')
writer_weekly_drops_overall = methods.generate_rxmg_report(writer_weekly_drops_overall,monthly_drops,monthly_drops, 'data_provider',list(monthly_drops.data_provider.unique()),offers)
writer_weekly_drops_overall.save()

# monthly reports by vertical
writer_monthly_drops_irvineonly = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Irvine - Offer & Vertical Performance by Vertical-{fullTimeRxmg}.xlsx')
writer_monthly_drops_irvineonly = methods.generate_rxmg_report(writer_monthly_drops_irvineonly,monthly_drops,monthly_dropsIrvine,'Source_Vertical',list(monthly_dropsIrvine.Source_Vertical.unique()),offers)
writer_monthly_drops_irvineonly.save()

# weekly full report
writer_weekly_drops_overall = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Irvine & Venice -Offer & Vertical Performance by DP.DS-{StartTimeWeek}-{EndTimeWeek}.xlsx')
writer_weekly_drops_overall = methods.generate_rxmg_report(writer_weekly_drops_overall,weekly_drops,weekly_drops, 'data_provider',list(weekly_drops.data_provider.unique()),offers)
writer_weekly_drops_overall.save()

# weekly reports by vertical
writer_weekly_drops_irvineonly = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Irvine - Offer & Vertical Performance by Vertical-{StartTimeWeek}-{EndTimeWeek}.xlsx')
writer_weekly_drops_irvineonly = methods.generate_rxmg_report(writer_weekly_drops_irvineonly,weekly_drops,weekly_dropsIrvine,'Source_Vertical',list(monthly_dropsIrvine.Source_Vertical.unique()),offers)
writer_weekly_drops_irvineonly.save()


# rename openers in file name
openers = {
    "O30": "PRODUCTION",
    "O1": "TEST",
    "M": "MINING",
    "W": "WELCOME",
    "C": "CUSTOM SEGMENT",
    }
uniqueListOfOpeners = ['O30','O1','M','W','C']

# weekly emails broke down by segment (O30,O1,M)
for opener in uniqueListOfOpeners:
    drop_by_opener = weekly_drops.loc[weekly_drops.openers == opener]
    
    writer_weekly_drops = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Irvine {openers[opener]} DROPS- Offer & Vertical Performance by DP.DS - {StartTimeWeek}-{EndTimeWeek}.xlsx')
    writer_weekly_drops = methods.generate_rxmg_report(writer_weekly_drops,weekly_drops,drop_by_opener,'data_provider',list(monthly_dropsIrvine.data_provider.unique()),offers)
    writer_weekly_drops.save()


# In[27]:


# =========================== data splits by vertical ========================================== #

# All time historical
writer_full_drops = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Combined Reports/HISTORICAL - Offer & Vertical Performance by Vertical - 1.28.19-{endTimeRxmg}.xlsx')
writer_full_drops = methods.generate_rxmg_report(writer_full_drops,full_drops,full_drops,'Source_Vertical',list(full_drops.Source_Vertical.unique()),offers)
writer_full_drops.save()

# monthly reports
writer_monthly_drops_overall = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Combined Reports/Irvine & Venice - Offer & Vertical Performance by Vertical-{fullTimeRxmg}.xlsx')
writer_monthly_drops_overall = methods.generate_rxmg_report(writer_monthly_drops_overall,monthly_drops,monthly_drops,'Source_Vertical',list(monthly_drops.Source_Vertical.unique()),offers)
writer_monthly_drops_overall.save()

# file 2-4: Venice weekly message report
# weekly full report
writer_weekly_drops_overall = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Combined Reports/Irvine & Venice - Offer & Vertical Performance by Vertical-{StartTimeWeek}-{EndTimeWeek}.xlsx')
writer_weekly_drops_overall = methods.generate_rxmg_report(writer_weekly_drops_overall,weekly_drops,weekly_drops,'Source_Vertical',list(monthly_drops.Source_Vertical.unique()),offers)
writer_weekly_drops_overall.save()


# ### - Total sent by pubid

# In[28]:


fullDrops = full_drops[full_drops.Date>='2019-8-1']
fullDrops['Month']=fullDrops.Date.dt.strftime('%b')

SentSubTotal = ExcelWriter(f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/Total Sent by Month for PubID.xlsx')
for month in list(fullDrops.Month.unique()):
    subDF = fullDrops[fullDrops.Month == month]
    monthlySubTotal = subDF.groupby('Affiliate ID',as_index=False).agg({'Sent':'sum'})
    monthlySubTotal.to_excel(SentSubTotal,f'{month}',index = False)
SentSubTotal.save()
    


# ### - subject line performance report

# In[29]:


# weekly subject line performance: 
groupMaster = methods.subjectLinePerformance(weekly_drops,offers)

for path in [
    f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/subject line performance_COMBINED_{StartTimeWeek}-{EndTimeWeek}.xlsx',
    f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/subject line performance_COMBINED_{StartTimeWeek}-{EndTimeWeek}.xlsx'
]:
    subjectWriter = ExcelWriter(path)
    groupMaster.to_excel(subjectWriter,'overall performance',index = False)
    methods.format(groupMaster, subjectWriter, 'overall performance')
    for pubid in list(weekly_drops['Affiliate ID'].unique()):
        weekly_dropsByPubid = weekly_drops[weekly_drops['Affiliate ID']==pubid]
        groupMasterByPubid = methods.subjectLinePerformance(weekly_dropsByPubid,offers)
        groupMasterByPubid.to_excel(subjectWriter,f'{pubid} performance',index = False)
        methods.format(groupMasterByPubid, subjectWriter, f'{pubid} performance')
    subjectWriter.save()

# monthly subject line performance:

groupMaster = methods.subjectLinePerformance(monthly_drops,offers)

for path in [
    f'/Users/yanangao/Desktop/{folder_nameRxmg}/Irvine Reports/subject line performance_COMBINED_{fullTimeRxmg}.xlsx',
    f'/Users/yanangao/Desktop/{folder_nameRxmg}/Venice Reports/subject line performance_COMBINED_{fullTimeRxmg}.xlsx'
]:
    subjectWriter = ExcelWriter(path)
    groupMaster.to_excel(subjectWriter,'overlap performance',index = False)
    methods.format(groupMaster, subjectWriter, 'overlap performance')
    for pubid in list(monthly_drops['Affiliate ID'].unique()):
        monthly_dropsByPubid = monthly_drops[monthly_drops['Affiliate ID']==pubid]
        groupMasterByPubid = methods.subjectLinePerformance(monthly_dropsByPubid,offers)
        groupMasterByPubid.to_excel(subjectWriter,f'{pubid} performance',index = False)
        methods.format(groupMasterByPubid, subjectWriter, f'{pubid} performance')
    subjectWriter.save()


# ### - Offer/Vertical Last 30 days production analysis

# In[30]:


methods.productionAnalysis(monthly_drops,offers,folder_nameRxmg,'Combined Reports',prefixRxmg,fullTimeRxmg,'COMBINED')
# methods.productionAnalysis(monthly_drops,offers,folder_nameRxmg,'Venice Reports',prefixRxmg,fullTimeRxmg)
methods.productionAnalysis(monthly_dropsIrvine,offers,folder_nameRxmg,'Irvine Reports',prefixRxmg,fullTimeRxmg,'IRVINE ONLY')
methods.productionAnalysis(monthly_dropsVenice,offers,folder_nameRxmg,'Venice Reports',prefixRxmg,fullTimeRxmg,'VENICE ONLY')


# ### - offer summary stats by dataset

# In[31]:


methods.offerSummaryByDatasets(full_drops, offers,folder_nameRxmg)


# In[ ]:





# In[ ]:




