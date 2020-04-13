#!/usr/bin/env python
# coding: utf-8

# In[2]:


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
from xlsxwriter.utility import xl_col_to_name
import datetime

warnings.filterwarnings('ignore')
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', 500)


# ### Reading Files

# In[3]:


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
startTimeRxmg = brontoRxmg.date_brt.min().date().strftime("%m.%d.%y")
endTimeRxmg = brontoRxmg.date_brt.max().date().strftime("%m.%d.%y")
fullTimeRxmg = " - ".join([startTimeRxmg,endTimeRxmg])
prefixRxmg = "Irvine & Venice Combined Files"
folder_nameRxmg = "Irvine & Venice Combined Files"

# combine EA and Bronto:
df_newRxmg, dup_fullRxmg, missing_fullRxmg = methods.combine_bronto_ea(brontoRxmg, eaRxmg)


# ### Error message report

# In[4]:


# ================================ Rxmg ERROR REPORT V2 ======================================================= #
# message checking report 
eaDomains = list(eaRxmg.sender_domain.unique())
# brtNewStruc = brontoRxmg[brontoRxmg['date_brt'] >= '2019-12-7']
brtNewStruc = brontoRxmg[(brontoRxmg['Message'].str.count('_')>=11)|(brontoRxmg['drop'].str.contains('.C'))]
# revenueNewStruc = revenueRxmg[revenueRxmg['Date'] >= '2019-12-7']
revenueNewStruc = revenueRxmg[(revenueRxmg['Message'].str.count('_')>=11)|(revenueRxmg['drop'].str.contains('.C'))]
errorBrtRxmg, errorIteRxmg,errorTMRxmg,errorrevRxmg, notInRevRxmg, notInSenderRxmg, dupSenderRxmg, dupRevRxmg = methods.error_reportV2(brtNewStruc,revenueNewStruc,fullTimeRxmg,df_newRxmg, dup_fullRxmg, missing_fullRxmg,DPDSDomainESPSAPUBlist,domainNames,eaDomains)

# save error report
methods.error_report_saver(folder_nameRxmg,prefixRxmg,'(new structure)',fullTimeRxmg,errorBrtRxmg, errorIteRxmg,errorTMRxmg,errorrevRxmg,notInRevRxmg,notInSenderRxmg,dupSenderRxmg, dupRevRxmg,missing_fullRxmg)

# ================================ Rxmg ERROR REPORT V1 ======================================================= #
# message checking report 
eaDomains = list(eaRxmg.sender_domain.unique())
# brtOldStruc = brontoRxmg[brontoRxmg['date_brt'] < '2019-12-7']
# revenueOldStruc = revenueRxmg[revenueRxmg['Date'] < '2019-12-7']
brtOldStruc = brontoRxmg[(brontoRxmg['Message'].str.count('_')<11)&(~brontoRxmg['drop'].str.contains('.C'))]
revenueOldStruc = revenueRxmg[(revenueRxmg['Message'].str.count('_')<11)&(~revenueRxmg['drop'].str.contains('.C'))]
errorBrtRxmg, errorIteRxmg, errorTMRxmg,errorrevRxmg, notInRevRxmg, notInSenderRxmg, dupSenderRxmg, dupRevRxmg = methods.error_report(brtOldStruc,revenueOldStruc,fullTimeRxmg,df_newRxmg, dup_fullRxmg, missing_fullRxmg,DPDSDomainlist,domainNames,eaDomains)

# save error report
methods.error_report_saver(folder_nameRxmg,prefixRxmg,'(old structure)',fullTimeRxmg,errorBrtRxmg, errorIteRxmg,errorTMRxmg,errorrevRxmg,notInRevRxmg,notInSenderRxmg,dupSenderRxmg, dupRevRxmg,missing_fullRxmg)

