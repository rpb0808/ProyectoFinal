import os
import sys
import csv
import boto3
import shutil 
import textwrap
import pandas as pd
import numpy as np

from datetime import timedelta

def conver(data):
    
    # creacion de Data Frame vacio
    df=pd.DataFrame(columns=['Time','Type','RecCatName','Order','Batch','UnitPCU','UnitNo','UnitName','PCU',
                'Module','InstNo','InstName','MsgText','MsgID','Status','MsgClass','BatchYear','RecCatNo','MsgDefNo'])

    for i in range(0, (len(data)-1)):
        df.loc[i,'Time']= timedelta(hours=int(data[i][0:2]),
                           minutes=int(data[i][3:5]),
                           seconds=int(data[i][6:8]))
        df.loc[i,'Type']= str(data[i][10:11]).strip()
        df.loc[i,'RecCatName']= str(data[i][12:28]).strip()
        df.loc[i,'Order']= str(data[i][30:36]).strip()
        df.loc[i,'Batch']= str(data[i][36:42]).strip()
        df.loc[i,'UnitPCU']= str(data[i][43:46]).strip()
        df.loc[i,'UnitNo']= str(data[i][47:50]).strip()
        df.loc[i,'UnitName']= str(data[i][51:67]).strip()
        df.loc[i,'PCU']= str(data[i][68:71]).strip()
        df.loc[i,'Module']= str(data[i][72:80]).strip()
        df.loc[i,'InstNo']= str(data[i][81:85]).strip()
        df.loc[i,'InstName']= str(data[i][86:103]).strip()
        df.loc[i,'MsgText']= str(data[i][103:152]).strip()
        df.loc[i,'MsgID']= str(data[i][152:157]).strip()
        df.loc[i,'Status']= str(data[i][158:159]).strip()
        df.loc[i,'MsgClass']= str(data[i][161:163]).strip()
        df.loc[i,'BatchYear']= str(data[i][164:166]).strip()
        df.loc[i,'RecCatNo']= str(data[i][167:216]).strip()
        df.loc[i,'MsgDefNo']= str(data[i][216:222]).strip()
    return(df)