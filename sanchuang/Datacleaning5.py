# -*- encoding: utf-8 -*-

import numpy as np
import pandas as pd


def getdata():
    for catelog in range(23,34):
        for csv in range(1,52):
            df = pd.read_excel('C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data\\'+str(catelog)+'\\'+str(csv)+'.xlsx')
            print(str(catelog)+'-'+str(csv)+'index'+str(len(df.index)))
            if len(df.index)>2:
                i = 1
                while i<len(df.index):
                    print(str(catelog)+'-'+str(csv)+'-'+str(i))
                    if df.delta[i-1]-df.delta[i]>3:
                        insertRow = df.loc[i-1]
                        above = df.loc[:i-1]
                        blow = df.loc[i:]
                        df = above.append(insertRow,ignore_index=True).append(blow,ignore_index=True)
                        df.delta[i] = df.delta[i-1]-3
                        i = i + 1
                    else:
                        i = i + 1
                        pass
            elif len(df.index)==2:
                df.delta[1] = df.delta[0] - 3
            else:
                pass
            
            f = open('C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data2\\'+str(catelog)+'\\'+str(csv)+'.xlsx','w')
            f.close()
            df.to_excel('C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data2\\'+str(catelog)+'\\'+str(csv)+'.xlsx')
            print('OK')

getdata()