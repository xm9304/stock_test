import datetime

import pandas as pd
import numpy as np
import xlsxwriter
import math

df = pd.DataFrame(np.arange(1100000))

# df = pd.DataFrame(np.arange(12000000).reshape(300000,40))
row_num = int(df.shape[0])
print(row_num)
split_num = 1000000
n = math.ceil(row_num / split_num)
with pd.ExcelWriter("test.xlsx") as writer:
    for i in range(n):
        handle_df = df.iloc[split_num * i: split_num * (i + 1)]
        # writer = pd.ExcelWriter('test.xlsx', engine='xlsxwriter', options={'strings_to_urls':False})  # options参数可带可不带，根据实际情况
        # df.to_excel(writer, index=False)
        # writer.save()
        # print(handle_df)
        print(datetime.datetime.now())
        handle_df.to_excel(writer, sheet_name=f'sheet{i+1}')
        print(datetime.datetime.now())

# print(df)
