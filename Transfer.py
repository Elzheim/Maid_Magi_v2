import json
from collections import OrderedDict
import gspread
from gspread import worksheet
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

scope = [
'https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive',
]
json_file_name = 'maidmagi-2f63aacdce5f.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_name, scope)
gc = gspread.authorize(credentials)
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1G-GQQDwm3LwbH9LjuTU2AlCg6Sq18dUswcDIsd-rID4/edit?usp=sharing'
# 스프레스시트 문서 가져오기 
doc = gc.open_by_url(spreadsheet_url)
# 시트 선택하기
worksheet_fixed = doc.worksheet('교역품')

Goods_data = worksheet_fixed.col_values(1)

data = OrderedDict()
data_dic= OrderedDict()
for Goods in Goods_data:
    cells = worksheet_fixed.find(Goods)
    lines = worksheet_fixed.row_values(cells.row)
    goods = lines[0]
    cate = lines[1]
    chunk = lines[2:]
    data[goods] = {'Category':cate , 'Culture' :{chunk[i]:chunk[i+1] for i in range(0,len(chunk),2)}}
    

with open('Goods_2.json','w') as make_file:
    json.dump(data, make_file, ensure_ascii=False,indent='\t')