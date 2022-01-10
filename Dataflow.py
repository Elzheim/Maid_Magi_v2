import json
import sqlite3
from collections import OrderedDict
import pandas as pd
from datetime import datetime

class Uploader():
    def __init__(self, goods=None, server=None, data=None):
        self.goods = goods
        self.server = server
        self.data = data
    
    def line_split(data):
        Out = data.split('\n')
        return Out
    
    def data_digest(self,data):
        line_list = Uploader.line_split(data)
        stack=[None]*len(line_list)
        for i, line in enumerate(line_list):
            list_in = line.split()
            stack[i]=list_in
        stack= pd.DataFrame(stack,columns=['City','Price','Trend','Drop'])
        now = datetime.now()
        self.date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        stack.insert(4,'Time',[self.date_time]*len(stack))
        stack_json = OrderedDict()
        stack_json[self.goods] = {self.server : stack.to_dict('records')}
        return stack

    def upload(self, goods=None, server=None, data=None):
        self.goods = goods
        self.server = server.upper()
        self.data = Uploader.data_digest(self,data)
        target = (self.goods,)
        conn = sqlite3.connect(f'Data_{self.server}.db',isolation_level=None)
        cur = conn.cursor() 
        table_list = cur.execute(f'select name from sqlite_master where type="table";').fetchall()
        if target in table_list:
            query = cur.execute(f'select * from {self.goods}')
            cols = [column[0] for column in query.description]
            target_pd = pd.DataFrame(data=query.fetchall(), columns=cols)
            target_pd.update(self.data)
            self.data = pd.concat([target_pd,self.data]).drop_duplicates()
            self.data.to_sql(f'{self.goods}',conn,if_exists='replace', index=False)
        else:
            self.data.to_sql(f'{self.goods}',conn,if_exists='append', index=False)
        
        conn.commit()
        conn.close()

class Downloader():
    def __init__(self, goods = None, server = None):
        self.goods = goods
        self.server = server

    def call_back(self, goods=None, server=None):
        self.goods = goods
        self.server = server
        if self.goods != None and self.server == None:
            return Downloader.goods_only(self)

        elif self.goods!= None and self.server!=None:
            return Downloader.goods_server(self)
        else:
            raise AttributeError

    def goods_only(self):
        server = ['A','B']
        data = {}
        for ser in server:
            data[ser] = Downloader.parser(self, ser)
        data_A = data['A']
        data_B = data['B']
        return data_A, data_B
    
    def goods_server(self):
        server = self.server
        data = Downloader.parser(self,server)
        return data

    def parser(self, ser):
        Out = []
        db = sqlite3.connect(f'Data_{ser}.db')
        cur = db.cursor()
        table_list = cur.execute(f'select name from sqlite_master where type="table";').fetchall()
        target_table = (self.goods,)
        print(table_list)
        if target_table in table_list:
            query=cur.execute(f'select * from {self.goods}')
            cols = [col[0] for col in query.description]
            race_result = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
            race_result = race_result.values.tolist()
            for data in race_result:
                for i in range(len(data)):
                    if data[i] == None:
                        data[i] ='None'
                str_data = '\t'.join(data)
                Out.append(str_data)
        print(Out)
        return Out

    def goods_call(self, goods):
        with open('Goods.json','r',encoding='UTF-8') as js:
            json_data = json.load(js)
        data = json_data[goods]
        return data
    
    def culture_call(self, culture):
        with open('City.json','r',encoding='UTF-8') as js:
            json_data = json.load(js)
        data = json_data[culture]
        print(data)

ud = Uploader()
goods = '명주'
server = 'b'
data = '런던 128 상 향신폭\n더블린 120 상 미술폭\n플리머스 110 하 '
#ud.upload(goods,server,data)

dl = Downloader()
dl.call_back(goods)
#print(dl.call_back(goods))
#dl.call_back(goods,server)
#dl.goods_call(goods)
#dl.culture_call('브리튼 섬')