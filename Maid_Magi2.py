import json
import sqlite3
from collections import OrderedDict
import pandas as pd
from datetime import datetime
import os
import discord
from discord.ext import commands



token = os.environ.get('BOT_TOKEN') # 봇 토큰  # 봇 토큰 

intents = discord.Intents.default()
intents.members = True
client = commands.Bot(command_prefix='!',intents=intents)

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
        db = sqlite3.connect(f'Data_{ser}.db')
        cur = db.cursor()
        table_list = cur.execute(f'select name from sqlite_master where type="table";').fetchall()
        target_table = (self.goods,)
        if target_table in table_list:
            query=cur.execute(f'select * from {self.goods}')
            cols = [col[0] for col in query.description]
            race_result = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
            race_result = race_result.values.tolist()
            Out = []
            for data in race_result:
                for i in range(len(data)):
                    if data[i] == None:
                        data[i] ='None'
                str_data = '\t'.join(data)
                Out.append(str_data)
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

up=Uploader()
down=Downloader()

@client.event
async def on_ready(): # when bot get ready
    print('Hello World!')
    print(client.user)
    print('==========================================')

@client.command(aliases=['ㅅㅅ','ㅆ','시세']) #ㅅㅅ,ㅆ,시세에 명령어 작동
async def price(ctx, goods=None,server= None,*,message=None):
    goods = goods
    server = server
    message = message
    if message == None:
        datas = down.call_back(goods,server)
        if datas ==[]:      # call_back에 의한 자료가 없을 경우
            await ctx.send('죄송해요..... 입력된 자료가 없어요......ㅠㅠ')
        else:               # call_back에 의한 자료가 있는 경우
            embed = discord.Embed(title="시세 받아라~", description='물어보신 교역품과 서버 시세에요!',color=discord.Color.random())
            for i, data in enumerate(datas):
                embed.add_field(name=f'{i+1}번째 기록된 도시예요!', value=data,inline= False)
            await ctx.send(embed=embed)
    else:
        up.upload(goods,server,message)
        await ctx.send('소중한 정보를 주셔서 감사합니당!')
    
@client.command(aliases=['ㄱㅇㅍ','교역품'])
async def trades(ctx, goods):
    goods, cate, cul_city = down.goods_call(goods)
    embed = discord.Embed(title=f'{goods}', desciption=f'{cate}',color=discord.Color.random())
    for key, value in cul_city.items():
        embed.add_field(name=f'{key}',value=f'{value}',inline=False)
    await ctx.send(embed=embed)

@client.command(aliases=['ㅁㅎㄱ','문화권'])
async def cultures(ctx,*,culture):
    culture = culture
    data= down.culture_call(culture)
    embed = discord.Embed(title=f'{culture}',description=f'{data}', color=discord.Color.random())
    await ctx.send(embed=embed)
    
if __name__=="__main__":
    client.run(token)