from nonebot import on_command
from nonebot_plugin_alconna.uniseg import UniMessage, At,File
from nonebot.adapters.onebot.v11 import Bot, Event
from nonebot.adapters.onebot.v11 import (MessageSegment)
from nonebot.params import ArgPlainText
import sqlite3
from nonebot import require
from nonebot.adapters import Message
import nonebot_plugin_localstore as store
from nonebot.params import CommandArg
from pathlib import Path
from pathlib import PurePath
import datetime
import pandas as pd
binding = on_command("绑定cn")
@binding.handle()
async def handle_binding(event:Event,args: Message = CommandArg()):
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    group_id = event.get_session_id()
    user=event.get_user_id()
    true_group_id = group_id.split('_')[1]
    cn=args.extract_plain_text()
    sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS table_name (id INTEGER PRIMARY KEY, cn TEXT)")
    cursor.execute("SELECT * FROM table_name WHERE id = ?", (user,))
    result_id = cursor.fetchone()
    cursor.execute("SELECT * FROM table_name WHERE cn = ?", (cn,))
    result_cn = cursor.fetchone()
    if result_id:
        await binding.send("您在本群似乎已经有注册过cn了哦")
    else:
        if result_cn:
            await binding.send("这个cn已经被注册过了哦")
        else:
            cursor.execute("INSERT INTO table_name (id, cn) VALUES (?, ?)", (user, cn))
            conn.commit()
            await binding.send("绑定成功")
    cursor.close()
    conn.close()
#绑定cn部分
def spilttable(table,group_id):
    table=table.split('\n')
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS table_name (xh INTEGER PRIMARY KEY AUTOINCREMENT, pbtime TEXT, swtime TEXT, tablename TEXT,peishu INTEGER,shangpai INTEGER)")
    current_time = datetime.datetime.now()
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("INSERT INTO table_name (pbtime, swtime, tablename,peishu) VALUES (?, ?, ?,?)", (current_time_str, current_time_str, table[0],0))
    cursor.execute("SELECT xh FROM table_name WHERE tablename = ?", (table[0],))
    result_xh = cursor.fetchone()
    xh=result_xh[0]
    gz_class=table[1].split('	')
    gz_price=table[2].split('	')
    data=[row.strip().split('	') for row in table[3:]]
    j=0;
    cursor.execute("CREATE TABLE IF NOT EXISTS xize (cn TEXT, gz_name TEXT, xh INTEGER, price INTEGER,yishen INTEGER,peishu INTEGER,yifa INTEGER,shangpai INTEGER)")
    for row in data:
        for i in range(len(row)):
            if((row[i]!='	')&(row[i]!='') and not row[i].isdigit()): # Add condition to check if row[i] is not a pure number
                cursor.execute("INSERT INTO xize (cn, gz_name, xh, price,yishen,peishu,yifa,shangpai) VALUES (?,?,?,?,?,?,?,?)", (row[i], gz_class[i], xh, gz_price[i],0,j+1,0,0))
        j=j+1
    conn.commit()
    cursor.close()
    conn.close()
    data=[row.strip().split('	') for row in table[2:]]
    df = pd.DataFrame(data, columns=gz_class)#todo
    df.to_excel(pure_data_dir +'/'+true_group_id+'_'+str(xh)+'.xlsx', index=False)
    return xh



gettable=on_command("导入排表")
@gettable.handle()
async def handle_gettable(event:Event,args: Message = CommandArg()):
    await gettable.send("正在导入排表")
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):#todo:添加管理员
        xh=spilttable(args.extract_plain_text(),group_id)
        await gettable.send("导入成功,他的id是:"+str(xh))
    else:
        await gettable.send("您没有权限导入排表哦")
#查询肾额
def cal(name,true_group_id):
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM xize WHERE cn = ? AND yishen = 0 AND shangpai=1', (name,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    msg=UniMessage("您的待肾有:\n")
    for row in result:
        msg+=row[1]+":"+str(row[3])+"\n"
    total_price = sum(row[3] for row in result)
    msg+="总计:"+str(total_price)
    return msg

howmuch=on_command("查询肾额")
@howmuch.handle()
async def handle_howmuch(event:Event,args: Message = CommandArg()):
    group_id = event.get_session_id()
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    name=event.get_user_id()
    cursor.execute("SELECT * FROM table_name WHERE id = ?", (name,))
    result = cursor.fetchone()
    if result:
        total=cal(result[1],true_group_id)
        await howmuch.send(await total.export())
        await howmuch.send("这个数据并不是实时更新的哦")
    else:
        await howmuch.send("您还没有绑定cn哦")

tiaopei=on_command("调配")
@tiaopei.handle()
async def handle_tiaopei(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    arg=arg.split(' ')
    id=arg[0]
    peishu=int(arg[1])
    group_id = event.get_session_id()
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table_name WHERE xh = ?", (id,))
        result = cursor.fetchone()
        if result:
            cursor.execute("UPDATE table_name SET peishu = ? WHERE xh = ?", (peishu,id))
            if peishu==-1:
                cursor.execute("UPDATE xize SET shangpai=1 WHERE xh = ?", (id,))
            else:
                cursor.execute("UPDATE xize SET shangpai=1 WHERE xh = ? AND peishu<=?", (id,peishu))
            conn.commit()
            await tiaopei.send("调配成功")
        else:
            await tiaopei.send("没有这个排表哦")
    else:
        await tiaopei.send("您没有权限调配哦")
    cursor.close()
    conn.close()
def mixit(dic):
    stri=''
    for key in dic:
        stri+=key+'*'+str(dic[key])+' '
    return stri
hebin=on_command("合并")
@hebin.handle()
async def handle_hebin(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    arg=arg.split(' ')
    sb_name=arg[0]
    group_id = event.get_session_id()
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    pblist=arg[1:]
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cn_list={}
    cn_price={}
    cursor = conn.cursor()
    for pb in pblist:
        cursor.execute("SELECT * FROM xize WHERE xh = ? AND shangpai=1", (pb,))
        result = cursor.fetchall()
        for row in result:
            if row[0] not in cn_list:
                cn_list[row[0]]={row[1]:1}
                cn_price[row[0]]=row[2]
            else:
                if row[1] not in cn_list[row[0]]:
                    cn_list[row[0]][row[1]]=1
                else:
                    cn_list[row[0]][row[1]]+=1
                cn_price[row[0]]+=row[2]
    cursor.close()
    conn.close()
    cn_pb={}
    for key in cn_list:
        cn_pb[key]=mixit(cn_list[key])
    cn_wx_price={  }
    for key in cn_price:
        cn_wx_price[key]=cn_price[key]+0.01 if cn_price[key]<100 else cn_price[key]*1.01
    df = pd.DataFrame({'cn': list(cn_pb.keys()), '物品': list(cn_pb.values()), '支付宝价格': list(cn_price.values()), '微信价格': list(cn_wx_price.values())})
    df.to_excel(pure_data_dir +'/'+true_group_id+'_'+sb_name+'.xlsx', index=False)
    await hebin.send("合并成功")
    await bot.upload_group_file(group_id=int(true_group_id),file=pure_data_dir +'/'+true_group_id+'_'+sb_name+'.xlsx',name=sb_name+'.xlsx')