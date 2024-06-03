from nonebot import on_command,on_request
from nonebot_plugin_alconna.uniseg import UniMessage, At,File
from nonebot.adapters.onebot.v11 import Bot, Event,PrivateMessageEvent, GroupMessageEvent
from nonebot.adapters.onebot.v11 import (MessageSegment)
from nonebot.rule import is_type
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
import re
from nonebot.adapters.onebot.v11.event import FriendRequestEvent
group = is_type(GroupMessageEvent)
pri=is_type(PrivateMessageEvent)
async   def in_binding(true_group_id,event,args):
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    user=event.get_user_id()
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
binding = on_command("绑定cn",rule=group)
@binding.handle()
async def handle_binding(event:Event,args: Message = CommandArg()):
    group_id = event.get_session_id()
    true_group_id = group_id.split('_')[1]
    await in_binding(true_group_id,event,args)
binding2 = on_command("绑定cn",rule=pri)
@binding2.handle()
async def handle_binding2(event:Event,args: Message = CommandArg()):
    await in_binding("764562701",event,args)
#绑定cn部分
async def spilttable(table,group_id,bot):
    table=table.split('\n')
    left=table[0].find("【")
    right=table[0].find("】")
    table[0]=table[0][left+1:right]
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS table_name (xh INTEGER PRIMARY KEY AUTOINCREMENT, pbtime TEXT, swtime TEXT, tablename TEXT,peishu INTEGER,shangpai INTEGER)")
    current_time = datetime.datetime.now()
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("INSERT INTO table_name (pbtime, swtime, tablename,peishu,shangpai) VALUES (?, ?, ?,?,?)", (current_time_str, current_time_str, table[0],0,0))
    cursor.execute("SELECT xh FROM table_name WHERE tablename = ?", (table[0],))
    result_xh = cursor.fetchone()
    xh=result_xh[0]
    gz_class=table[1].split('	')
    gz_price=table[2].split('	')
    data=[row.strip().split('	') for row in table[3:]]
    j=0;
    cursor.execute("CREATE TABLE IF NOT EXISTS xize (ind INTEGER PRIMARY KEY AUTOINCREMENT,cn TEXT, gz_name TEXT, xh INTEGER, price INTEGER,yishen TEXT,peishu INTEGER,yifa INTEGER,shangpai INTEGER)")
    for row in data:
        for i in range(len(row)):
            if((row[i]!='	')&(row[i]!='') and not row[i].isdigit()): # Add condition to check if row[i] is not a pure number
                cursor.execute("INSERT INTO xize (cn, gz_name, xh, price,yishen,peishu,yifa,shangpai) VALUES (?,?,?,?,?,?,?,?)", (row[i], gz_class[i], xh, gz_price[i],"0",j+1,0,0))
        j=j+1
    conn.commit()
    cursor.close()
    conn.close()
    data=[row.strip().split('	') for row in table[2:]]
    df = pd.DataFrame(data, columns=gz_class)#todo
    df.to_excel(pure_data_dir +'/'+true_group_id+'_'+str(xh)+'.xlsx', index=False)
    await bot.upload_group_file(group_id=int(true_group_id),file=pure_data_dir +'/'+true_group_id+'_'+str(xh)+'.xlsx',name=table[0]+'.xlsx')
    return xh



gettable=on_command("导入排表")
@gettable.handle()
async def handle_gettable(bot:Bot,event:Event,args: Message = CommandArg()):
    await gettable.send("正在导入排表")
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):#todo:添加管理员
        xh=await spilttable(args.extract_plain_text(),group_id,bot)
        await gettable.send("导入成功,他的id是:"+str(xh))
    else:
        await gettable.send("您没有权限导入排表哦")


#查询肾额
def cal(name,true_group_id):
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    sqaddress=pure_data_dir +'/'+true_group_id + 'sbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sb")
    result = cursor.fetchall()
    money={}
    names={}
    for row in result:
        money[row[0]]=row[5]
        names[row[0]]=row[3]
    total=0
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM xize WHERE cn = ? AND yishen <> "0"', (name,))
    result = cursor.fetchall()
    msg=UniMessage("您的肾额情况如下:\n")
    for row in result:
        shoushen=row[5]
        shoushen=shoushen.split(',')
        for sb in shoushen:
            total+=row[4]-money[int(sb)] if money[int(sb)]<=0 else money[int(sb)]
            msg+=names[int(sb)]+":"+row[2]+" "+str(row[4]-money[int(sb)] if money[int(sb)]<=0 else money[int(sb)])+"元\n"
    msg+="总计:"+str(total)+"元"
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
async def  callperson(cs_cn_list,sb_name,true_group_id,bot,event):
    at_list=[]
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor() 
    for cs_cn in cs_cn_list:
        cursor.execute("SELECT * FROM table_name WHERE cn = ?", (cs_cn,))
        result = cursor.fetchone()
        if result:
            cs_cn_list.remove(cs_cn)
            at_list.append(result[0])
    cursor.close()
    conn.close()
    at_list2=[]
    member_list=await bot.get_group_member_list(group_id=int(true_group_id)) 
    for cs_cn in cs_cn_list:
        for member in member_list:
                if cs_cn in member['card']:
                    at_list2.append(member['user_id'])
                    cs_cn_list.remove(cs_cn)
                    break
    msg=UniMessage("💸叮咚，催肾名单:\n")
    for at in at_list:
        msg+=At("user",at)
    for at in at_list:
        msg+=At("user",at)
    msg+="\n😈"+sb_name
    cant_find=""
    for cs_cn in cs_cn_list:
        cant_find+=cs_cn+" "
    if cant_find!="":
        msg+="\n😡找不到的cn有:"+cant_find
    await gaishen.send(await msg.export())
    for user_id in at_list:
        await bot.send_private_msg(user_id=int(user_id),message="这里是【大人收手吧】"+sb_name)
@hebin.handle()
async def handle_hebin(bot:Bot,event:Event,args: Message = CommandArg()):
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        arg=args.extract_plain_text()
        arg=arg.split(' ')
        sb_name=arg[0]
        dandian=arg[1]
        group_id = event.get_session_id()
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        pblist=arg[2:]
        sqaddress=pure_data_dir +'/'+true_group_id + 'sbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS sb (xh INTEGER PRIMARY KEY AUTOINCREMENT, sstime TEXT, ddltime TEXT, sbname TEXT,include TEXT,dandian INTERGER)")
        current_time = datetime.datetime.now()
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        ddl_time = current_time + datetime.timedelta(days=14)
        ddl_time_str = ddl_time.strftime("%Y-%m-%d %H:%M:%S")
        pblist=','.join(pblist)
        cursor.execute("INSERT INTO sb (sstime, ddltime, sbname,include,dandian) VALUES (?, ?, ?,?,?)", (current_time_str, ddl_time_str, sb_name,pblist,dandian))
        cursor.execute("SELECT xh FROM sb WHERE sbname = ?", (sb_name,))
        result_xh = cursor.fetchone()
        xh=result_xh[0]
        await hebin.send("合并成功,他的id是:"+str(xh))
        conn.commit()
        cursor.close()
        conn.close()
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cn_list={}
        cn_price={}
        cursor = conn.cursor()
        for pb in pblist:
            cursor.execute("SELECT * FROM xize WHERE xh = ? AND shangpai=1", (pb,))
            result = cursor.fetchall()
            for row in result:
                if row[1] not in cn_list:
                    cn_list[row[1]]={row[2]:1}
                    cn_price[row[1]]=row[4]+float(dandian) if float(dandian)<=0 else float(dandian)
                else:
                    if row[2] not in cn_list[row[1]]:
                        cn_list[row[1]][row[2]]=1
                    else:
                        cn_list[row[1]][row[2]]+=1
                    cn_price[row[1]]+=row[4]+float(dandian) if float(dandian)<=0 else float(dandian)
                muqian=row[5]
                muqian=muqian.split(',')
                if(muqian==['0']):
                    muqian=[str(xh)]
                else:
                    muqian+=[str(xh)]
                muqian=','.join(muqian)
                cursor.execute("UPDATE xize SET yishen=? WHERE ind=?", (muqian,row[0]))
        conn.commit()
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
        await callperson(list(cn_pb.keys()),sb_name+" 已经可以交咯，请尽快完成哦",true_group_id,bot,event)
        await bot.upload_group_file(group_id=int(true_group_id),file=pure_data_dir +'/'+true_group_id+'_'+sb_name+'.xlsx',name=sb_name+'.xlsx')
    else:
        await hebin.send("您没有权限合并哦")



chakan=on_command("查看排表")
@chakan.handle()
async def handle_chakan(event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    if arg=='':
        arg=-1
    else: arg=int(arg)
    group_id = event.get_session_id()
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name")
    result = cursor.fetchall()
    msg=UniMessage("排表列表:\n")
    length=len(result)
    if arg==-1:
        start=0
        end=length
    else:
        if arg*10<length:
            start=arg*10
            end=(arg+1)*10 if (arg+1)*10<length else length
        else:
            start=0
            end=0
    for i in range(start,end):
        msg+=str(result[i][0])+":"+result[i][2]+"\n"
    await chakan.send(await msg.export())
    cursor.close()
    conn.close()
shanchu=on_command("删除排表")
@shanchu.handle()
async def handle_shanchu(event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table_name WHERE xh = ?", (arg,))
        result = cursor.fetchone()
        if result: 
            cursor.execute("DELETE FROM table_name WHERE xh = ?", (arg,))
            cursor.execute("DELETE FROM xize WHERE xh = ?", (arg,))
            conn.commit()
            await shanchu.send("删除成功")
        else:
            await shanchu.send("没有这个排表哦")
        cursor.close()
        conn.close()
    else:
        await shanchu.send("您没有权限删除哦")
gaishen=on_command("改肾")
@gaishen.handle()
async def handle_gaishen(bot:Bot,event:Event,args: Message = CommandArg()):
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        arg=args.extract_plain_text()
        arg=arg.split(' ')
        sb_id=arg[0]
        cs_cn_list=arg[1:]
        group_id = event.get_session_id()
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'sbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sb WHERE xh = ?", (sb_id,))
        result = cursor.fetchall()
        pblist=result[0][4].split(',')
        sb_name=result[0][3]
        cursor.close()
        conn.close()
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        for pb in pblist:
            cursor.execute("SELECT * FROM xize WHERE xh = ?", (pb))
            result = cursor.fetchall()
            for row in result:
                if row[1] not in cs_cn_list:
                    yishen=row[5]
                    yishen=yishen.split(',')
                    if sb_id in yishen:
                        yishen.remove(sb_id)
                        if(yishen==[]):
                            yishen="0"
                        else:
                            yishen=','.join(yishen)
                        cursor.execute("UPDATE xize SET yishen = ? WHERE ind = ?", (yishen,row[0]))
                else:
                    yishen=row[5]
                    yishen=yishen.split(',')
                    if sb_id not in yishen:
                        if(yishen==['0']):
                            yishen=[sb_id]
                        else:
                            yishen+=[sb_id]
                            yishen=','.join(yishen)
                        cursor.execute("UPDATE xize SET yishen = ? WHERE ind = ?", (yishen,row[0]))
        await gaishen.send("改肾成功")
        conn.commit()
        cursor.close()
        conn.close()
    else:
        await gaishen.send("不是您谁？")
desb=on_command("删除肾表")
@desb.handle()
async def handle_desb(event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'sbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sb WHERE xh = ?", (arg,))
        result = cursor.fetchone()
        if result:
            cursor.execute("DELETE FROM sb WHERE xh = ?", (arg,))
            conn.commit()
            cursor.close()
            conn.close()
            sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
            conn = sqlite3.connect(sqaddress)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xize")
            result = cursor.fetchall()
            for row in result:
                yishen=row[5]
                yishen=yishen.split(',')
                if arg in yishen:
                    yishen.remove(arg)
                    if(yishen==[]):
                        yishen="0"
                    else:
                        yishen=','.join(yishen)
                    cursor.execute("UPDATE xize SET yishen = ? WHERE ind = ?", (yishen,row[0]))
            conn.commit()
            cursor.close()
            conn.close()
            await desb.send("删除成功")
        else:
            await desb.send("没有这个肾表哦")
        cursor.close()
        conn.close()
    else:
        await desb.send("您没有权限删除哦")
cuishen=on_command("催肾")
@cuishen.handle()
async def handle_cuishen(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    if(arg==""):
        tongji=1
    else:   
        tongji=0
        sb_id=arg.split(' ')
    user=event.get_user_id()
    if(user=='50191427' or user=='1301117439'):
        group_id = event.get_session_id()
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM xize WHERE yishen <> '0'")
        result = cursor.fetchall()
        cs_cn_list=[]
        for row in result:
            yishen=row[5]
            yishen=yishen.split(',')
            if tongji==1 or sb_id in yishen:
                if row[1] not in cs_cn_list:
                    cs_cn_list.append(row[1])
        cursor.close()
        conn.close()
        await callperson(cs_cn_list,"在【大人收手吧】的肾表"+" 已经可以交咯，请尽快完成哦",true_group_id,bot,event)
        await cuishen.send("催肾成功")
    else:
        await cuishen.send("不是您谁？")
friendadd=on_request()
@friendadd.handle()
async def handle_friendadd(bot:Bot,event:FriendRequestEvent):
    await bot.set_friend_add_request(flag=event.flag, approve=True, remark="")
    await bot.send_private_msg(user_id=int(event.user_id),message="您好，我是哆来咪，记得绑定cn哦")
zhuanren=on_command("转让")
@zhuanren.handle()
async def handle_zhuanren(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    arg=arg.split(' ')
    group_id = event.get_session_id()
    user=event.get_user_id()
    tocn=arg[0]
    pbxh=int(arg[1])
    paixu=int(arg[3])
    zhipin=arg[2]
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name WHERE id = ?", (user,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    if result:
        cn=result[0][1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM xize WHERE cn= ? AND xh = ? AND peishu = ? AND gz_name=?", (cn,pbxh,paixu,zhipin))
        result = cursor.fetchone()
        if result:
            cursor.execute("CREATE TABLE IF NOT EXISTS zhuanrang (ind INTEGER PRIMARY KEY AUTOINCREMENT, zhuanren TEXT, beizhuren TEXT, xh INTEGER, peishu INTEGER, zhipin TEXT)")
            cursor.execute("INSERT INTO zhuanrang (zhuanren, beizhuren, xh, peishu, zhipin) VALUES (?, ?, ?, ?, ?)", (cn,tocn,pbxh,paixu,zhipin))
            conn.commit()
            cursor.execute("SELECT * FROM zhuanrang WHERE zhuanren = ? AND beizhuren = ? AND xh = ? AND peishu = ? AND zhipin=?", (cn,tocn,pbxh,paixu,zhipin))
            result = cursor.fetchall()
            id=result[0][0]
            await zhuanren.send("转让已经记录，请等待接单者发送/接 "+str(id)+",请注意，在转入之前，这个制品仍然会在您这")
        else:
            await zhuanren.send("没有找到符合项，请检查是否为自己的排表或者是否输入错误，格式为/转让 对方cn 排表号 制品 排序")
    else:
        await zhuanren.send("您还没有绑定cn哦")
    cursor.close()
    conn.close()
jiedan=on_command("接")
@jiedan.handle()
async def handle_jiedan(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    group_id = event.get_session_id()
    user=event.get_user_id()
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
    conn = sqlite3.connect(sqaddress)
    arg=arg.split(' ')
    id=int(arg[0])
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name WHERE id = ?", (user,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    cn=result[0][1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM zhuanrang WHERE ind = ?", (id,))
    result = cursor.fetchall()
    if result:
        if (result[0][2]==cn) or (result[0][2]=='0'):
            cursor.execute("SELECT * FROM xize WHERE xh = ? AND peishu = ? AND gz_name=?", (result[0][3],result[0][4],result[0][5]))
            result = cursor.fetchone()
            if result:
                cursor.execute("UPDATE xize SET cn = ? WHERE ind = ?", (cn,result[0]))
                cursor.execute("DELETE FROM zhuanrang WHERE ind = ?", (id,))
                conn.commit()
                await jiedan.send("接单成功")
                if result[6]!='0':
                    await jiedan.send("请注意，这个制品存在未付清的肾哦，请联系管理更新肾表，或者在接单后自行付清")
            else:
                await jiedan.send("没有找到符合项，请检查是否为自己的排表或者是否输入错误")
        else:
            await jiedan.send("这不是您的转让哦")
    else:
        await jiedan.send("没有找到这个转让哦")
    cursor.close()
    conn.close()
diaoluo=on_command("掉落")
@diaoluo.handle()
async def handle_diaoluo(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    arg=arg.split(' ')
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        pbxh=int(arg[0])
        paixu=int(arg[2])
        zhipin=arg[1]
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table_name WHERE id = ?", (user,))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        if result:
            cn=result[0][1]
            sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
            conn = sqlite3.connect(sqaddress)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xize WHERE xh = ? AND peishu = ? AND gz_name=?", (pbxh,paixu,zhipin))
            result = cursor.fetchone()
            if result:
                cursor.execute("CREATE TABLE IF NOT EXISTS zhuanrang (ind INTEGER PRIMARY KEY AUTOINCREMENT, zhuanren TEXT, beizhuren TEXT, xh INTEGER, peishu INTEGER, zhipin TEXT)")
                cursor.execute("INSERT INTO zhuanrang (zhuanren, beizhuren, xh, peishu, zhipin) VALUES (?, ?, ?, ?, ?)", (cn,str(0),pbxh,paixu,zhipin))
                conn.commit()
                cursor.execute("SELECT * FROM zhuanrang WHERE zhuanren = ? AND beizhuren = ? AND xh = ? AND peishu = ? AND zhipin=?", (cn,str(0),pbxh,paixu,zhipin))
                result = cursor.fetchall()
                id=result[0][0]
                await zhuanren.send("掉落已经记录，请等待接单者发送/接 "+str(id)+",请注意，在转入之前，这个制品仍然会在原主这")
            else:
                await zhuanren.send("没有找到符合项，请检查是否为自己的排表或者是否输入错误，格式为/转让 对方cn 排表号 制品 排序")
        else:
            await zhuanren.send("您还没有绑定cn哦")
        cursor.close()
        conn.close()
    else:
        await zhuanren.send("不是您谁？")
shanchuzr=on_command("删除转让")
@shanchuzr.handle()
async def handle_shanchuzr(event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    group_id = event.get_session_id()
    user=event.get_user_id()
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'cn.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name WHERE id = ?", (user,))
    cn=cursor.fetchone()[1]
    cursor.close()
    conn.close()
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM zhuanrang WHERE ind = ?", (arg,))
    result = cursor.fetchone()
    if result:
        if result[1]==cn or (user=='50191427')or (user=='1301117439'):
            cursor.execute("DELETE FROM zhuanrang WHERE ind = ?", (arg,))
            conn.commit()
            await shanchuzr.send("删除成功")
    else:
        await shanchuzr.send("没有这个转让哦")
    cursor.close()
    conn.close()
askperson=on_command("通知")
@askperson.handle()
async def handle_askperson(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    arg=arg.split(' ')
    neirong=arg[0]
    pb=arg[1:]
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        cs_cn_list=[]
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        for pbxh in pb:
            cursor.execute("SELECT * FROM xize WHERE xh = ?", (pbxh,))
            result = cursor.fetchall()
            for row in result:
                if row[1] not in cs_cn_list:
                    cs_cn_list.append(row[1])
        await callperson(cs_cn_list,neirong,true_group_id,bot,event)
        await askperson.send("通知成功")
    else:
        await askperson.send("不是您谁？")