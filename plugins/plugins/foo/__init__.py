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
import re
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
    left=table[0].find("【")
    right=table[0].find("】")
    table[0]=table[0][left+1:right]
    data_dir = store.get_data_dir("guzi")
    pure_data_dir = data_dir.as_posix()
    true_group_id = group_id.split('_')[1]
    sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
    conn = sqlite3.connect(sqaddress)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS table_name (xh INTEGER PRIMARY KEY AUTOINCREMENT, pbtime TEXT, swtime TEXT, tablename TEXT,peishu INTEGER,shangpai INTEGER,shoushen INTERGER)")
    current_time = datetime.datetime.now()
    current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("INSERT INTO table_name (pbtime, swtime, tablename,peishu,shangpai,shoushen) VALUES (?, ?, ?,?,?,?)", (current_time_str, current_time_str, table[0],0,0,0))
    cursor.execute("SELECT xh FROM table_name WHERE tablename = ?", (table[0],))
    result_xh = cursor.fetchone()
    xh=result_xh[0]
    gz_class=table[1].split('	')
    gz_price=table[2].split('	')
    data=[row.strip().split('	') for row in table[3:]]
    j=0;
    cursor.execute("CREATE TABLE IF NOT EXISTS xize (cn TEXT, gz_name TEXT, xh INTEGER, price INTEGER,yishen INTEGER,peishu INTEGER,yifa INTEGER,shangpai INTEGER,dinjin INTEGER)")
    for row in data:
        for i in range(len(row)):
            if((row[i]!='	')&(row[i]!='') and not row[i].isdigit()): # Add condition to check if row[i] is not a pure number
                cursor.execute("INSERT INTO xize (cn, gz_name, xh, price,yishen,peishu,yifa,shangpai,dinjin) VALUES (?,?,?,?,?,?,?,?,?)", (row[i], gz_class[i], xh, gz_price[i],0,j+1,0,0,1))
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
    cursor.execute('SELECT * FROM table_name WHERE shoushen != 0')
    result = cursor.fetchall()
    total_price=0
    msg=UniMessage("您的待肾有:\n")
    for row in result:
        cursor.execute('SELECT * FROM xize WHERE cn = ? AND yishen = 0 AND shangpai=1 AND xh=?', (name,row[0]))
        results = cursor.fetchall()
        if results:
            msg+="在排表:"+row[3]+"\n"
            for rows in results:
                msg+=rows[1]+":"+str(rows[3])+"\n"
        total_price += sum(rows[3] for rows in results)
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
    member_list=await bot.get_group_member_list(group_id=int(true_group_id)) 
    for cs_cn in cs_cn_list:
        for member in member_list:
                if member['card']==cs_cn:
                    at_list.append(member['user_id'])
                    cs_cn_list.remove(cs_cn)
                    break
    msg=UniMessage("叮咚，催肾名单:\n")
    for at in at_list:
        msg+=At("user",at)
    msg+="\n"+sb_name+" 已经可以交咯，请尽快完成哦"
    cant_find=""
    for cs_cn in cs_cn_list:
        cant_find+=cs_cn+" "
    if cant_find!="":
        msg+="\n找不到的cn有:"+cant_find
    await gaishen.send(await msg.export())
    for user_id in at_list:
        await bot.send_private_msg(user_id=int(user_id),message="这里是【大人收手吧】"+sb_name+" 已经可以交咯，请尽快完成哦")
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
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cn_list={}
        cn_price={}
        cursor = conn.cursor()
        for pb in pblist:
            cursor.execute("SELECT * FROM table_name WHERE xh = ?", (pb,))
            result = cursor.fetchall()
            if result[0][6]==0:
                cursor.execute("UPDATE table_name SET shoushen = 1 WHERE xh = ?", (pb,))
                conn.commit()
                cursor.execute("SELECT * FROM xize WHERE xh = ? AND shangpai=1", (pb,))
                result = cursor.fetchall()
                for row in result:
                    if row[0] not in cn_list:
                        cn_list[row[0]]={row[1]:1}
                        cn_price[row[0]]=row[2] if dandian=='-1' else int(dandian)
                    else:
                        if row[1] not in cn_list[row[0]]:
                            cn_list[row[0]][row[1]]=1
                        else:
                            cn_list[row[0]][row[1]]+=1
                        cn_price[row[0]]+=row[2] if dandian=='-1' else int(dandian)
            else:
                await hebin.send("有排表已经被合并过了哦,如果需要重新合并请使用修改肾表")
                return
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
        await callperson(list(cn_pb.keys()),sb_name,true_group_id,bot,event)
        await bot.upload_group_file(group_id=int(true_group_id),file=pure_data_dir +'/'+true_group_id+'_'+sb_name+'.xlsx',name=sb_name+'.xlsx')
    else:
        await hebin.send("您没有权限合并哦")



chakan=on_command("查看排表")
@chakan.handle()
async def handle_chakan(event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
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
    for row in result:
        msg+=str(row[0])+":"+row[3]+"\n"
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
async def handle_cuishen(bot:Bot,event:Event,args: Message = CommandArg()):
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
                if row[0] not in cs_cn_list:
                    cursor.execute("UPDATE xize SET yishen = 1 WHERE xh = ? AND cn = ?", (pb,row[0]))
                else:
                    cursor.execute("UPDATE xize SET yishen = 0 WHERE xh = ? AND cn = ?", (pb,row[0]))
        conn.commit()
        cursor.close()
        conn.close()
        at_list=[]
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
        member_list=await bot.get_group_member_list(group_id=int(true_group_id)) 
        for cs_cn in cs_cn_list:
            for member in member_list:
                    if member['card']==cs_cn:
                        at_list.append(member['user_id'])
                        cs_cn_list.remove(cs_cn)
                        break
        msg=UniMessage("叮咚，催肾名单:\n")
        for at in at_list:
            msg+=At("user",at)
        msg+="\n"+sb_name+" 已经可以交咯，请尽快完成哦"
        cant_find=""
        for cs_cn in cs_cn_list:
            cant_find+=cs_cn+" "
        if cant_find!="":
            msg+="\n找不到的cn有:"+cant_find
        await gaishen.send(await msg.export())
        for user_id in at_list:
            await bot.send_private_msg(user_id=int(user_id),message="这里是【大人收手吧】"+sb_name+" 已经可以交咯，请尽快完成哦")
    else:
        await gaishen.send("不是您谁？")
xiugaipaibiao=on_command("修改排表")
@xiugaipaibiao.handle()
async def handle_xiugaipaibiao(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    first=arg.find(' ')
    if first==-1:
        await xiugaipaibiao.send("格式错误")
        return
    id=int(arg[:first])
    table=arg[first+1:]
    table=table.split('\n')
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table_name WHERE xh = ?", (id,))
        result = cursor.fetchone()
        if result:
            cursor.execute("DELETE FROM xize WHERE xh = ?", (id,))
            conn.commit()
            gz_class=table[1].split('	')
            gz_price=table[2].split('	')
            data=[row.strip().split('	') for row in table[3:]]
            j=0; 
            for row in data:
                for i in range(len(row)):
                    if((row[i]!='	')&(row[i]!='') and not row[i].isdigit()): # Add condition to check if row[i] is not a pure number
                        cursor.execute("INSERT INTO xize (cn, gz_name, xh, price,yishen,peishu,yifa,shangpai) VALUES (?,?,?,?,?,?,?,?)", (row[i], gz_class[i], int(id), gz_price[i],0,j+1,0,0))
                j=j+1
            conn.commit()
            cursor.close()
            conn.close()
            data=[row.strip().split('	') for row in table[2:]]
            df = pd.DataFrame(data, columns=gz_class)#todo
            df.to_excel(pure_data_dir +'/'+true_group_id+'_'+str(id)+'.xlsx', index=False)
            await xiugaipaibiao.send("修改成功")
    else:
        await xiugaipaibiao.send("您没有权限修改排表哦")
genxinshenbiao=on_command("更新肾表")
@genxinshenbiao.handle()
async def handle_genxinshenbiao(bot:Bot,event:Event,args: Message = CommandArg()):
    arg=args.extract_plain_text()
    arg=arg.split(' ')
    sbindex=arg[0]
    group_id = event.get_session_id()
    user=event.get_user_id()
    if((user=='50191427')| (user=='1301117439')):
        data_dir = store.get_data_dir("guzi")
        pure_data_dir = data_dir.as_posix()
        true_group_id = group_id.split('_')[1]
        sqaddress=pure_data_dir +'/'+true_group_id + 'sbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sb WHERE xh = ?", (sbindex,))
        result = cursor.fetchone()
        cn_list={}
        cn_price={} 
        cursor.close()
        conn.close()
        sqaddress=pure_data_dir +'/'+true_group_id + 'pbtable.db'
        conn = sqlite3.connect(sqaddress)
        cursor = conn.cursor()
        if result:
            pblist=result[4].split(',')
            sb_name=result[3]
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
            conn.commit()
            cursor.close()
            conn.close()
            await genxinshenbiao.send("更新成功")
            cn_pb={}
            for key in cn_list:
                cn_pb[key]=mixit(cn_list[key])
            cn_wx_price={  }
            for key in cn_price:
                cn_wx_price[key]=cn_price[key]+0.01 if cn_price[key]<100 else cn_price[key]*1.01
            df = pd.DataFrame({'cn': list(cn_pb.keys()), '物品': list(cn_pb.values()), '支付宝价格': list(cn_price.values()), '微信价格': list(cn_wx_price.values())})
            df.to_excel(pure_data_dir +'/'+true_group_id+'_'+sb_name+'.xlsx', index=False)
            await bot.upload_group_file(group_id=int(true_group_id),file=pure_data_dir +'/'+true_group_id+'_'+sb_name+'.xlsx',name=sb_name+'.xlsx')
        else:
            await genxinshenbiao.send("没有这个排表哦")

    else:
        await genxinshenbiao.send("您没有权限更新肾表哦")
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
            pblist=result[4].split(',')
            for pb in pblist:
                cursor.execute("UPDATE table_name SET shoushen = 0 WHERE xh = ?", (pb,))
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

