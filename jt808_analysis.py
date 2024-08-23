#usr/bin/env python
#conding:utf-8
# JT808设备协议解析
# oldk 2021.3.24
import re
import datetime
import binascii


def BCC_Check(data):
    data = data.replace(' ', '')
    n = int(len(data) / 2)
    xor = 0
    for r in range(n):
        xor ^= int(data[2 * r:2 * r + 2], 16)
    xor = hex(xor)

    if len(xor) != 4:
        xor = '0' + xor[2:]
    else:
        # 返回16进制字符串
        xor = xor[2:]
    return xor

def loca_report(rule,data):
    loca_data={}
    rule=rule+r'(?P<alarm_data>\w{8})(?P<status>\w{8})(?P<lat>\w{8})(?P<lng>\w{8})(?P<hight>\w{4})(?P<speed>\w{4})(?P<dirct>\w{4})(?P<dev_upload>\w{12})'
    attch_data=data[len(re.search(rule,data).group()):]
    data=re.search(rule,data).groupdict()
    
    # print(data)
    # print('attch_data',attch_data)

    alarm_data=data['alarm_data']
    alarm_happen=''
    for x in range(len(alarm_data)):
        alarm_happen+='{:04b}'.format(int(alarm_data[x],16))

    all_alarm=[]
    if alarm_happen[-1]=='1':
        all_alarm.append('sos_alarm')#紧急报警

    if alarm_happen[-2]=='1':
        all_alarm.append('speed_alarm')#超速

    if alarm_happen[-3]=='1':
        all_alarm.append('fatigue_drive')#疲劳驾驶

    if alarm_happen[-6]=='1':
        all_alarm.append('antenna_alarm')#天线故障

    if alarm_happen[-8]=='1':
        all_alarm.append('low_power')#低电压报警

    if alarm_happen[-9]=='1':
        all_alarm.append('power_cut')#断电报警

    if alarm_happen[-18]=='1':
        all_alarm.append('remove_alarm')#拆除报警

    if alarm_happen[-29]=='1':
        all_alarm.append('illegal_move_alarm')#非法位移报警
    # print(all_alarm)
    status=data['status']
    status_happen=''
    for x in range(len(status)):
        status_happen+='{:04b}'.format(int(status[x],16))
    all_status=[]
    if status_happen[-1]=='1':
        all_status.append('acc_on')
    else:
        all_status.append('acc_off')

    if status_happen[-2]=='1':
        all_status.append('track')
    else:
        all_status.append('un_track')

    if status_happen[-11]=='0':
        all_status.append('oil_normal')
    else:
        all_status.append('oil_cut')

    if status_happen[-12]=='0':
        all_status.append('ele_normal')
    else:
        all_status.append('ele_cut')


    lat=int(data['lat'], 16) / pow(10, 6)
    lng=int(data['lng'], 16) / pow(10, 6)
    hight=str(int(data['hight'],16))
    speed=str(int((int(data['speed'],16)/10)))
    dirct=str(int(data['dirct'],16))
    dev_upload=data['dev_upload']
    dev_upload='20%s-%s-%s %s:%s:%s'%(dev_upload[0:2],dev_upload[2:4],dev_upload[4:6],dev_upload[6:8],dev_upload[8:10],dev_upload[10:12])
    # print(data)
    if attch_data:
        
        # print('attch_data',attch_data)
        
        def handle_atth(d):
            attch_rule=r'(?P<msg_id>\w{2})(?P<msg_len>\w{2})'
            # print('d==>',d)
            msg_head=d[0:4]
            
            res=re.search(attch_rule,msg_head).groupdict()
            # print('msg_head==>',res)
            msg_id=res['msg_id']
            msg_len=int(res['msg_len'],16)*2
            value=d[4:4+msg_len]
            # print('value==>',value)
            if msg_id=='01':
                value=int(value,16)/10
                loca_data['mileage']=value#里程
            elif msg_id=='02':
                value=int(value,16)/10
                loca_data['oil']=value#油耗
            elif msg_id=='03':
                value=int(value,16)/10
                loca_data['speed']=value#速度
            elif msg_id=='30':
                value=int(value,16)
                loca_data['rssi']=value#通讯信号强度
            elif msg_id=='31':
                value=int(value,16)
                loca_data['gnss_num']=value#定位卫星颗数
            else:
                return

            handle_len=4+msg_len
            next_data=d[handle_len:]
            if len(next_data)>0:
                 handle_atth(next_data)

        handle_atth(attch_data)


    loca_data.update(lat=lat,lng=lng,alarm=str(all_alarm),status=str(all_status),hight=hight,speed=speed,dirct=dirct,dev_upload=dev_upload)
    return loca_data


def jt808_resopnes(responekind,mesg_head,mesg_result):
    response='0'
    auth='313233343536'
    mesg_id=mesg_head['mesg_id']
    phone_num=mesg_head['phone_num']
    mesg_num=mesg_head['mesg_num']
    
    if responekind=='8001':
        mesg_perp=  '%04x' % int(len(mesg_num+mesg_id)/2 + 1)
        response_body=responekind+mesg_perp+phone_num+'0000'+mesg_num+mesg_id+mesg_result
    elif responekind=='8100':
        mesg_perp=  '%04x' % int(len(mesg_num+auth)/2 + 1)
        response_body=responekind+mesg_perp+phone_num+'0000'+mesg_num+mesg_result+auth
    response_check=BCC_Check(response_body)
    response_body=response_body+response_check
    response_body=response_body.replace('7d','7d01')
    response='7e'+response_body.replace('7e','7d02')+'7e'
    response=binascii.a2b_hex(response)
    return response


def jt808_analysis(data,serv_receive,device_id='0'):

    response=b'0'
    all_data={}
    all_data['device_id']=device_id
    all_data['save_kind']='no'
    all_data['serv_receive']=serv_receive
    # ascii转16进制字符串
    data=binascii.hexlify(data).decode()
    # print(data)
    # 去掉头尾标识符，进行转译处理
    data=data[2:-2].replace('7d02','7e')
    data=data.replace('7d01','7d')
    # 校验
    if BCC_Check(data[0:-2]) == data[-2:]:
        print('check-ok!')
    else:
        print('chec—failed')
        all_data['response']=response
        
        return all_data
    # 去掉校验码，获取需要解析的数据字段
    data=data[0:-2]
    # 数据头处理，命令id，是否包含分包处理，电话号码，数据流水号
    rule=r'(?P<mesg_id>\w{4})(?P<mesg_explain>\w{4})(?P<phone_num>\w{12})(?P<mesg_num>\w{4})'
    head_len=len(re.search(rule,data).group())
    result=re.search(rule,data).groupdict()
    # print(result)
    mesg_id=result['mesg_id']
    phone_num=result['phone_num']
    mesg_num=result['mesg_num']
    all_data['device_id']=str(int(phone_num))

    response=jt808_resopnes('8001',result,'00')
    mesg_explain=''
    for x in range(len(result['mesg_explain'])):
        mesg_explain+='{:04b}'.format(int(result['mesg_explain'][x],16))

    mesg_split=mesg_explain[2]
    mesg_encryption=mesg_explain[5]
    # print(mesg_explain)
    if mesg_split=='1':
        rule=rule+r'(?P<mesg_total_num>\w{4})'+r'(?P<mesg_order_num>\w{2})'
    if mesg_encryption=='1':
        #消息体RSA解密转码
        print('RSA_algorithm_encryption')

    if mesg_id=='0001':#终端回应平台下发指令
        rule=rule+r'(?P<answer_num>\w{4})(?P<answer_id>\w{4})(?P<command_result>\w{2})'
        data=re.search(rule,data).groupdict()
        # print('0001：终端回应平台下发指令',data)

    # 设备登录
    elif mesg_id=='0100':
        # if len(data)-head_len==76:
        rule=rule+r'(?P<Provincial_ID>\w{4})(?P<City_County_ID>\w{4})(?P<Manufacturer_ID>\w{10})(?P<Terminal_model>\w{16})(?P<Terminal_ID>\w{14})(?P<car_num_color>\w{2})(?P<car_identification>\w{2})'
        data=re.search(rule,data).groupdict()
        all_data.update(data)
        response=jt808_resopnes('8100',result,'00')
        all_data['save_kind']='yes'

    # 主要的位置上报信息
    elif mesg_id=='0200':
        # print(data)
        loca_data=loca_report(rule,data)
        all_data.update(loca_data)
        all_data['save_kind']='yes'

    elif mesg_id=='0201':
        data_head=data[0:head_len]
        data_body=data[head_len:]
        # 把消息体的前4位，应答流水号去掉，重新组成数据进行处理
        handle_data=data_head+data_body[4:]
        # 处理0200位置信息
        loca_data=loca_report(rule,data)
        all_data.update(loca_data)
        all_data['save_kind']='yes'
        print('0201：终端查询位置信息回复')

    elif mesg_id=='0704':
        print('0704：多条数据上传')
        rule=rule+r'(?P<data_num>\w{4})(?P<data_type>\w{2})(?P<detail>\w*)'
        res=re.search(rule,data).groupdict()
        data_num=int(res['data_num'],16)
        type_dict={'00':'正常','01':'补报'}
        data_type=type_dict[res['data_type']]
        detail=res['detail']
        multi_rule=rule=r'(?P<mesg_explain>\w{4})(?P<phone_num>\w{12})(?P<mesg_num>\w{4})'
        all_loca=[]
        for x in range(data_num):
            data_len=int(detail[0:4],16)*2
            handle_len=4+data_len
            data_body=detail[4:handle_len]
            data_res=loca_report('',data_body)
            detail=detail[handle_len:]
            lng=data_res.get('lng')
            
            if lng not in [0.0,'','0',None]:
                data_res['device_id']=all_data['device_id']
                data_res['serv_receive']=serv_receive
                all_loca.append(data_res)

        all_data['save_kind']='yes'
        all_data['all_loca']=all_loca
    all_data['response']=response
    # print('all_data',all_data)
    return all_data


# heart_beat=b'~\x00\x02\x00\x00\x01A\x19P\x10\x08\x1b\x99\x91~'
# re_connect=b'~\x01\x02\x00\x02\x01A\x19P\x10\x03\x04&119~'
# loca_data=b'~\x02\x00\x00(\x01A\x19@\x11E\x00\x95\x00\x00\x00\x00\x00\x00\x00\x03\x01Z*\n\x06\xcb\xf6`\x00\x00\x00\x00\x00\x00\x19\t\x10\x157\x07\x01\x04\x00\x00\x00\x00+\x04\xfa2\xfa9\xd5~'
# nd=binascii.a2b_hex('7e02000048051160004464000d00000000000c00020159885e06cbf34400000000000020080718072201040000000130011c310109e4020063e50100e60100e7080000000000000000ee0a01cc01262c009a6743001e7e')
# print(binascii.a2b_hex('7e000200000140278609190c10ee7e'))
# register_data=b'~\x02\x00\x00"\x01A\x19P\x10\x01\x04\xcc\x00\x00\x00\x00\x00\x00\x00\x03\x02\t)\xfc\x06\xb4\x17\xe0\x00\x00\x00\x00\x00\x00\x19\t\x16\t\x00\x05\x01\x04\x00\x00\x02>[~'
# org_data=b'~\x02\x00\x00"\x01A\x19P\x10\x01\x05Z\x00\x00\x00\x00\x00\x00\x00\x03\x02\t24\x06\xb4q\xd0\x00\x00\x00\x00\x01@\x19\t\x16\t4\'\x01\x04\x00\x00\x02]}\x01~'
d1=b'~\x07\x04\x00o\x01A\x19P\x10\x07\x01L\x00\x03\x01\x00"\x00\x00\x00\x00\x00\x00\x00\x03\x02\t!\x84\x06\xb3\xf3 \x00\x00\x00\x00\x00Z\x19\t\x16\t0\x07\x01\x04\x00\x00\x03W\x00"\x00\x00\x00\x00\x00\x00\x00\x03\x02\t!\x84\x06\xb3\xf3 \x00\x00\x00\x00\x00Z\x19\t\x16\t07\x01\x04\x00\x00\x03W\x00"\x00\x00\x00\x00\x00\x00\x00\x03\x02\t!\x84\x06\xb3\xf3 \x00\x00\x00\x00\x00Z\x19\t\x16\t1\x07\x01\x04\x00\x00\x03W\xd6~'
# d2=b'~\x02\x00\x00"\x01A\x19P\x10\x01\x059\x00\x00\x00\x00\x00\x00\x00\x03\x02\t24\x06\xb4q\xd0\x00\x00\x00\x00\x01@\x19\t\x16\t\'\x07\x01\x04\x00\x00\x02]-~'
# d3=b'~\x02\x00\x00"\x01A\x19P\x10\x01\x04\xcc\x00\x00\x00\x00\x00\x00\x00\x03\x02\t)\xfc\x06\xb4\x17\xe0\x00\x00\x00\x00\x00\x00\x19\t\x16\t\x00\x05\x01\x04\x00\x00\x02>[~'
d4=b'~\x01\x00\x00!\x019\x11\x13\x11\x19\x00\x15\x00,\x00!11111GS03A\x00\x00\x000000099\x01\xd4\xa5A12345]~'
# d5=b'~\x00\x02\x00\x00\x010\x02w\x19\x95\x0c\x10\xd6~'
# d6=b"~\x00\x02\x00\x00\x01@'\x86\t\x19\x0c\x10\xee~"
# d7=b'~\x07\x04\x00\x81\x01A\x19P\x10p\x02]\x00\x03\x01\x00(\x00\x00\x00\x00\x00\x00\x00\x03\x02\x08\x93,\x06\xb3\x82\xc8\x00\x00\x00Z\x00\xe6 \t\x18\x16\x11"\x01\x04\x00\x01\x17\x8b+\x04(\x10\x1f\xf2\x00(\x00\x00\x00\x00\x00\x00\x00\x03\x02\x08\x92\x08\x06\xb3\x82\x08\x00\x00\x002\x00\xfa \t\x18\x16\x11R\x01\x04\x00\x01\x17\x8b+\x04(\x10\x1f\xf2\x00(\x00\x00\x00\x00\x00\x00\x00\x03\x02\x08\x86\xe0\x06\xb3B\xf8\x00\x00\x00\xb4\x00\xfa \t\x18\x18\x17R\x01\x04\x00\x01\x17\xd0+\x04(\x10\x1f\xf2\x04~'
nd1=b'~\x07\x04\x00\xe1\x01\x01@\x009\x82\x00\x08\x00\x03\x01\x00H\x00\x00\x00\x00\x00L\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00p\x01\x01\x00\x00B\x01\x04\x00\x00\x00\x000\x01\x1f1\x01\x00\xe4\x02\x00d\xe5\x01\x01\xe6\x01\x00\xe7\x08\x00\x00\x00\x00\x00\x00\x00\x00\xee\n\x01\xcc\x01%=\x06\x1e%\xa1\x00\x00H\x00\x00\x00\x00\x00L\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00p\x01\x01\x00\x017\x01\x04\x00\x00\x00\x000\x01\x1f1\x01\x04\xe4\x02\x00d\xe5\x01\x01\xe6\x01\x00\xe7\x08\x00\x00\x00\x00\x00\x00\x00\x00\xee\n\x01\xcc\x01%=\x06\x1e%\xa1\x00\x00H\x00\x00\x00\x00\x00L\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00p\x01\x01\x00\x017\x01\x04\x00\x00\x00\x000\x01\x1f1\x01\x04\xe4\x02\x00d\xe5\x01\x01\xe6\x01\x00\xe7\x08\x00\x00\x00\x00\x00\x00\x00\x00\xee\n\x01\xcc\x01%=\x06\x1e%\xa1\x00\xfc~'
# nd2=b"~\x07\x04\x00M\x01\x01@\x007\x84\x00\x06\x00\x01\x01\x00H\x00\x00\x00\x00\x00L\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00p\x01\x01\x00\x00'\x01\x04\x00\x00\x00\x000\x01\x1f1\x01\x00\xe4\x02\x00d\xe5\x01\x01\xe6\x01\x00\xe7\x08\x00\x00\x00\x00\x00\x00\x00\x00\xee\n\x01\xcc\x01%=\x06\x1e%e\x00\xf5~"
# nd3=b'~\x07\x04\x00M\x01\x01@\x009\x82\x00\x12\x00\x01\x01\x00H\x00\x00\x01\x80\x00L\x00\x01\x01Y\xe6\xa8\x06\xcb\xf3\x98\x00\x00\x00\x00\x00\x00!\x02\x02\tTP\x01\x04\x00\x00\x00\x020\x01\x121\x01\x00\xe4\x02\x01\x0e\xe5\x01\x01\xe6\x01\x00\xe7\x08\x00\x00\x00\x00\x00\x00\x00\x00\xee\n\x01\xcc\x01%=\x06\x1f\x83)\x00,~'
# nd4=b'~\x07\x04\x00M\x01\x01@\x009\x82\x00\x11\x00\x01\x01\x00H\x00\x00\x01\x80\x00L\x00\x01\x01Y\xe6\xa8\x06\xcb\xf3\x98\x00\x00\x00\x00\x00\x00!\x02\x02\tTP\x01\x04\x00\x00\x00\x020\x01\x121\x01\x00\xe4\x02\x01\x0e\xe5\x01\x01\xe6\x01\x00\xe7\x08\x00\x00\x00\x00\x00\x00\x00\x00\xee\n\x01\xcc\x01%=\x06\x1f\x83)\x00/~'
rby0200=b'~\x02\x00\x004\x010P\x03\x81\x01\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x01Y\x07\xb2\x06\xcc\x17I\x00\x00\x00 \x00\xe6!\x03$\x18\x08R\x01\x04\x00\x00\x00\x00+\x04\x04\xc1\x04\xc10\x01\x151\x01\x00\x00\x04\x00\xce\x04\xc1^~'
rby02001=b'~\x02\x00\x004\x010P\x03\x81\x01\x00E\x00\x00\x00\x00\x00\x00\x00\x00\x01Y\x07\xb2\x06\xcc\x17I\x00\x00\x00 \x00\xe6!\x03$\x18\x10R\x01\x04\x00\x00\x00\x00+\x04\x05\xc1\x05\xc10\x01\x131\x01\x00\x00\x04\x00\xce\x05\xc1D~'
rby02002=b'~\x02\x00\x004\x010P\x03\x81\x01\x00M\x00\x00\x01\x00\x00\x00\x08\x00\x01Y\x07\xb2\x06\xcc\x17I\x00\x00\x00 \x00\xe6!\x03$\x18\x14\x04\x01\x04\x00\x00\x00\x00+\x04\x00n\x00n0\x01\x161\x01\x00\x00\x04\x00\xce\x00n\xb8~'
rby02003=b'~\x02\x00\x004\x010P\x03\x81\x01\x00V\x00\x00\x00\x00\x00\x00\x00\x00\x01Y\x07\xb2\x06\xcc\x17I\x00\x00\x00 \x00\xe6!\x03$\x18\x175\x01\x04\x00\x00\x00\x00+\x04\np\np0\x01\x161\x01\x00\x00\x04\x00\xce\np\x8c~'
rby02004=b'~\x02\x00\x004\x010P\x03\x81\x01\x00_\x00\x00\x00\x00\x00\x00\x00\x00\x01Y\x07\xb2\x06\xcc\x17I\x00\x00\x00 \x00\xe6!\x03$\x18!5\x01\x04\x00\x00\x00\x00+\x04\x02n\x02n0\x01\x161\x01\x00\x00\x04\x00\xce\x02n\xa5~'
rby02005=b'~\x02\x00\x004\x010P\x03\x81\x01\x00c\x00\x00\x00\x00\x00\x00\x00\x00\x01Y\x07\xb2\x06\xcc\x17I\x00\x00\x00 \x00\xe6!\x03$\x18#\x05\x01\x04\x00\x00\x00\x00+\x04\np\np0\x01\x161\x01\x00\x00\x04\x00\xce\np\xbd~'
# print(binascii.hexlify(b'~\x00\x02\x00\x00\x010\x02w\x19\x95\x0c\x10\xd6~'))
# nnnn=b'~\x02\x00\x00&\x01@\'\x85\x90w\x00\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\x11\x12#$R\x01\x04\x00\x00\x00\x12a\x02\x04\xba\xa7~~\x02\x00\x00Y\x01@\'\x85\x90w\x00\x02\x00\x00\x00\x00\x00\x00\x00\x01\x01Y"\x8e\x06\xcb\xf1x\x00\x00\x00\x00\x00\x00\x19\x11\x12\x18&3\x01\x04\x00\x00\x00\x12a\x02\x04|S1\x06\x01\xcc\x00$\xa4\x11\x960\x01\xcc\x00$\xa4\x11\x951\x01\xcc\x00$\xa4\x11O.\x01\xcc\x00$\xa4\x11P,\x01\xcc\x00$\xa4\x14\xb7,\x01\xcc\x00$\xa4\x0e\xda+\xfc~'
# n1=b"~\x02\x00\x00&\x01@'\x85\x90w\x00\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\x11\x12#$R\x01\x04\x00\x00\x00\x12a\x02\x04\xba\xa7~"
# n2=b'~\x02\x00\x00Q\x01@\'\x85\x90w\x00\x06\x00\x00\x00\x00\x00\x00\x00\x03\x01Y\x1a\x96\x06\xcc\t8\x00\x00\x00\n\x00k\x19\x11\x16\x166"\x01\x04\x00\x00\x00\x12a\x02\x04\xdaS)\x05\x01\xcc\x00$\xa4\x11\x95/\x01\xcc\x00$\xa4\x11P1\x01\xcc\x00$\xa4\x11O.\x01\xcc\x00$\xa4\x0e\xda)\x01\xcc\x00$\x81\x0f\xe9\'*~'

if __name__ == '__main__':
    jt808_analysis(d1,'serv_receive')
