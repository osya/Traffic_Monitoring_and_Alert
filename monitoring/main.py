#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import MySQLdb.cursors
import psycopg2
import psycopg2.extras
import logging
import time
import datetime as dt
import sched
import types
from settings import Config as Config
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# TODO: block in the resource_block
# TODO: Code review
# TODO: Write unit tests

logger = logging.getLogger('Monitoring Rule')
logger.setLevel(logging.INFO)
# add Console logger
ch = logging.StreamHandler()
logger.addHandler(ch)
fh = logging.FileHandler('/home/valeriy/monitoring/monitoring.log')
logger.addHandler(fh)


def send_email_log(cur,result_dict,client_id):
    if result_dict['status'] == True:
        status = 0
    else:
        status = 1
    sql = """insert into email_log(send_time,client_id,email_addresses,type,status,error) values (CURRENT_TIMESTAMP(0),%s,%s,%s,%s,%s)"""
    cur.execute(sql,(client_id,result_dict['send_email'],4,status,result_dict['msg']))


def get_smtp_info_by_send(cur,send_mail_id):
    sql = """SELECT  smtp_host AS host, smtp_port AS port,username,password as  password,loginemail as is_auth,
email as from_email,name as name, secure as smtp_secure FROM mail_sender where id = %s""" % send_mail_id
    cur.execute(sql)
    smtp_setting = cur.fetchone()
    return smtp_setting


def get_smtp_info(cursor):
    sql = """SELECT smtphost as host,smtpport as port,emailusername as username,emailpassword as password,loginemail as is_auth,
				fromemail as from_email, smtp_secure as smtp_secure,noc_email as noc_email FROM system_parameter LIMIT 1"""
    cursor.execute(sql)
    smtp_setting = cursor.fetchone()
    return smtp_setting


def do_send_email(cursor,mail_subject,mail_content,send_email,sent_from,client_id,client_name):
    result_dict = {'send_email' : send_email,'msg':'','status': False}
    logger.info("send_email: "+str(send_email))
    if send_email.strip()== '':
        result_dict['msg'] = 'client[%s] email is not configured' % client_name
        return result_dict

    if sent_from == 'Default' or sent_from == 'default':
        smtp_setting = get_smtp_info(cursor)
    else:
        smtp_setting = get_smtp_info_by_send(cursor,sent_from)
        if smtp_setting is None:
            smtp_setting = get_smtp_info(cursor)


    smtp_info = smtp_setting
    msg = MIMEMultipart()
    msg['Subject'] = mail_subject
    msg['From'] = smtp_info['from_email']
    msg['to'] = send_email

    part = MIMEText(mail_content, 'html')
    msg.attach(part)
    if smtp_info['smtp_secure'] == 2:
        smtp = smtplib.SMTP_SSL(smtp_info['host'], smtp_info['port'])
    else:
        smtp = smtplib.SMTP(smtp_info['host'], smtp_info['port'])

    try:
        smtp.set_debuglevel(True)
        if smtp_info['smtp_secure'] == 1:
            smtp.starttls()
        smtp.ehlo()
        smtp.login(smtp_info['username'], smtp_info['password'])
        smtp.sendmail(smtp_info['from_email'], send_email, msg.as_string())
    except smtplib.SMTPRecipientsRefused:
        result_dict['msg'] = 'All recipients were refused.'
        result_dict['status'] = False
    except smtplib.SMTPHeloError:
        result_dict['msg'] = 'The server didn’t reply properly to the HELO greeting.'
        result_dict['status'] = False
    except smtplib.SMTPSenderRefused:
        result_dict['msg'] = 'The server didn’t accept the %s.' % smtp_info['from_email']
        result_dict['status'] = False
    except smtplib.SMTPDataError:
        result_dict['msg'] = 'The server replied with an unexpected error code (other than a refusal of a recipient).'
        result_dict['status'] = False
    else:
        result_dict['msg'] = 'OK'
        result_dict['status'] = True
    finally:
        smtp.quit()

    send_email_log(cursor,result_dict,client_id)

    return result_dict


def get_client_email_info(cur,resource_id):
    sql = """select client.email,client.client_id,client.noc_email,resource.alias,client.name FROM resource inner join client on resource.client_id = client.client_id
where resource_id = %s"""
    cur.execute(sql,(resource_id,))
    email_info = cur.fetchone()
    return email_info


def send_type_email(rule,cursor,email_content_dirc,system_info):
    actual_table_template = """<div style="width: 500px;margin: 0 auto 0;"><table border=0 cellpadding=5 cellspacing=0 style="background-color:#FAFAFA; border-collapse:collapse; border:0px solid #ccc; width:100%;white-space:nowrap;font-size:14px"><thead><tr><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><font color="#ffffff">Client</font></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><font color="#ffffff">Trunk</font></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><font color="#ffffff">Code</font></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><font color="#ffffff">ASR</font></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">ABR</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">ACD</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">PDD</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">Revenue</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">Profitability</span></th></tr></thead><tbody>{data_body}</tbody></table></div><div style="height: 15px;"></div>"""
    actual_table_tbody_template = """<tr style="height: 20px"><td style="border:1px solid #cfcfcf">{client}</td><td style="border:1px solid #cfcfcf">{trunk}</td><td style="border:1px solid #cfcfcf">{code}</td><td style="border:1px solid #cfcfcf">{actual_asr}</td><td style="border:1px solid #cfcfcf">{actual_abr}</td><td style="border:1px solid #cfcfcf">{actual_acd}</td><td style="border:1px solid #cfcfcf">{actual_pdd}</td><td style="border:1px solid #cfcfcf">{actual_revenue}</td><td style="border:1px solid #cfcfcf">{actual_profitability}</td></tr>"""

    limit_table_template = """<div style="width: 500px;margin: 0 auto 0;"><table border=0 cellpadding=5 cellspacing=0 style="background-color:#FAFAFA; border-collapse:collapse; border:0px solid #ccc; width:100%;white-space:nowrap;font-size:14px"><thead><tr><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><font color="#ffffff">Rule</font></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><font color="#ffffff">ASR</font></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">ABR</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">ACD</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">PDD</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">Revenue</span></th><th style="background-color:#51A351; text-align:left;border:1px solid #cfcfcf"><span style="color:#FFFFFF">Profitability</span></th></tr></thead><tbody>{data_body}</tbody></table></div><div style="height: 15px;"></div>"""
    limit_table_tbody_template = """<tr style="height: 20px"><td style="border:1px solid #cfcfcf">{rule}</td><td style="border:1px solid #cfcfcf">{limit_asr}</td><td style="border:1px solid #cfcfcf">{limit_abr}</td><td style="border:1px solid #cfcfcf">{limit_acd}</td><td style="border:1px solid #cfcfcf">{limit_pdd}</td><td style="border:1px solid #cfcfcf">{limit_revenue}</td><td style="border:1px solid #cfcfcf">{limit_profitability}</td></tr>"""

    sql = """SELECT trouble_ticket_subject,trouble_ticket_content,trouble_ticket_sent_from from alert_rules where id = %s""" % rule['id']
    cursor.execute(sql)
    email_template = cursor.fetchone()
    mail_content = email_template['trouble_ticket_content']
    mail_subject = email_template['trouble_ticket_subject']
    mail_from = email_template['trouble_ticket_sent_from']
    #limit
    limit_asr = 'ignore' if rule['asr'] == '1' else rule['asr'] + ' ' + str(rule['asr_value']) + '%'
    limit_abr = 'ignore' if rule['abr'] == '1' else rule['abr'] + ' ' + str(rule['abr_value']) + '%'
    limit_acd = 'ignore' if rule['acd'] == '1' else rule['acd'] + ' ' + str(rule['acd_value']) + 's'
    limit_pdd = 'ignore' if rule['pdd'] == '1' else rule['pdd'] + ' ' + str(rule['pdd_value']) + 's'
    limit_revenue = 'ignore' if rule['revenue'] == '1' else rule['revenue'] + ' ' + str(round(rule['revenue_value'],2))
    limit_profitability = 'ignore' if rule['profitability'] == '1' else rule['profitability'] + ' ' + str(rule['profitability_value']) + '%'

    rule_name = rule['rule_name']

    limit_table_tbody_template = limit_table_tbody_template.replace('{rule}',str(rule_name)).replace('{limit_asr}',str(limit_asr)).replace('{limit_abr}',str(limit_abr)).replace('{limit_acd}',str(limit_acd)) \
                                    .replace('{limit_pdd}',str(limit_pdd)).replace('{limit_revenue}',str(limit_revenue)).replace('{limit_profitability}',str(limit_profitability))
    limit_table_template = limit_table_template.replace('{data_body}',limit_table_tbody_template)

    switch_alias = system_info['switch_alias']

    mail_content = mail_content.replace('{rule_name}',str(rule['rule_name'])).replace('{switch_alias}',str(switch_alias)).replace('{limit_table}',limit_table_template)


    send_type =  rule['trouble_ticket_sent_to']

    if(send_type != 2):
        send_sys_email = system_info['noc_email']
        tmp_sys_email = system_info['system_admin_email']
        if send_sys_email.strip()== '':
            send_sys_email = tmp_sys_email


    for resource_id in email_content_dirc:
        client_email_info = get_client_email_info(cursor,resource_id)
        client_id = client_email_info['client_id']
        client_name = client_email_info['name']
        resource_name = client_email_info['alias']
        send_email = client_email_info['noc_email']
        tmp_email = client_email_info['email']


        if send_email.strip()== '':
            send_email = tmp_email

        data_table = ''
        detail_log_ids = ''
        for code in email_content_dirc[resource_id]:
            asr = email_content_dirc[resource_id][code]['running_info']['asr']
            abr = email_content_dirc[resource_id][code]['running_info']['abr']
            acd = email_content_dirc[resource_id][code]['running_info']['acd']
            pdd = email_content_dirc[resource_id][code]['running_info']['pdd']
            revenue = email_content_dirc[resource_id][code]['running_info']['revenue']
            profitability = email_content_dirc[resource_id][code]['running_info']['profitability']

            if detail_log_ids == '':
                detail_log_ids += str(email_content_dirc[resource_id][code]['detail_log_id'])
            else:
                detail_log_ids += ',' + str(email_content_dirc[resource_id][code]['detail_log_id'])

            if code is None:
                code = ''
            data_table += actual_table_tbody_template.replace('{client}',str(client_name)).replace('{trunk}',str(resource_name)).replace('{code}',str(code)).replace('{actual_asr}',str(asr) + '%') \
                .replace('{actual_abr}',str(abr) + '%').replace('{actual_acd}',str(acd) + 's').replace('{actual_pdd}',str(pdd) + 's') \
                .replace('{actual_revenue}',str(round(revenue,2))).replace('{actual_profitability}',str(profitability) + '%')
        actual_table_template = actual_table_template.replace('{data_body}',data_table)
        mail_content = mail_content.replace('{rule_name}',str(rule['rule_name'])).replace('{switch_alias}',str(switch_alias)) \
                            .replace('{actual_table}',actual_table_template)

        if(send_type == 2): #client
            mail_content = mail_content.replace('{username}',str(client_name))
            rst_dirc = do_send_email(cursor,mail_subject,mail_content,send_email,mail_from,client_id,client_name)
            rst_dirc['detail_log_ids'] = detail_log_ids
            rst_dirc['email_type'] = send_type
            save_log_detail(cursor,rst_dirc,'email_client')
        elif(send_type == 1): #admin
            mail_content = mail_content.replace('{username}','Admin')
            rst_dirc = do_send_email(cursor,mail_subject,mail_content,send_sys_email,mail_from,client_id,'admin')
            rst_dirc['detail_log_ids'] = detail_log_ids
            rst_dirc['email_type'] = send_type
            save_log_detail(cursor,rst_dirc,'email_admin')
        else:
            mail_content1 = mail_content.replace('{username}',str(client_name))
            rst_dirc = do_send_email(cursor,mail_subject,mail_content1,send_email,mail_from,client_id,client_name)
            rst_dirc['detail_log_ids'] = detail_log_ids
            rst_dirc['email_type'] = send_type
            save_log_detail(cursor,rst_dirc,'email_client')

            mail_content = mail_content.replace('{username}','Admin')
            rst_dirc = do_send_email(cursor,mail_subject,mail_content,send_sys_email,mail_from,client_id,'admin')
            rst_dirc['detail_log_ids'] = detail_log_ids
            rst_dirc['email_type'] = send_type
            save_log_detail(cursor,rst_dirc,'email_admin')


def get_system_parameter(cur):
    sql = """select * from system_parameter limit 1"""
    cur.execute(sql)
    return cur.fetchone()


def email(rule,return_arr,cursor):
    is_all_trunk = False
    include = rule['include']
    if include is None or include == '':
        include = 0

    exclude = rule['exclude']
    if exclude is None or exclude == '':
        exclude = 0
    if include == 0 and exclude == 0:
        is_all_trunk = True

    client_email_content_arr = {}
    # admin_email_content_arr = {}

    for key in return_arr:
        detail_log_id = return_arr[key]['alert_rules_log_detail_id']
        trunk_id = return_arr[key]['trunk_id']
        trunk_type = return_arr[key]['trunk_type']
        code = return_arr[key]['code']
        running_info = return_arr[key]['running_info']


        # if is_all_trunk:
    if trunk_id not in client_email_content_arr.keys():
        client_email_content_arr[trunk_id] = {}
    # if trunk_id not in admin_email_content_arr.keys():
    #     admin_email_content_arr[trunk_id] = {}
    client_email_content_arr[trunk_id][code] = {'trunk_type':trunk_type,'running_info':running_info,'detail_log_id':detail_log_id}
            # admin_email_content_arr[trunk_id][code] = {'trunk_type':trunk_type,'running_info':running_info}

    system_info = get_system_parameter(cursor)

    send_type_email(rule,cursor,client_email_content_arr,system_info)


def do_block(cursor, resource_id, trunk_type, rule_name, code=''):
    sql = """select client_id FROM resource where resource_id = %s"""
    cursor.execute(sql, (resource_id,))
    client = cursor.fetchone()
    client_id = client['client_id']
    update_by = "Alert Rule[%s]" % rule_name
    if code == '':
        if trunk_type == 1:
            block_sql = """insert into resource_block (ingress_client_id,ingress_res_id,action_type,update_by,create_time) values (%s,%s,1,'%s',current_timestamp(0)) returning res_block_id""" % (
                client_id, resource_id, update_by)
        else:
            block_sql = """insert into resource_block (egress_client_id,engress_res_id,action_type,update_by,create_time) values (%s,%s,1,'%s',current_timestamp(0)) returning res_block_id""" % (
                client_id, resource_id, update_by)
    else:
        if trunk_type == 1:
            block_sql = """insert into resource_block (ingress_client_id,ingress_res_id,digit,action_type,update_by,create_time) values (%s,%s,'%s',1,'%s',current_timestamp(0)) returning res_block_id""" % (
                client_id, resource_id, code, update_by)
        else:
            block_sql = """insert into resource_block (egress_client_id,engress_res_id,digit,action_type,update_by,create_time) values (%s,%s,'%s',1,'%s',current_timestamp(0)) returning res_block_id""" % (
                client_id, resource_id, code, update_by)
    logger.info("block_sql: " + block_sql)
    cursor.execute(block_sql)
    res_block_id = cursor.fetchone()
    res_block_id = res_block_id['res_block_id']
    return res_block_id


def judge_is_in_blocks(blocks, trunk_id, trunk_type, code=''):
    if code == '':
        global inserted_trunk_all_block_arr
        for item in blocks:
            if trunk_type == 1:  # ingress
                if trunk_id == item['ingress_res_id'] and item['digit'] is None:
                    if item['engress_res_id'] is None and item['egress_client_id'] is None and \
                                    item['ani_prefix'] is None and item['time_profile_id'] is None and \
                                    item['ani_length'] is None and item['ani_max_length'] == 32 and \
                                    item['dnis_length'] is None and item['dnis_max_length'] == 32:
                        return item['res_block_id']
            else:
                if trunk_id == item['engress_res_id'] and item['digit'] is None:
                    if item['ingress_res_id'] is None and item['ingress_client_id'] is None and \
                                    item['ani_prefix'] is None and item['time_profile_id'] is None and \
                                    item['ani_length'] is None and item['ani_max_length'] is None and \
                                    item['dnis_length'] is None and item['dnis_max_length'] == 32:
                        return item['res_block_id']
            if trunk_id in inserted_trunk_all_block_arr:
                return item['res_block_id']

        inserted_trunk_all_block_arr.append(trunk_id)
        return False
    else:
        global inserted_trunk_code_dic
        for item in blocks:
            if trunk_type == 1:  # ingress
                if trunk_id == item['ingress_res_id'] and code == item['digit']:
                    if item['engress_res_id'] is None and item['egress_client_id'] is None and \
                                    item['ani_prefix'] is None and item['time_profile_id'] is None and \
                                    item['ani_length'] is None and item['ani_max_length'] == 32 and \
                                    item['dnis_length'] is None and item['dnis_max_length'] == 32:
                        return item['res_block_id']
            else:
                if trunk_id == item['engress_res_id'] and code == item['digit']:
                    if item['ingress_res_id'] is None and item['ingress_client_id'] is None and \
                                    item['ani_prefix'] is None and item['time_profile_id'] is None and \
                                    item['ani_length'] is None and item['ani_max_length'] is None and \
                                    item['dnis_length'] is None and item['dnis_max_length'] == 32:
                        return item['res_block_id']
            if trunk_id in inserted_trunk_code_dic.keys() and code in inserted_trunk_code_dic[trunk_id].keys():
                return item['res_block_id']

        if trunk_id not in inserted_trunk_code_dic.keys():
            inserted_trunk_code_dic[trunk_id] = {}

        inserted_trunk_code_dic[trunk_id][code] = ''

        return False


def block(rule, return_arr, cursor):
    # for item in return_arr:
    is_block_all_trunk = False
    include = rule['include']
    if include is None or include == '':
        include = 0

    exclude = rule['exclude']
    if exclude is None or exclude == '':
        exclude = 0
    if include == 0 and exclude == 0:
        is_block_all_trunk = True

    # 查出resource_block 表已有的记录，
    sql = """SELECT * FROM resource_block"""
    cursor.execute(sql)
    blocks = cursor.fetchall()
    # myprint("have blocks")
    #
    # myprint("return_arr: "+return_arr)

    # 如果block所有
    if is_block_all_trunk:
        global inserted_trunk_all_block_arr
        inserted_trunk_all_block_arr = []

        for key in return_arr:
            trunk_id = return_arr[key]['trunk_id']
            trunk_type = return_arr[key]['trunk_type']
            rst = judge_is_in_blocks(blocks, trunk_id, trunk_type)
            if rst:
                logger.info("exist in resource_block trunk_id:" + str(trunk_id))
                val = {'alert_rules_log_detail_id': return_arr[key]['alert_rules_log_detail_id'],
                       'resource_block_id': rst}
                save_log_detail(cursor, val, 'block_true')
                continue
            else:
                # 添加进resource_block 表
                rst = do_block(cursor, trunk_id, trunk_type, rule['rule_name'])
                val = {'alert_rules_log_detail_id': return_arr[key]['alert_rules_log_detail_id'],
                       'resource_block_id': rst}
                save_log_detail(cursor, val, 'block_true')

    else:
        global inserted_trunk_code_dic
        inserted_trunk_code_dic = {}
        for key in return_arr:
            trunk_id = return_arr[key]['trunk_id']
            code = return_arr[key]['code']
            trunk_type = return_arr[key]['trunk_type']

            if code is None or code == '':
                continue
            rst = judge_is_in_blocks(blocks, trunk_id, trunk_type, code)
            if rst:
                logger.info("exist in resource_block trunk_id:" + str(trunk_id) + " code:" + str(code))
                val = {'alert_rules_log_detail_id': return_arr[key]['alert_rules_log_detail_id'],
                       'resource_block_id': rst}
                save_log_detail(cursor, val, 'block_true')
                continue
            else:
                # 添加进resource_block 表
                rst = do_block(cursor, trunk_id, trunk_type, rule['rule_name'], code)
                val = {'alert_rules_log_detail_id': return_arr[key]['alert_rules_log_detail_id'],
                       'resource_block_id': rst}
                save_log_detail(cursor, val, 'block_true')


def save_log_detail(cursor, val, opt, return_arr=[]):
    if opt == 'block_false':
        for key in return_arr:
            sql = """update alert_rules_log_detail set is_block = %s where id = %s"""
            cursor.execute(sql, (val, return_arr[key]['alert_rules_log_detail_id']))
    elif opt == 'block_true':
        resource_block_id = val['resource_block_id']
        id = val['alert_rules_log_detail_id']
        sql = """update alert_rules_log_detail set is_block = true, resource_block_id = %s where id = %s"""
        cursor.execute(sql, (resource_block_id, id))
    elif opt == 'email_false':
        for key in return_arr:
            sql = """update alert_rules_log_detail set is_email = %s where id = %s"""
            cursor.execute(sql, (val, return_arr[key]['alert_rules_log_detail_id']))
    elif opt == 'email_true':
        for key in return_arr:
            sql = """update alert_rules_log_detail set is_email = %s where id = %s"""
            cursor.execute(sql, (val, return_arr[key]['alert_rules_log_detail_id']))
    elif opt == 'email_client':
        detail_log_ids = val['detail_log_ids']
        status = val['status']
        email_address = val['send_email']
        email_type = val['email_type']
        sql = """update alert_rules_log_detail set partner_email_status = %s, partner_email_address = '%s',email_type = %s where id in (%s)""" % (
            status, email_address, email_type, detail_log_ids)
        cursor.execute(sql)
    elif opt == 'email_admin':
        detail_log_ids = val['detail_log_ids']
        status = val['status']
        email_address = val['send_email']
        email_type = val['email_type']
        sql = """update alert_rules_log_detail set system_email_status = %s, system_email_address = '%s',email_type = %s where id in (%s)""" % (
            status, email_address, email_type, detail_log_ids)
        cursor.execute(sql)


def save_return_arr_to_detail(rule, alert_rules_log_id, return_arr, cursor):
    # myprint(return_arr)
    # for item in return_arr:
    is_all_trunk = False
    include = rule['include']
    if include is None or include == '':
        include = 0

    exclude = rule['exclude']
    if exclude is None or exclude == '':
        exclude = 0
    if include == 0 and exclude == 0:
        is_all_trunk = True

    for key in return_arr:
        if not is_all_trunk and return_arr[key]['code'] is None:
            continue
        running_info = return_arr[key]['running_info']
        sql = """INSERT INTO alert_rules_log_detail(alert_rules_log_id,resource_id,code,asr,acd,abr,pdd,revenue,profitability)
                  VALUES (%s, %s, '%s', %s, %s, %s, %s, %s, %s ) returning id"""
        sql = sql % (alert_rules_log_id, return_arr[key]['trunk_id'], return_arr[key]['code'], running_info['asr'],
                     running_info['acd'], running_info['abr'], running_info['pdd'], running_info['revenue'],
                     running_info['profitability'])
        cursor.execute(sql, (alert_rules_log_id,))
        alert_rules_log_detail_id = cursor.fetchone()
        alert_rules_log_detail_id = alert_rules_log_detail_id['id']
        return_arr[key]['alert_rules_log_detail_id'] = alert_rules_log_detail_id

    return return_arr


def save_finish_alert_rule_log(cursor, id_, status):
    sql = """update alert_rules_log set status = %s , finish_time = CURRENT_TIMESTAMP(0) where id = %s"""
    cursor.execute(sql, (status, id_))


def judge_num(n1, n2, flg):
    n1 = float(n1)
    n2 = float(n2)
    if flg == '<':
        if n1 < n2:
            return True
        return False
    elif flg == '>':
        if n1 > n2:
            return True
        return False
    elif flg == '=':
        if n1 == n2:
            return True
        return False
    else:
        return False


def judge_define_condition(rule, cursor):
    logger.info("***judge min_call***")
    is_true = True

    # time
    exec_time_interval = rule['sample_size'] * 60
    endtime_str = time.strftime("%Y-%m-%d %H:%M:00")
    logger.info("endtime: " + endtime_str)
    endtime = time.strptime(endtime_str, "%Y-%m-%d %H:%M:%S")
    endtime = time.mktime(endtime)
    starttime = endtime - exec_time_interval
    starttime = time.gmtime(starttime)
    starttime_str = time.strftime("%Y-%m-%d %H:%M:%S", starttime)
    logger.info("starttime: " + starttime_str)
    # starttime_str = "2015-08-28 07:55:00"
    specific_minutes = rule.get('specific_minutes')
    where_time = " answer_time_of_date  >=   (NOW() - INTERVAL %s MINUTE)" % specific_minutes if specific_minutes else None
    logger.info("where_time: " + where_time)

    trunk_type = rule['trunk_type']
    res_id = rule['res_id']
    if isinstance(res_id, types.TupleType):
        res_id = res_id[0]
    all_trunk = rule['all_trunk']

    # trunk

    second_group_field_map = {
        1: 'routing_digits',
        2: 'origination_source_number'
    }
    second_group_field = second_group_field_map.get(rule['monitor_by']) or None;

    if trunk_type == 1:  # ingress
        group = "GROUP BY ingress_id"
        group_field = "ingress_id as trunk_id"
        if second_group_field:
            group += ', %s' % second_group_field
            group_field += ', %s' % second_group_field
        where_trunk = " AND ingress_id is not null " if all_trunk else " AND ingress_id in (%s) " % (res_id,)
    else:
        group = "GROUP BY egress_id"
        group_field = "egress_id as trunk_id"
        if second_group_field:
            group += ', %s' % second_group_field
            group_field += second_group_field
            where_trunk = " AND egress_id is not null " if all_trunk else " AND egress_id in (%s) " % (res_id,)

    logger.info("where_trunk: " + where_trunk)

    # include
    where_code = ""
    if rule['include']:
        in_codes_arr = rule['in_codes'].split(',')
        if in_codes_arr:
            for index, in_codes in enumerate(in_codes_arr):
                in_codes_arr[index] = "'" + in_codes + "'"
            in_codes_arr1 = ','.join(in_codes_arr)

            if trunk_type == 1:
                where_code += " AND orig_code in (%s) " % (in_codes_arr1,)
            else:
                where_code += " AND term_code in (%s) " % (in_codes_arr1,)

    # exclude
    if rule['exclude']:
        ex_codes_arr = rule['ex_codes'].split(',')
        if not ex_codes_arr:
            for key, ex_codes in enumerate(ex_codes_arr):
                ex_codes_arr[key] = "'" + ex_codes + "'"
            ex_codes_arr1 = ','.join(ex_codes_arr)

            if trunk_type == 1:
                where_code += " AND orig_code not in (%s) " % (ex_codes_arr1,)
            else:
                where_code += " AND term_code not in (%s) " % (ex_codes_arr1,)

    logger.info("where_code: " + where_code)

    count_sql = """SELECT %s, count(*) as total_attempt, sum( call_duration > 0) as `sum`,
sum(`pdd`) as total_pdd, sum (`egress_cost` ) as total_egress_cost, sum (`ingress_client_cost` ) as total_ingress_cost,
sum ( call_duration ) as total_duration, sum ( call_duration>0) as non_zero, sum( ring_time>0) as seizure  FROM `demo_cdr` WHERE %s %s %s %s """ \
                % (group_field, where_time, where_trunk, where_code, group)

    logger.info("count_sql: " + count_sql)  # for debug
    cursor.execute(count_sql)
    sum = cursor.fetchone()
    # myprint(sum)
    if sum is None:
        sum = 0
    else:
        sum = int(sum['sum'])
    # myprint("sum: " + str(sum))

    min_call_attempt = rule['min_call_attempt']
    if min_call_attempt is None or min_call_attempt == '':
        min_call_attempt = 0
    min_call_attempt = int(min_call_attempt)
    logger.info("min_call_attempt: " + str(min_call_attempt))

    if sum < min_call_attempt:
        is_true = False

    if not is_true:
        return {}
    else:
        logger.info("***judge other condition***")
        # myprint((group_field,where_time,where_trunk,where_code,group))
        sql_2 = """SELECT sum(call_duration) as duration,count(case when call_duration > 0 then 1 else null end) as not_zero_calls,
                    count(case when binary_value_of_release_cause_from_protocol_stack like '486%' then 1 else null end) as busy_calls,count(*) as total_calls,
                    count( case when binary_value_of_release_cause_from_protocol_stack like '487%' then 1 else null end ) as cancel_calls,sum(case when call_duration > 0 then pdd else 0 end) as pdd,
                    sum(ingress_client_cost) as ingress_client_cost_total,sum(egress_cost) as egress_cost_total,""" + group_field + """ FROM demo_cdr"""

        sql_2 += """ where %s %s %s %s """ % (where_time, where_trunk, where_code, group)

        # myprint("sql_2: " + sql_2)
        cursor.execute(sql_2)
        data = cursor.fetchall()

        # 生成每一个condition,并判断
        return_arr = {}
        i = 1
        for item in data:
            second_data = {}

            duration = int(item['duration']) if item['duration'] is not None else 0
            not_zero_calls = int(item['not_zero_calls']) if item['not_zero_calls'] is not None else 0
            busy_calls = int(item['busy_calls']) if item['busy_calls'] is not None else 0
            total_calls = int(item['total_calls']) if item['total_calls'] is not None else 0
            cancel_calls = int(item['cancel_calls']) if item['cancel_calls'] is not None else 0
            ingress_client_cost_total = \
                item['ingress_client_cost_total'] if item['ingress_client_cost_total'] is not None else 0
            egress_cost_total = item['egress_cost_total'] if item['egress_cost_total'] is not None else 0
            pdd = item['pdd'] if item['pdd'] is not None else 0

            second_data['acd'] = round((duration / not_zero_calls / 60), 2) if not_zero_calls != 0 else 0
            second_data['abr'] = round(not_zero_calls / total_calls * 100, 2) if total_calls != 0 else 0

            asr_ = busy_calls + cancel_calls + not_zero_calls
            second_data['asr'] = round(not_zero_calls / asr_ * 100, 2) if asr_ != 0 else 0
            second_data['pdd'] = round(pdd / not_zero_calls) if not_zero_calls != 0 else 0
            second_data['profitability'] = (
                                               ingress_client_cost_total - egress_cost_total) / ingress_client_cost_total * 100 if ingress_client_cost_total != 0 else 0
            second_data['revenue'] = ingress_client_cost_total - egress_cost_total

            if (rule['revenue'] != '1' and not judge_num(second_data['revenue'], rule['revenue_value'],
                                                         rule['revenue'])) \
                    or (rule['acd'] != '1' and not judge_num(second_data['acd'], rule['acd_value'], rule['acd'])) \
                    or (rule['asr'] != '1' and not judge_num(second_data['asr'], rule['asr_value'], rule['asr'])) \
                    or (rule['abr'] != '1' and not judge_num(second_data['abr'], rule['abr_value'], rule['abr'])) \
                    or (rule['pdd'] != '1' and not judge_num(second_data['pdd'], rule['pdd_value'], rule['pdd'])) \
                    or (rule['profitability'] != '1' and not judge_num(second_data['profitability'],
                                                                       rule['profitability_value'],
                                                                       rule['profitability'])):
                continue

            logger.info("can run")
            return_arr[i] = {}
            return_arr[i]['running_info'] = second_data
            return_arr[i]['trunk_id'] = item['trunk_id']
            return_arr[i]['trunk_type'] = trunk_type
            return_arr[i]['code'] = item['code']
            i += 1
    return return_arr


def judge_time(rule, cursor):
    now_timestamp_str = time.strftime("%Y-%m-%d %H:%M:00")
    logger.info(now_timestamp_str)

    interval_type = rule['execution_schedule']
    last_run_time = rule['last_run_time']

    is_true = False

    if interval_type == 1:
        logger.info("by minute")
        if not last_run_time:
            is_true = True
        else:
            last_run_time_str = str(last_run_time)
            pos = last_run_time_str.find(':')
            pos = last_run_time_str.find(':', pos + 1)
            last_run_time_str = last_run_time_str[:pos] + ":00"
            logger.info("by minute:last_run_time" + last_run_time_str)
            every_min = rule['specific_minutes']
            if every_min is None or every_min == '':
                every_min = 1
            every_min = int(every_min)
            logger.info("interval time:" + str(every_min))
            next_plan_run_time = last_run_time + dt.timedelta(minutes=every_min)
            logger.info("next_plan_run_time:" + str(next_plan_run_time))
            logger.info("***\n")

            if every_min == 1:
                is_true = True
            else:
                last_run_time = dt.datetime.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")
                last_run_time = time.mktime(last_run_time.timetuple())
                now_timestamp = time.mktime(time.strptime(now_timestamp_str, "%Y-%m-%d %H:%M:%S"))
                if last_run_time <= now_timestamp - 60 * every_min:  # 超过间隔，满足
                    is_true = True
                else:
                    is_true = False
    elif interval_type == 2:  # 按天
        logger.info("by day")
        day_time = rule['daily_time']
        if day_time is None or day_time == '':
            day_time = 0
        day_time = int(day_time)

        now_timestamp = time.strptime(now_timestamp_str, "%Y-%m-%d %H:%M:%S")
        now_hour = now_timestamp.tm_hour
        now_day = now_timestamp.tm_mday

        if last_run_time is None or last_run_time == '':
            if int(now_hour) == int(day_time):
                is_true = True
        else:
            last_run_time_str = str(last_run_time)
            pos = last_run_time_str.find(':')
            last_run_time_str = last_run_time_str[:pos] + ":00:00"
            last_run_time = time.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")

            last_run_hour = last_run_time.tm_hour
            last_run_day = last_run_time.tm_mday

            logger.info("by_day:last_run_time" + last_run_time_str)
            logger.info(day_time)
            logger.info("***\n")

            if now_day != last_run_day:
                if int(now_hour) == int(day_time):
                    is_true = True
            else:
                if now_hour != last_run_hour and int(now_hour) == int(day_time):
                    is_true = True
    else:
        logger.info("by week")
        week_time = rule['weekly_time']
        if week_time is None or week_time == '':
            week_time = 0
        week_day = rule['weekly_value']
        if week_day is None or week_day == '':
            week_day = 0

        now_timestamp = time.strptime(now_timestamp_str, "%Y-%m-%d %H:%M:%S")
        now_hour = now_timestamp.tm_hour
        now_wday = now_timestamp.tm_wday

        if last_run_time is None or last_run_time == '':
            if int(now_hour) == int(week_time) and int(now_wday) == int(week_day):
                is_true = True
        else:
            last_run_time_str = str(last_run_time)
            pos = last_run_time_str.find(':')
            last_run_time_str = last_run_time_str[:pos] + ":00:00"
            last_run_time = time.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")

            last_run_hour = last_run_time.tm_hour
            last_run_wday = last_run_time.tm_wday

            logger.info("by week:last_run_time: " + last_run_time_str)
            # myprint(week_time+now_hour+last_run_hour)
            # myprint(week_day+now_wday+last_run_wday)
            logger.info("***\n")

            if now_wday != last_run_wday:
                if int(now_hour) == int(week_time) and int(now_wday) == int(week_day):
                    is_true = True
            else:
                if now_hour != last_run_hour and int(now_hour) == int(week_time) and int(now_wday) == int(week_day):
                    is_true = True

    if is_true:
        cursor.execute("""update alert_rules set last_run_time = CURRENT_TIMESTAMP(0) where id = %s""", (rule['id'],))
    return is_true


def alert_rule(pg_cur, ms_cur):
    sql = """SELECT * FROM alert_rules"""
    pg_cur.execute(sql)

    for rule in pg_cur.fetchall():
        logger.info("***Alert Rule***")
        if not rule['active']:
            logger.info(rule['rule_name'] + ' is inactive')
            continue
        else:
            logger.info("rule_name: " + rule['rule_name'])
            is_true = judge_time(rule, pg_cur)
            if not is_true:
                continue
            return_arr = judge_define_condition(rule, ms_cur)

            sql = """INSERT INTO alert_rules_log(alert_rules_id, create_on,limit_asr,limit_abr,limit_acd,limit_pdd,limit_revenue,limit_profitability,limit_asr_value,limit_abr_value,limit_acd_value,limit_pdd_value,limit_revenue_value,limit_profitability_value) VALUES (%s, CURRENT_TIMESTAMP(0),%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s) returning id"""
            pg_cur.execute(sql, (
                rule['id'], rule['asr'], rule['abr'], rule['acd'], rule['pdd'], rule['revenue'],
                rule['profitability'], rule['asr_value'], rule['abr_value'], rule['acd_value'],
                rule['pdd_value'],
                rule['revenue_value'], rule['profitability_value']))
            alert_rules_log_id = pg_cur.fetchone()
            alert_rules_log_id = alert_rules_log_id['id']
            if return_arr == {}:
                save_finish_alert_rule_log(pg_cur, alert_rules_log_id, 0)
                continue
            else:
                # log_detail
                return_arr = save_return_arr_to_detail(rule, alert_rules_log_id, return_arr, pg_cur)

                # 是否block
                is_block = rule['is_block']
                if not is_block:
                    save_log_detail(pg_cur, False, 'block_false', return_arr)
                    logger.info("not block")
                else:
                    logger.info("block")
                    block(rule, return_arr, pg_cur)

                is_email = rule['is_email']
                if not is_email:
                    save_log_detail(pg_cur, False, 'email_false', return_arr)
                    logger.info("not send mail")
                else:
                    save_log_detail(pg_cur, True, 'email_true', return_arr)
                    logger.info("send mail")
                    email(rule, return_arr, pg_cur)

                save_finish_alert_rule_log(pg_cur, alert_rules_log_id, 1)
        logger.info("##########")


def connect_to_memsql(host, port, user, password, db):
    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db,
                               cursorclass=MySQLdb.cursors.DictCursor)
    except:
        raise 'Unable to connect to the MemSQL'
    return conn, conn.cursor()


def connect_to_postgresql(host, port, database, user, password=None):
    try:
        conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        conn.autocommit = True
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except:
        raise 'I am unable to connect to the PostgreSQL'
    return conn, cursor


def process_loop(pg_cur, ms_cur, sc):
    alert_rule(pg_cur, ms_cur)
    sc.enter(Config['MONITORING_CYCLE_DELAY'], 1, process_loop, (pg_cur, ms_cur, sc,))


def main():
    pg, pg_cur = connect_to_postgresql(Config['POSTGRESQL_HOST'], Config['POSTGRESQL_PORT'],
                                       Config['POSTGRESQL_DATABASE'], Config['POSTGRESQL_USER'],
                                       Config['POSTGRESQL_PASSWORD'])
    ms, ms_cur = connect_to_memsql(host=Config['MEMSQL_HOST'], port=Config['MEMSQL_PORT'], user=Config['MEMSQL_USER'],
                                   password=Config['MEMSQL_PASSWORD'], db=Config['MEMSQL_DB'])

    s = sched.scheduler(time.time, time.sleep)
    s.enter(0, 1, process_loop, (pg_cur, ms_cur, s,))
    s.run()

    pg.close()
    ms.close()


if __name__ == '__main__':
    main()
