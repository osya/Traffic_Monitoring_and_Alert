#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import psycopg2
import psycopg2.extras
import smtplib
import logging
import time
import datetime as dt
import sched

# TODO: email, block in the resoruce_block

logger = logging.getLogger('Monitoring Rule')
logger.setLevel(logging.INFO)
# add Console logger
ch = logging.StreamHandler()
logger.addHandler(ch)


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
    where_time = " answer_time_of_date  >=   (NOW() - INTERVAL 15 MINUTE)"
    logger.info("where_time: " + where_time)

    trunk_type = rule['trunk_type']
    res_id = rule['res_id'],
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
            group_field += second_group_field
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

    count_sql = """SELECT %s, count(*) as total_attempt ,sum( call_duration >0),
sum(`pdd`) as total_pdd , sum (`egress_cost` ) as total_egress_cost, sum (`ingress_client_cost` ) as total_ingress_cost, sum ( call_duration ) as total_duration

, sum ( call_duration>0)
as non_zero   ,  sum ( ring_time>0) as seizure  FROM `demo_cdr` WHERE %s %s %s %s """ \
                % (group_field, where_time, where_trunk, where_code, group)

    # myprint("count_sql: " + count_sql)
    cursor.execute(count_sql)
    sum = cursor.fetchone()
    # myprint(sum)
    if sum is None:
        sum = 0
    else:
        sum = int(sum[0])
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
            last_run_time = dt.datetime.strptime(last_run_time_str, "%Y-%m-%d %H:%M:%S")
            last_run_time = time.mktime(last_run_time.timetuple())

            every_min = rule['specific_minutes']
            if every_min is None or every_min == '':
                every_min = 1
            every_min = int(every_min)
            logger.info("interval time:" + str(every_min))
            logger.info("***\n")
            if every_min == 1:
                is_true = True
            else:
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

            logger.info("by week:last_run_time" + last_run_time_str)
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
                if is_block != True:
                    save_log_detail(pg_cur, False, 'block_false', return_arr)
                    logger.info("not block")
                else:
                    logger.info("block")
                    block(rule, return_arr, pg_cur)

                is_email = rule['is_email']
                if is_email != True:
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
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db)
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
    sc.enter(60, 1, process_loop, (pg_cur, ms_cur, sc,))


def main():
    pg, pg_cur = connect_to_postgresql('localhost', 5432, 'class4_pr', 'postgres')
    ms, ms_cur = connect_to_memsql(host="209.126.102.168", port=3306, user="root", password="test123#", db="test")

    s = sched.scheduler(time.time, time.sleep)
    s.enter(0, 1, process_loop, (pg_cur, ms_cur, s,))
    s.run()

    pg.close()
    ms.close()

if __name__ == '__main__':
    main()
