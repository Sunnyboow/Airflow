#!/usr/bin/env python

# coding: utf-8

 

# In[ ]:

 

 

from datetime import date, timedelta, datetime

from airflow import DAG

#from airflow.contrib.hooks.vertica_hook import VerticaHook

from airflow.operators.bash_operator import BashOperator

from airflow.models.connection import Connection

from airflow.operators.python_operator import PythonOperator

from airflow.operators.python_operator import BranchPythonOperator

from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.sftp.operators.sftp import SFTPOperator

from airflow.operators.email_operator import EmailOperator

from airflow.utils.email import send_email

 

 

# In[ ]:

 

 

import pendulum


local_tz = pendulum.timezone("Europe/Kiev")

 

default_arg=dict(

    start_date=datetime(2023, 10, 31, tzinfo=local_tz),

    owner='omaslak'

)

 

# In[ ]:

 

dag= DAG(

    dag_id="payments_1st_month",

    default_args= default_arg,

    tags = ['mail','daily','SAS','Stat.csv',"Recovered_not_correct.xlsx"],

    schedule='00 15 * * * ',

    catchup=False)

 

 

# In[ ]:

 

 

from datetime import date, timedelta, datetime

# Dates range for getting Requests for Recovery (Previous week)

dt_date = date.today()-timedelta(days=1)

prev_date = date.today()-timedelta(days=2)

 

 

# In[ ]:

 

 

def choose_branch(**kwargs):

    import saspy

    import pandas as pd

    from datetime import date, timedelta, datetime

   

 

    # Dates range for getting Requests for Recovery (Previous week)

    dt_date = date.today()-timedelta(days=1)

    prev_date = date.today()-timedelta(days=2)

 

 

    if date.today().day == 2:#last_day_month:

        return ['last_day_month']

    else:

        return ['recovery_requests_from_bill']

   

branching_step_operator = BranchPythonOperator(

        task_id='branching_step',

        python_callable=choose_branch,

        provide_context=True,

        dag=dag

    )

 

 

# In[ ]:

 

 

last_day_month_operator = DummyOperator(

    task_id='last_day_month',

    dag=dag)

 

 

# In[ ]:

 

 

def recovery_requests_from_bill():

    import oracledb

    import ast

    import pandas as pd

    import numpy as np

    from datetime import date, timedelta, datetime

   

    

    credentials=Connection.get_connection_from_secrets("Oracle_BILL")

    extra = ast.literal_eval(credentials.extra)  

    print (type(extra.get('dns')),extra.get('dns'))

    connection = oracledb.connect(user = str(credentials.login)

                                  , password = str(credentials.password)

                                  , dsn = extra.get('dns'))

 

   

    # Dates range for getting Requests for Recovery (Previous week)

    dt_date = date.today()-timedelta(days=1)

    prev_date = date.today()-timedelta(days=2)

 

    if datetime.today().month-1 == 1:

        m = 12

        print(m)

    else:

        m = datetime.today().month-2

        print(m)

 

            #Getting dataframe with requests for recovery

    rec_pop ="""

            Select t.* ,

            tp.TARIFF_PLAN_NAME as recovery_tariff_plan_name,

            tp.TARIFF_PLAN_CODE as recovery_tariff_plan_code

 

            from

            (

            select /*+ parallel(10)*/ --вигрузка виконаних реквестів на відновлення

            r.req_id,

            r.date_of_req,

            r.date_of_complete,

            d.DEACTIVATION_DATE,

            cast(str.val as varchar2(20)) msisdn,

            last_day(r.date_of_complete) + 1 - r.date_of_complete day_to_end,

            (select tp.TARIFF_PLAN_ID  -- ID TP

                from cust.terminal_device@crm.mts.com.ua t, cust.tariff_plan@crm.mts.com.ua tp

               where t.msisdn = cast(str.val as varchar2(20))

                 and t.tariff_plan_id = tp.TARIFF_PLAN_ID

                 and r.date_of_complete between t.date_from and nvl(t.date_to, sysdate)) TARIFF_PLAN_ID,

                 uc.user_nt_name, uc.user_full_name

 

               from crm.request@crm.mts.com.ua r, crm.att_str@crm.mts.com.ua str

               left join

               (Select  max(r.DATE_OF_COMPLETE) DEACTIVATION_DATE, cast(str.val as varchar2(20)) msisdn

 

                from crm.request@crm.mts.com.ua r, crm.att_str@crm.mts.com.ua str

                where

                r.req_type_id = 100

                and str.att_type_id = 48

                and r.req_id = str.req_id

                and r.date_of_req between

                to_date('01."""+str(m)+'.'+str(datetime.today().year)+""" 00:00:00', 'dd.mm.yyyy hh24:mi:ss') and

                to_date('"""+str(f"{dt_date:%d.%m.%Y}")+""" 00:00:00', 'dd.mm.yyyy hh24:mi:ss')

 

                group by  cast(str.val as varchar2(20))  ) d on d.msisdn = cast(str.val as varchar2(20)) , crm.users_crm@crm.mts.com.ua uc

 

            where r.req_type_id = 596 -- ID стандартного реквеста 596 Восстановление обслуживания по контракту

               and r.date_of_complete is not null

               and r.date_of_req between

                   to_date('"""+str(f"{prev_date:%d.%m.%Y}")+""" 00:00:00', 'dd.mm.yyyy hh24:mi:ss') and

                   to_date('"""+str(f"{dt_date:%d.%m.%Y}")+""" 00:00:00', 'dd.mm.yyyy hh24:mi:ss')

               and r.req_id = str.req_id

               and str.att_type_id = 48 --вывод одного из атрибутов реквеста, который соответствует  Номеру телефона

               and uc.user_id = r.user_id

 

               ) t

            left join cust.tariff_plan@crm.mts.com.ua tp on  t.tariff_plan_id = tp.TARIFF_PLAN_ID

 

            """

    rec_pop = pd.read_sql_query(rec_pop,connection)

    rec_pop

 

    print('Заяви на відновлення прогрузились')

 

 

# Відсікаємо тих, у кого деактивація і відновлення номеру проведені в один і той же місяць

    rec_pop[pd.DatetimeIndex(rec_pop['DATE_OF_COMPLETE']).month != pd.DatetimeIndex(rec_pop['DEACTIVATION_DATE']).month]

 

 

# SQL in this block can run only 1000 msisdn using tuple. So we need to split rec_pop DF to chunks.

 

# Function for chunks

    n = 999  #chunk row size

    list_df = [rec_pop[i:i+n] for i in range(0,rec_pop.shape[0],n)]

#list_df

    chunk_qty = round(len(rec_pop)/n)

 

    if chunk_qty == 0:

        chunk_qty = 1

    else:

        chunk_qty

 

#Getting actual states for all needed msisdns

 

    table =[]

 

    for i in range(0,chunk_qty):

 

        tup = list_df[i]['MSISDN']

 

        actual_state = """select td.msisdn, td.tariff_plan_id as actual_tariff_plan_id , tp.TARIFF_PLAN_NAME as act_TP_NAME, pa.billing_group_id as actual_billing_group_id, bg.BILLING_GROUP_NAME

                from cust.terminal_device@crm.mts.com.ua td, cust.personal_account_td@crm.mts.com.ua pat, cust.personal_account@crm.mts.com.ua pa, cust.billing_group@crm.mts.com.ua bg, cust.tariff_plan@crm.mts.com.ua tp

                where td.msisdn in

                {} and td.date_to is null and td.terminal_device_id = pat.terminal_device_id and pat.personal_account_id = pa.personal_account_id

                and pat.date_to is null and pa.date_to is null and bg.BILLING_GROUP_ID = pa.billing_group_id

                and td.tariff_plan_id = tp.TARIFF_PLAN_ID

                """.format(tuple(tup))

        actual_state1 = pd.read_sql_query(actual_state,connection)

 

        table.append(actual_state1)

 

        actual_state = pd.concat(table, axis=0, ignore_index=True)

#actual_state = data.drop_duplicates()

        actual_state

 

# Creating DF with recovered abons with actual state

        recovered_abons = pd.merge(rec_pop,actual_state, how = 'left', on = ['MSISDN'])

        recovered_abons = recovered_abons.fillna('deactivated for today')

        recovered_abons

 

        recovered_abons.to_csv(r"/opt/airflow/dags/files/recovered_abons.csv",index=False,sep=';')

 

        del rec_pop

        del actual_state

 

        print('Актуальні ТП підтягнуті')

 

#Getting dictionary with all actual services like 'Плата%(1-й місяць)'

        services = r"""select tp.tariff_plan_id, tp.tariff_plan_code, tp.tariff_plan_name, concat(s.service_id,'.08') as service_id, s.service_code, s.service_name

                from rd.tariff_plan_service@crm.mts.com.ua ts, rd.tariff_plan@crm.mts.com.ua tp, rd.service@crm.mts.com.ua s

                where 1=1

                and ts.tariff_plan_id = tp.tariff_plan_id

                and ts.service_id = s.service_id

                and (s.service_name like 'Плата%(1-й місяць)' or s.service_name like 'Плата%(1-ий місяць)')

                and ts.end_date is null"""

        df_services = pd.read_sql_query(services,connection)

        df_services['SERVICE_ID'] = df_services['SERVICE_ID'].astype('float')

        df_services.to_csv(r"/opt/airflow/dags/files/df_services.csv",index=False,sep=';')

 

 

# In[ ]:

 

 

get_stat_file_operator = SFTPOperator(

    task_id="get_stat_file",

    ssh_conn_id="tableau",

    local_filepath=r"/opt/airflow/dags/files/Stat.csv",

    remote_filepath=r"/airflow_dags/constant_files/Recovered_1st_month/Stat.csv",

    operation="get",

    create_intermediate_dirs=True,

    dag=dag

)

 

 

# In[ ]:

 

 

def getting_srvp_services_and_blocks():

    import saspy

    import numpy as np

    import pandas as pd

 

   

    df_services = pd.read_csv(r"/opt/airflow/dags/files/df_services.csv",sep=';')

    recovered_abons = pd.read_csv(r"/opt/airflow/dags/files/recovered_abons.csv",sep=';')

 

    conn=Connection.get_connection_from_secrets("sas_connection")

    SAS_Enviroment = {

                    "java": "/usr/bin/java",

                    "iomhost": str(conn.host),

                    "omruser": str(conn.login),

                    "omrpw": str(conn.password),

                    "iomport": str(conn.port),

                    "encoding": "cyrillic",

                }

    sas = saspy.SASsession(results="pandas", **SAS_Enviroment)

 

    #Getting all connected services from list above

    req_SRVP = '''PROC SQL;

                CREATE TABLE WORK.SRVP AS

                SELECT ('380'||t2.phone_num) AS MSISDN,

                t1.* FROM D_SRV.SRVP_MR13 t1

                   LEFT JOIN T_MTSU.APP_N_UNIQ t2 ON (t1.app_n = t2.app_n)

                WHERE t1.service in {}

                and t2.is_closed = 0;

                QUIT;'''.format(tuple(df_services['SERVICE_ID'].astype(float)))

 

    sas.submit(req_SRVP)

    SRVP_DF =  sas.sd2df(table='SRVP')

 

 

 

    SRVP_DF_new = pd.merge(SRVP_DF,df_services[['SERVICE_ID','SERVICE_NAME']], how = 'left', left_on = 'service', right_on = 'SERVICE_ID')

    SRVP_DF_new = SRVP_DF_new.drop_duplicates()

    del SRVP_DF

 

    print('Сервіси з SRVP підтягнуті')

 

    recovered_abons['MSISDN'] = recovered_abons['MSISDN'].astype(int)

    SRVP_DF_new['MSISDN'] = SRVP_DF_new['MSISDN'].astype(int)

    Recovered_w_1month = pd.merge(recovered_abons, SRVP_DF_new[['MSISDN','SERVICE_ID','SERVICE_NAME','date_from','date_to']], how = 'left', on ='MSISDN')

 

 

    #Getting 'Блокування по низькому балансу' with balance >0.02 uah

    req_BLOCK = '''PROC SQL;

           CREATE TABLE WORK.BLOCKING AS

              SELECT t1.app_n,

                  t2.acc_n,

                  ('380'||t2.phone_num) AS MSISDN,

                  t2.CALCULATION_METHOD_ID,

                  t1.service,

                  t1.date_from,

                  t1.date_to,

                  t2.is_closed,

                  t3.UAH

              FROM D_APP.BLOCKING t1

                   LEFT JOIN T_MTSU.APP_N_UNIQ t2 ON (t1.app_n = t2.app_n)

                   LEFT JOIN D_LOC.ASPN_'''+str(f"{dt_date:%Y%m%d}")+''' t3 ON (t1.app_n = t3.TD_ID)

              WHERE t1.service in (339.08, 1070.08, 2104.08, 6071.08,345.08, 479.08, 1471.08) AND t1.date_to = '1Jan2040:0:0:0'dt

              /* AND t3.UAH >= 0.02*/

              ;

        QUIT;'''

    sas.submit(req_BLOCK)

    BLOCK =  sas.sd2df(table='BLOCKING')

    BLOCK = BLOCK.rename(columns={"date_from": "Block_date","service": "Block_service_ID", "UAH":"Balance"})

    BLOCK

 

 

 

    def categorise(row): 

        if row['Block_service_ID'] == 339.08:

            return 'Блокування по низькому балансу'

        elif row['Block_service_ID'] == 1070.08:

            return 'Повне блокування по низькому балансу'

        elif row['Block_service_ID'] == 2104.08:

            return 'Примусове блокування за несплату рахунку'

        elif row['Block_service_ID'] == 6071.08:

            return 'Блокировка по решению абонента'

        elif row['Block_service_ID'] == 345.08:

            return 'Часткове блокування'

        elif row['Block_service_ID'] == 479.08:

            return 'Блокування "Апарат втрачено"'

        elif row['Block_service_ID'] == 1471.08:

            return 'Блокування за рiшенням оператора'

        return ''

 

    BLOCK['Blocking_NAME'] = BLOCK.apply(lambda row: categorise(row), axis=1)

    BLOCK

 

    print('Блокування підтягнуті')

 

    Recovered_w_1month['MSISDN']= Recovered_w_1month['MSISDN'].astype(int)

    BLOCK['MSISDN']= BLOCK['MSISDN'].astype(int)

 

    Recovered_w_1month = pd.merge(Recovered_w_1month, BLOCK[['MSISDN','Blocking_NAME','Block_service_ID','Block_date','Balance']], how = 'left', on = 'MSISDN')

    Recovered_w_1month

 

    #Recovered_correct

    Recovered_correct = Recovered_w_1month[Recovered_w_1month['DATE_OF_REQ']<Recovered_w_1month['date_from']].drop_duplicates()

    #Recovered_correct.to_excel('R://RA_EXCHANGE//omaslak//Recovered_customers//Recovered_correct_'+str(f"{prev_date:%d.%m.%Y}")+'.xlsx', index=False)

 

    #Recovered_not_correct

    Recovered_not_correct = Recovered_w_1month[Recovered_w_1month['SERVICE_ID'].isna()]

    t = Recovered_w_1month[Recovered_w_1month['DATE_OF_REQ']>=Recovered_w_1month['date_from']]

    t = t.drop(["SERVICE_ID","SERVICE_NAME","date_from","date_to"], axis=1)

 

    #Recovered_not_correct_all = Recovered_not_correct.append(t)

    Recovered_not_correct_all = pd.concat([t, Recovered_not_correct], ignore_index=True)

    Recovered_not_correct_all = Recovered_not_correct_all.drop_duplicates()

 

    df = pd.merge(Recovered_not_correct_all, Recovered_correct[['MSISDN']], on=['MSISDN'], how="outer", indicator=True)

    df = df[df['_merge'] == 'left_only']

    df = df.drop(["_merge"], axis=1)

    df = df[df['ACTUAL_BILLING_GROUP_ID']!= 1192]

    df = df.drop(["SERVICE_ID","SERVICE_NAME","date_from","date_to"], axis=1)

 

    df.to_excel(r"/opt/airflow/dags/files/Recovered_not_correct_"+str(f"{prev_date:%d.%m.%Y}")+".xlsx", index=False)

 

    print('Файл Recovered_not_correct створено')

 

 

    d = {'Date': [str(f"{prev_date:%d.%m.%Y}")], 'QTY_MSISDN': [df['MSISDN'].nunique()]}

    df1 = pd.DataFrame(data=d)

 

 

    stat = pd.read_csv(r"/opt/airflow/dags/files/Stat.csv",sep=';')

 

#     stat = stat.append(df1, sort=False)

#     stat.to_csv(r"/opt/airflow/dags/files/Stat.csv",sep=';')

 

    stat1 = pd.concat([stat, df1], ignore_index=True)

    stat1.to_csv(r"/opt/airflow/dags/files/Stat.csv",sep=';')

 

 

# In[ ]:

 

 

 

def send_recovery_email():

   

    import saspy

    import pandas as pd

 

    Recovered_not_correct=pd.read_excel(r"/opt/airflow/dags/files/Recovered_not_correct_"+str(f"{prev_date:%d.%m.%Y}")+".xlsx")

 

 

    if len(Recovered_not_correct)>0  :

 

        send_email(to=['egolovan@vodafone.ua'],

                    cc =['omaslak@vodafone.ua', 'vlushik@vodafone.ua'],

                    subject=f'Підключення "Плата%(1-й місяць)" відновленим ПоП '+str(f"{prev_date:%d.%m.%Y}")+'',

                    files = [r"/opt/airflow/dags/files/Recovered_not_correct_"+str(f"{prev_date:%d.%m.%Y}")+".xlsx"],

                            html_content="""<html>

                <head>

                <style>

                p.small {

                  line-height: 1;

                  font-family: "Calibri", "sans-serif";

                }

                </style>

                </head>

                <body>"""+f'''

 

                    <p class="small">Катерина, доброго дня!</p>

                    <p class="small">Прошу виконати підключення послуги «Плата за ТП …(1-місяць)» абонентам із файлу у вкладенні,<br></p>

                    <p class="small">яким виповнили відновлення номеру телефону.</p>

                    <p class="small">А також зняти блокування тим абонентам, у кого вони залишились (останні 4 колонки файлу).</p> 

                    <p class="small">Дякую!<br></p>

                  </body>

                </html>''')

 

    else:

        print('Nothing to send')

 

 

# In[ ]:

 

 

recovery_requests_from_bill_operator = PythonOperator(

    task_id='recovery_requests_from_bill',

    python_callable=recovery_requests_from_bill,

    op_kwargs={'date_start': 1},

    dag=dag)

 

srvp_services_and_blocks_operator = PythonOperator(

    task_id='getting_srvp_services_and_blocks',

    python_callable=getting_srvp_services_and_blocks,

    op_kwargs={'date_start': 1},

    dag=dag)

 

put_stat_file_operator = SFTPOperator(

    task_id="put_stat_file",

    ssh_conn_id="tableau",

    local_filepath=r"/opt/airflow/dags/files/Stat.csv",

    remote_filepath=r"/airflow_dags/constant_files/Recovered_1st_month/Stat.csv",

    operation="put",

    create_intermediate_dirs=True,

    dag=dag

)

 

put_recovered_file_operator = SFTPOperator(

    task_id="put_recovered_file",

    ssh_conn_id="tableau",

    local_filepath=r"/opt/airflow/dags/files/Recovered_not_correct_"+str(f"{prev_date:%d.%m.%Y}")+".xlsx",

    remote_filepath=r"/airflow_dags/constant_files/Recovered_1st_month/Recovered_not_correct_"+str(f"{prev_date:%d.%m.%Y}")+".xlsx",

    operation="put",

    create_intermediate_dirs=True,

    dag=dag

)

 

send_recovery_email_operator = PythonOperator (task_id = "send_recovery_email",

                                 python_callable = send_recovery_email,

                                 provide_context = True,

                                 dag=dag)

 

dell_local_stat_file_operator = BashOperator(

    task_id='dell_local_stat_file',

    bash_command='rm /opt/airflow/dags/files/Stat.csv',

    dag=dag)

 

dell_recovered_not_correct_file_operator = BashOperator(

    task_id='dell_local_rec_not_cor_file',

    bash_command='rm /opt/airflow/dags/files/Recovered_not_correct_'+str(f"{prev_date:%d.%m.%Y}")+'.xlsx',

    dag=dag)

 

 

# In[ ]:

 

 

get_stat_file_operator >>branching_step_operator >> [recovery_requests_from_bill_operator,last_day_month_operator]

recovery_requests_from_bill_operator >> srvp_services_and_blocks_operator >> put_stat_file_operator >> put_recovered_file_operator >> send_recovery_email_operator >> dell_local_stat_file_operator >> dell_recovered_not_correct_file_operator

 