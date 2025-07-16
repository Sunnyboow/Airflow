from datetime import  timedelta, datetime
#import datetime


from airflow import DAG
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.operators.bash_operator import BashOperator
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email

import pandas as pd
import pyodbc
from datetime import date, timedelta, datetime
import os
from shutil import copyfile
import pendulum

local_tz = pendulum.timezone("Europe/Kiev")

default_arg=dict(

    start_date=datetime(2023, 11, 11, tzinfo=local_tz),

    owner='omaslak'

)


dag= DAG(
    dag_id="Night_payments_desynch",
    default_args= default_arg,
    tags = ['mail','daily','vertica_sps'],
    schedule='11 00 * * * ',
    catchup=False)


get_list_desync_file_operator = SFTPOperator(
    task_id="get_list_desync_file",
    ssh_conn_id="tableau",
    local_filepath=r"/opt/airflow/dags/files/list_for_sync.csv",
    remote_filepath=r"/airflow_dags/constant_files/Night_payments_desynch/list_for_sync.csv",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag

)

 

def get_night_payments():

    import pandas as pd
    import numpy as np
    #import pyodbc
  

    from datetime import datetime, timedelta, date


    day = 0      # Змініть цифру, якщо потрібен інший день. Вчора = 1
    dt_date = date.today()-timedelta(days=day)
    dt_date = dt_date.strftime("%Y%m%d")

    conn= VerticaHook('vertica_sps').get_conn()


    sql_n_payments = '''

    select t1.msisdn,
    t1.date_of_creation as date_of_fact_payment,
    count(t1.cdr_number) as payment_cnt,
    t2.payment_sum,
    t2.date_of_creation as date_of_creation_on_mscp,
    t2.operatorid,
    t2.actual_balance,
    t3.uah as aspn_balance,
    t4.withdraw_sum,
    t4.date_of_withdraw

     from sps.mscp_ser t1

    left join (
    select msisdn,
    balance_delta/1000000 as payment_sum,
    date_of_creation ,
    operatorid,
    actual_balance/1000000 as actual_balance

    from sps.mscp_ser

    where date_of_creation::date >= current_date -'''+str(day)+'''
    and operatorid in ('2470','') --  or activity_result = 3
    and balance_delta!=0
    and balance_delta > 0)
    
    t2 on t1.msisdn = t2.msisdn

    left join
    (select msisdn,uah
    from sps.aspn_'''+str(dt_date)+''')

    t3 on t1.msisdn = t3.msisdn

    left join
     (select msisdn,
     balance_delta/1000000 as withdraw_sum,
     date_of_creation as date_of_withdraw
     from sps.mscp_ser
      where date_of_creation::date = current_date - '''+str(day)+'''
      and balance_delta < 0)
      t4 on t1.msisdn = t4.msisdn

    where

     t1.date_of_creation::date = current_date - '''+str(day)+''' and
     t1.date_of_creation::time between '00:00:00' and '06:00:00'and
     t1.interface_type in ('255') and
     t1.operatorid is null and
     t1.activity_cause in ('19','5') and
     t1.activity_type in ('6') and
     t1.activity_result in ('4294807290','4294816296','3')

    group by
    t1.msisdn,
    t1.date_of_creation,
    t1.BALANCE_DELTA,
    t2.payment_sum,
    t2.date_of_creation,
    t2.operatorid,
    t2.actual_balance,
    t3.uah,
    t4.withdraw_sum,
    t4.date_of_withdraw

    order by t1.date_of_creation asc
    '''


    df = pd.read_sql_query(sql_n_payments,conn)
    df.to_csv(r"/opt/airflow/dags/files/night_payments.csv",index=False,sep=';')


get_night_payments_operator = PythonOperator(
    task_id ='get_night_payments',
    python_callable = get_night_payments,
    op_kwargs = {'date_start': 1},
    dag = dag)

def creating_koeffs():

    from datetime import date, timedelta, datetime

    # Dates range for getting Requests for Recovery (Previous week)

    day = 0      # Змініть цифру, якщо потрібен інший день. Вчора = 1
    dt_date = date.today()-timedelta(days=day)
    dt_date = dt_date.strftime("%Y%m%d")

    df = pd.read_csv(r"/opt/airflow/dags/files/night_payments.csv",sep=';')

    #creating new data and calculating koeficients
    msisdns = df['msisdn'].drop_duplicates()
    test1 = []

    for i in msisdns:

        msisdn_df = df[df['msisdn']== i ]#.sort_values(by='app_n', ascending=False)
        date = msisdn_df['date_of_fact_payment'].min()

       

        test1num_fact_paym = msisdn_df[['msisdn','date_of_fact_payment','payment_sum',

                                        'date_of_creation_on_mscp','actual_balance']].drop_duplicates()

        payment_sum = test1num_fact_paym['payment_sum'].sum()
        actual_balance = test1num_fact_paym['actual_balance'].max()   
        date_of_creation_on_mscp = test1num_fact_paym['date_of_creation_on_mscp'].min()

        withdraw_test = msisdn_df[['msisdn','withdraw_sum','date_of_withdraw']].drop_duplicates()
        withdraw_sum = withdraw_test['withdraw_sum'].sum()
        date_of_withdraw = withdraw_test['date_of_withdraw'].min()

        #koef = (msisdn_df['actual_balance'].max() - msisdn_df['aspn_balance'].min())/msisdn_df['payment_sum'].sum()
        koef = (actual_balance - msisdn_df['aspn_balance'].min())/payment_sum

        data = {'msisdn': [i],'date_of_fact_payment':[date],'payment_sum':[payment_sum],
                'actual_balance':[actual_balance],'date_of_creation_on_mscp':[date_of_creation_on_mscp],
                'aspn_balance':[msisdn_df['aspn_balance'].min()], 'withdraw_sum':[withdraw_sum],
                'date_of_withdraw':[date_of_withdraw],'koef': [koef]}

       

        d = pd.DataFrame(data)
        test1.append(d)

    data = pd.concat(test1, axis=0, ignore_index=True)
    # Вичленяємо тількі ті, які викликали розсинхронізацію:
    balances_desync = []
    balances_desync.append(data[round(data['koef'],4)>1])
    balances_desync = pd.concat(balances_desync, axis=0, ignore_index=True)


    try:

        data['date_of_fact_payment'] = data['date_of_fact_payment'].astype('datetime64')
        data['date_of_withdraw'] = data['date_of_withdraw'].astype('datetime64')
        data['date_of_creation_on_mscp'] = data['date_of_creation_on_mscp'].astype('datetime64')
        balances_desync = balances_desync.append(data[(data['date_of_fact_payment'] < data['date_of_withdraw'])&(data['date_of_withdraw']
                        < data['date_of_creation_on_mscp'])])

      

    except:
        print(1)
    balances_desync = balances_desync.drop_duplicates()

    # Створюємо таблицю з необхідними поповненнями

    list_for_sync = balances_desync[['msisdn','date_of_fact_payment','date_of_creation_on_mscp','payment_sum']]

    # Створюємо файл з необхідними поповненнями
    temp = pd.read_csv(r"/opt/airflow/dags/files/list_for_sync.csv",sep=';',encoding = "windows-1251")
    temp['date_of_fact_payment']=pd.to_datetime(temp['date_of_fact_payment'], format='%Y-%m-%d  %H:%M:%S')
    temp['date_of_creation_on_mscp']=pd.to_datetime(temp['date_of_creation_on_mscp'], format='%Y-%m-%d  %H:%M:%S')

    list_for_sync['msisdn'] = list_for_sync[['msisdn']].apply(pd.to_numeric)
    list_for_sync['date_of_fact_payment']=pd.to_datetime(list_for_sync['date_of_fact_payment'], format='%Y-%m-%d  %H:%M:%S')
    list_for_sync['date_of_creation_on_mscp']=pd.to_datetime(list_for_sync['date_of_creation_on_mscp'], format='%Y-%m-%d  %H:%M:%S')
 
    #temp = temp.append(list_for_sync, sort=False)
    temp = pd.concat([temp, list_for_sync], ignore_index=True)
    temp = temp.drop_duplicates()
    temp.to_csv(r"/opt/airflow/dags/files/list_for_sync.csv", index=False,sep=';',encoding = "windows-1251")

    # Пишемо денний файл для відправки 

    list_for_sync.to_excel(r"/opt/airflow/dags/files/night_payments_for_sync_"+str(dt_date)+".xlsx")

creating_koeffs_operator = PythonOperator(

    task_id = 'creating_koeffs',
    python_callable = creating_koeffs,
    op_kwargs = {'date_start': 1},
    dag = dag)

def choose_branch(**kwargs):

    import saspy
    import pandas as pd
    from datetime import date, timedelta, datetime

    # Dates range for getting Requests for Recovery (Previous week)

   

    day = 0      # Змініть цифру, якщо потрібен інший день. Вчора = 1
    dt_date = date.today()-timedelta(days=day)
    dt_date = dt_date.strftime("%Y%m%d")

    list_for_sync = pd.read_excel(r"/opt/airflow/dags/files/night_payments_for_sync_"+str(dt_date)+".xlsx")


    if len(list_for_sync) > 0:#last_day_month:
        return ['send_mail_for_sync']
    else:
        return ['nothing_to_do']

   

branching_step_operator = BranchPythonOperator(
        task_id='branching_step',
        python_callable=choose_branch,
        provide_context=True,
        dag=dag

    )


nothing_to_do_operator = DummyOperator(
    task_id='nothing_to_do',
    dag=dag)

def send_mail_for_sync():
    import saspy
    import pandas as pd
    from datetime import date, timedelta, datetime

    day = 0      # Змініть цифру, якщо потрібен інший день. Вчора = 1
    dt_date = date.today()-timedelta(days=day)
    dt_date = dt_date.strftime("%Y%m%d")

    send_email(to=['email@ua'],

                    subject=f'Розсинхронізація балансів через збої платежів у нічний час',
                    files = [r"/opt/airflow/dags/files/night_payments_for_sync_"+str(dt_date)+".xlsx"],

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

 

                    <p class="small">Привіт!</p>
                    <p class="small">Прошу провести перевірку балансів.<br></p>
                    <p class="small">Дякую!<br></p>
                  </body>
                </html>''')


send_mail_for_sync_operator = PythonOperator (task_id = "send_mail_for_sync",
                                 python_callable = send_mail_for_sync,
                                 provide_context = True,
                                 dag=dag)


get_list_desync_file_operator >> get_night_payments_operator >> creating_koeffs_operator >> branching_step_operator >> [send_mail_for_sync_operator, nothing_to_do_operator]

 
