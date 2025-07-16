#!/usr/bin/env python

# coding: utf-8

 

# In[1]:

 

 

from datetime import date, timedelta, datetime

from airflow import DAG

#from airflow.contrib.hooks.vertica_hook import VerticaHook

#from airflow.operators.bash_operator import BashOperator

from airflow.models.connection import Connection

from airflow.operators.python_operator import PythonOperator

from airflow.operators.python_operator import BranchPythonOperator

from airflow.operators.dummy_operator import DummyOperator

#from airflow.providers.sftp.operators.sftp import SFTPOperator

from airflow.operators.email_operator import EmailOperator

from airflow.utils.email import send_email

 

 

# In[ ]:

 

 

import pendulum


local_tz = pendulum.timezone("Europe/Kiev")

 

default_arg=dict(

    start_date=datetime(2023, 10, 19, tzinfo=local_tz),

    owner='omaslak'

)

 

# In[ ]:

 

dag= DAG(

    dag_id="dud_birthday",

    default_args= default_arg,

    tags = ['mail','daily','SAS'],

    schedule='30 8 * * * ',

    catchup=False)

 

 

# In[ ]:

 

 

def Birthday_DF(**kwargs):

    import saspy

    import pandas as pd

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

    req_STAFF_PERSONS = '''PROC SQL;

       CREATE TABLE WORK.DUD AS

       SELECT t1.SAPID,

          t1.NUALAST,

          t1.NUAFIRST,

          t1.NUAMIDDLE,

          t1.NUASHORT,

          t1.JOBTITLEUA,

          t1.DEPNAMEUA,

          t1.BIRTHDATE,

          t1.SEX,

          t1.HIREDATE,

          t1.EMAIL

      FROM T_MTSU.STAFF_PERSONS t1

      WHERE t1.DEPARTMENTSTREEUA CONTAINS

           'Група компаній ВФ#ПрАТ "ВФ Україна"#Фінансова дирекція#Департамент управління доходами';

        QUIT;

        '''

 

    k = sas.submit(req_STAFF_PERSONS)

    DUD = sas.sd2df(table="DUD")

    #DUD = DUD[DUD['BIRTHDATE']=='06.03']

    #DUD['DR_TODAY']= True

    DUD['DR_TODAY']= datetime.now().strftime("%d.%m") == DUD['BIRTHDATE']

   

    

    DR = DUD[DUD['DR_TODAY']==True] 

    man_df = DR[DR['SEX']=='муж']

    woman_df = DR[DR['SEX']=='жен']

   

    DR.to_csv(r"/opt/airflow/dags/files/DR.csv",index=False,sep=';')

    man_df.to_csv(r"/opt/airflow/dags/files/man_df.csv",index=False,sep=';')

    woman_df.to_csv(r"/opt/airflow/dags/files/woman_df.csv",index=False,sep=';')

 

 

# In[ ]:

 

 

Birthday_DF_operator = PythonOperator(

    task_id='Birthday_DF',

    python_callable= Birthday_DF,

    provide_context=True,

    dag=dag)

 

 

# In[ ]:

 

 

def choose_branch(**kwargs):

    import saspy

    import pandas as pd

   

    DR = pd.read_csv(r"/opt/airflow/dags/files/DR.csv",sep=';')

   

    man_df = pd.read_csv(r"/opt/airflow/dags/files/man_df.csv",sep=';')

    woman_df = pd.read_csv(r"/opt/airflow/dags/files/woman_df.csv",sep=';')

    #DR = pd.DataFrame.DR

   

    if len(man_df)>0:

           

        return ['send_man_email']

       

    else:

        print('Nothing to do')

       

    if len(woman_df)>0:

           

        return ['send_woman_email']

    

    else:

        print('Nothing to do')

 

#branching_step_operator = BranchPythonOperator(

#        task_id='branching_step',

#        python_callable=choose_branch,

#        provide_context=True,

#        dag=dag

#    )

 

 

# In[ ]:

 

 

def send_man_df(**kwargs):

    import saspy

    import pandas as pd

    DR_man = pd.read_csv(r"/opt/airflow/dags/files/man_df.csv",sep=';')

   

    if len(DR_man)>0:

   

        for i in DR_man['SAPID']:

           

            t = DR_man[DR_man['SAPID']==i]

       

            send_man_email = send_email(to=[ t['EMAIL'].iloc[0]],

                   cc =['dud@vodafone.ua'],

                   subject=f'День народження '+str(f"{t['NUAFIRST'].iloc[0]}")+' '+str(f"{t['NUALAST'].iloc[0]}")+'',

                  

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

 

                <p class="small">Дорогий '''+str(f"{t['NUAFIRST'].iloc[0]}")+'''!</p>

                <p class="small">Від усього колективу вітаємо тебе з днем народження!<br>

                Нехай щастя супроводжує тебе на кожному кроці, здоров'я оберігає за будь-яких обставин,<br>

                удача та везіння стануть твоїми надійними супутниками у житті.<br>

                Бажаємо натхнення у всьому, позитивного настрою та виконання всіх планів.<br>

                Залишайся таким же сильним і надійним.<br></p>     

                <p class="small">З любов'ю, Департамент управління доходами<br></p>

              </body>

            </html>''')

    else:

        print('Nothing to do')

 

# In[ ]:

 

 

def send_woman_df(**kwargs):

    import saspy

    import pandas as pd

    DR_woman = pd.read_csv(r"/opt/airflow/dags/files/woman_df.csv",sep=';')

    #woman_df = pd.read_csv(r"/opt/airflow/dags/files/woman_df.csv",sep=';')

   

    if len(DR_woman)>0:

        for i in DR_woman['SAPID']:

           

            t = DR_woman[DR_woman['SAPID']==i]

       

            send_woman_email = send_email(to=[ t['EMAIL'].iloc[0]],

                   cc =['dud@vodafone.ua'],

                   subject=f'День народження '+str(f"{t['NUAFIRST'].iloc[0]}")+' '+str(f"{t['NUALAST'].iloc[0]}")+'',

                  

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

 

                <p class="small">Люба '''+str(f"{t['NUAFIRST'].iloc[0]}")+'''!</p>

                <p class="small">Від усього колективу вітаємо тебе з днем народження!<br></p>

                <p class="small">Бажаємо, щоб життя було барвистою мозаїкою, складеною з душевної гармонії,<br>

                внутрішнього світла, нескінченного позитиву, і звичайно, безмежного здоров'я.<br>

                Нехай удача завжди йде з тобою пліч-о-пліч, а негаразди і смуток не знайдуть дорогу до тебе і твоїх близьких.<br>

                Щастя, успіхів в досягненні цілей, благополуччя!<br></p>    

                <p class="small">З любов'ю, Департамент управління доходами<br></p>

              </body>

            </html>''')

    else:

        print('Nothing to do')

 

man_df_email_operator = PythonOperator (task_id = "send_man_email",

                                 python_callable = send_man_df,

                                 provide_context = True,

                                 dag=dag)

 

       

        

woman_df_email_operator = PythonOperator (task_id = "send_woman_email",

                                 python_callable = send_woman_df,

                                 provide_context = True,

                                 dag=dag)

 

 

# In[ ]:

 

 

Birthday_DF_operator >> [man_df_email_operator,woman_df_email_operator] #branching_step_operator >> [man_df_email_operator,woman_df_email_operator]