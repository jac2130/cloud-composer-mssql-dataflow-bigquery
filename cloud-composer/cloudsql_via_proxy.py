#!/usr/bin/env python3
      # -*- coding: utf-8 -*-

from datetime import timedelta, datetime, timezone

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import pymssql
#import pymssql.cursors

import pendulum

local_tz = pendulum.timezone("Asia/Tokyo")

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 1, 1, tzinfo=local_tz),
      # 'end_date': datetime(2020, 12, 31),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False
}

dag = DAG('cloudsql_via_proxy', schedule_interval='@once',
               default_args=default_args)


      # Firstly run the dummy operator
dummy = DummyOperator(
    task_id='dummy',
    trigger_rule='all_success',
    dag=dag,
)


def fn_select():
    
    sql = "SELECT * FROM [CLOUDSQL_TABLE]"
         # connect the Cloud SQl via proxy
         # put the 'cluster IP' in the 'host'of the proxy pod
    connection = pymssql.connect(
        host='[SQLPROXY_CLUSTER_IP]',
        port=3306,
        user='[CLOUDSQL_USER]',
        password='[CLOUDSQL_PASSWORD]',
        db='[CLOUDSQL_DB]',
        charset='utf8')
        #cursorclass=pymssql.cursors.DictCursor)

         # try the SELECT query
    with connection.cursor() as cursor:
        cursor.execute(sql)
        dbdata = cursor.fetchall()
        for i in dbdata:
            print(i)
        connection.commit()


fn_select = PythonOperator(
         task_id='fn_select',
         python_callable=fn_select,
         dag=dag)


def fn_insert():
    sql = "INSERT INTO [CLOUDSQL_TABLE] (col1, col2, col3, col4) VALUES (%s, %s, %s, %s)"
    connection = pymssql.connect(
            host='[SQLPROXY_CLUSTER_IP]',
            port=3306,
            user='[CLOUDSQL_USER]',
            password='[CLOUDSQL_PASSWORD]',
            db='[CLOUDSQL_DB]',
            charset='utf8')
            #cursorclass=pymssql.cursors.DictCursor)

         # try the INSERT query
    with connection.cursor() as cursor:
        r = cursor.execute(sql, (9999, "insert2", "insert3", "insert4"))
        connection.commit()


fn_insert = PythonOperator(
         task_id='fn_insert',
         python_callable=fn_insert,
         dag=dag)


def fn_update():
    sql = "UPDATE [CLOUDSQL_TABLE] SET col2 = %s WHERE id = %s"
    connection = pymssql.connect(
            host='[SQLPROXY_CLUSTER_IP]',
            port=3306,
            user='[CLOUDSQL_USER]',
            password='[CLOUDSQL_PASSWORD]',
            db='[CLOUDSQL_DB]',
            charset='utf8')
            #cursorclass=pymssql.cursors.DictCursor)

         # try the UPDATE query
    with connection.cursor() as cursor:
        r = cursor.execute(sql, ("update2", 3))
        connection.commit()


fn_update = PythonOperator(
         task_id='fn_update',
         python_callable=fn_update,
         dag=dag)

#dummy >> fn_select >> fn_insert >> fn_update?