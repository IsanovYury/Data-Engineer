import pendulum

from airflow.models.baseoperator import chain
from airflow.decorators import dag, task

from airflow.operators.empty import EmptyOperator
from airflow.contrib.hooks.mongo_hook import MongoHook

default_args = {
    'owner': 'fyri',
    'provide_context': True,
    'start_date': pendulum.parse('2023-08-14', tz='Asia/Almaty'),
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': True,
    'email': ['isanov1712@gmail.com'],
    'retries': 0,
    'retry_delay': 300.0
}

@dag(
    dag_id="promocodes_v2",
    default_args=default_args,
    schedule='05 8 * * *',
    tags=["Mongo", "PROMOCODES", "GBQ"],
    catchup = False,
    render_template_as_native_obj=True,
    jinja_environment_kwargs={
        "keep_trailing_newline": True,
        # some other jinja2 Environment options here
    }
) 

def transfer_taskflow():
    '''
    #### Миграция промокодов со спеками из Mongo в GBQ
    1. Формируется два df:
        - с новыми и использованными промокодами;
        - со спеками и их общей информацией(описание, количество)
    2. Обновляется информация в GBQ: 
        - новые промокоды и информация об использованных промокодах append к таблице;
        - промокоды, чей end_date равен дню запуска, меняют статус на expired;
        - обновляется информация по спекам.
    '''
    @task()
    def df_to_bq(**kwargs):
        from google.oauth2 import service_account
        from google.cloud import bigquery
        import pandas as pd

        execution_date = kwargs['data_interval_start']
        execution_date_start = execution_date.set(hour=0,minute=0,second=0,microsecond=0).replace(tzinfo=None)
        execution_date_end = execution_date.set(hour=23,minute=59,second=59, microsecond=9999).replace(tzinfo=None)
        
        # 1. Формируем df
        # 1.1. Забираем информацию о спеках
        hook = MongoHook(conn_id='mongo_promocodes')
        client = hook.get_conn()
        database = client["promocode_db"]
        collection = database["specifications"]
        query = [
                {
                "$match":
                    {"created":{"$gte": pendulum.parse('2022-12-01T06:00:00', tz='Asia/Almaty')}}
                },
                {
                "$project":
                    {
                    "_id":1,
                    "title":1
                    }
                }    
                ] 
        cursor = collection.aggregate(query)
        try:
            specifications_df = pd.DataFrame(list(cursor))
        finally:
            client.close()
        specifications_df.rename(columns={"_id": 'specification_id'}, inplace=True)
        specifications_df = specifications_df.astype({'specification_id':'string'})

        # 1.2. Забираем информацию о промокодах
        hook = MongoHook(conn_id='mongo_promocodes')
        client = hook.get_conn()
        database = client["promocode_replica"]
        collection = database["coupons"]
        query = [
                {
                "$match":
                    {"specification_id": {"$in": list(specifications_df['specification_id'])}}
                },
                {
                "$project":
                    {
                    "_id":1,
                    "code":1,
                    "active":1,
                    "specification_id":1,
                    "end":1,
                    "start":1,
                    "limit":1
                    }
                }
                ]
        cursor = collection.aggregate(query)
        try:
            coupons_df = pd.DataFrame(list(cursor))
        finally:
            client.close()
        coupons_df.rename(columns={"_id": 'coupon_id'}, inplace=True)

        # 1.3 Формируем df промокодов со спеками
        # 1.3.1. Join
        coupons_and_spec_df  = coupons_df.merge(specifications_df, on = 'specification_id', how = 'left')

        # 1.3.2. Формируем df с общим количеством промокодов и их максимальным количеством использований по спекам (на день формирования)
        uniq_promo_cnt = coupons_and_spec_df[['specification_id', 'coupon_id']].groupby('specification_id').count()
        uniq_promo_cnt = uniq_promo_cnt.rename(columns={'coupon_id':'unique_promo_cnt'})
        uniq_promo_cnt = uniq_promo_cnt.merge(specifications_df[['title', 'specification_id']], on = 'specification_id', how = 'left')

        max_user_promo_cnt = coupons_and_spec_df[['specification_id', 'limit']].groupby('specification_id').sum()
        max_user_promo_cnt = max_user_promo_cnt.rename(columns={'limit':'user_limit'})
        spec_stats_df = max_user_promo_cnt.merge(uniq_promo_cnt, on = 'specification_id', how = 'left')
        spec_stats_df = spec_stats_df.rename(columns={'title':'specification_title'})

        # 1.4. Формируем df с операциями по промокодам на день запуска дага
        hook = MongoHook(conn_id='mongo_promocodes')
        client = hook.get_conn()
        database = client["promocode_db"]
        collection = database["coupon_operations"]
        query = [
                {
                "$match":
                    {"$and":
                        [
                        {
                        "updated": {"$gte": execution_date_start,
                                    "$lte": execution_date_end},
                        }
                        ]
                    }
                },
                {
                "$project":
                    {
                    "coupon_id":1,
                    "status":1,
                    "updated":1,
                    "user_id":1  
                    }    
                }
        ]
        cursor = collection.aggregate(query)
        try:
            coupons_operation_df = pd.DataFrame(list(cursor))
        finally:
            client.close()
        
        # 1.5. Формируем финальный df
        # 1.5.1. merge (если есть), операций за день к промокодам
        if coupons_operation_df.empty:
            promo_stats_df = coupons_df.copy()
            promo_stats_df[['status', 'updated']] = None
            coupons_operation_list = list()
        else:
            coupons_df = coupons_df.astype({'coupon_id':'string'})
            coupons_operation_df = coupons_operation_df.astype({'coupon_id':'string'})

            # проверка на использование промокода без отмены (если по user_id последняя операция - использованно)
            coupons_operation_df = coupons_operation_df.merge(coupons_df[['coupon_id', 'active']], how = 'left', on = 'coupon_id')
            last_activations = coupons_operation_df[((coupons_operation_df.status == 'used') | (coupons_operation_df.status == 'aborted')) 
                                                    & 
                                                    (coupons_operation_df.active == True)]
            last_activations = last_activations.sort_values(by='updated').drop_duplicates(subset=['coupon_id', 'user_id'] , keep='last')
            last_activations = last_activations.loc[last_activations['status']=='used']

            # оставляем закрытые промокоды
            closed_promocode = coupons_operation_df.loc[coupons_operation_df['status']=='canceled']
            coupons_operation_df = pd.concat([closed_promocode, last_activations], ignore_index=True)

            # формируем df промокодов с их последней активностью
            promo_stats_df = coupons_df.merge(coupons_operation_df, how = 'left', on = 'coupon_id')
            coupons_operation_list = coupons_operation_df['coupon_id'].tolist()

        # 1.5.3. Оставляем информацию о созданных и использованных промокодах, информацию по спекам
        promo_stats_df['start'] = pd.to_datetime(promo_stats_df['start'], errors = 'coerce')
        promo_stats_df['end'] = pd.to_datetime(promo_stats_df['end'], errors = 'coerce')
        promo_stats_df['updated'] = pd.to_datetime(promo_stats_df['updated'], errors = 'coerce')
        promo_stats_df = promo_stats_df.loc[
                               ((promo_stats_df['start'] >= execution_date_start) & 
                                (promo_stats_df['start'] <= execution_date_end))       # забираем созданные сегодня промо 
                                |          
                               ((promo_stats_df['updated'] >= execution_date_start) & 
                                (promo_stats_df['updated'] <= execution_date_end))     # забираем использованные сегодня промо
                               ]    
        
        # 1.5.4. Приводим df к финальной форме
        print(promo_stats_df.info())
        promo_stats_df = promo_stats_df[['coupon_id','code','specification_id','status','updated', 'end', 'start']]
        promo_stats_df.rename(columns={'coupon_id':'promocode_id', "updated":"updated_date", 'end':'end_date', 'start':'start_date'}, inplace=True)
        print(promo_stats_df.info())

        # 2.Работа с GoogleBQ
        credentials = service_account.Credentials.from_service_account_info(kwargs["connection_id"])
        bq_client = bigquery.Client(project='schema', credentials=credentials)

        ### 2.1. Проверяем на повторный запуск по дате
        bq_query_delete = """
                        DELETE FROM `schema.airflow_test.promocode_stats_test`
                        WHERE 
                        (update_date BETWEEN @update_dt_start AND @update_dt_end) AND
                        (start_date BETWEEN @update_dt_start AND @update_dt_end)                                  
                        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("coupons_operation_list", "STRING", coupons_operation_list), 
                bigquery.ScalarQueryParameter("update_dt_start", "DATETIME", execution_date_start), 
                bigquery.ScalarQueryParameter("update_dt_end", "DATETIME", execution_date_end), 
            ]
        )
        bq_client.query(bq_query_delete, job_config = job_config)

        ### 2.2. Добавляем созданные/использованные промокоды
        dataset_ref = bq_client.dataset('airflow_test')
        table_ref = dataset_ref.table(f'promocode_stats_test')
        schema = [
                bigquery.SchemaField(
                    "promocode_id",
                    "STRING",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "code",
                    "STRING",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "specification_id",
                    "STRING",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "status",
                    "STRING",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "updated_date",
                    "DATE",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "end_date",
                    "DATE",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "start_date",
                    "DATE",
                    mode="NULLABLE"
                )
               ]
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = 'WRITE_APPEND'
        bq_client.load_table_from_dataframe(promo_stats_df[['promocode_id', 'code', 'specification_id', 'status','updated_date', 'end_date','start_date']], 
                                            table_ref, job_config=job_config).result()

        ### 2.3. Обновляем информацию о просроченных промокодах 
        job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("update_dt_start", "DATETIME", execution_date_start), 
                            bigquery.ScalarQueryParameter("update_dt_end", "DATETIME", execution_date_end), 
                        ],
                        use_legacy_sql = False
                    )
        bq_query_update = """
                        UPDATE `schema.airflow_test.promocode_stats_test`
                        SET is_used = 'expired'
                        WHERE (end_date BETWEEN @update_dt_start AND @update_dt_end)
                              AND is_used IS null                                     
                        """
        bq_client.query(bq_query_update, job_config = job_config )
          
        ### 2.4. Обновляем информацию по спекам
        dataset_ref = bq_client.dataset('airflow_test')
        table_ref = dataset_ref.table(f'specification_stats_test')
        schema = [
                bigquery.SchemaField(
                    "specification_id",
                    "STRING",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "specification_title",
                    "STRING",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "unique_promo_cnt",
                    "INTEGER",
                    mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "user_limit",
                    "INTEGER",
                    mode="NULLABLE"
                )
               ]
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = 'WRITE_TRUNCATE'
        bq_client.load_table_from_dataframe(spec_stats_df[['specification_id','specification_title', 'unique_promo_cnt', 'user_limit']], 
                                            table_ref, job_config=job_config).result()

        return True
    
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    df_to_bq_task = df_to_bq(**{
                                "connection_id": "{{ conn.airflow_gcp_connection.extra_dejson.keyfile_dict }}"
                               }
                            )
    
    chain (begin, 
           df_to_bq_task,
           end)

transfer_taskflow()