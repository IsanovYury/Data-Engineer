import pendulum

from airflow.models.baseoperator import chain
from airflow.decorators import dag, task, task_group

from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


default_args = {
    'owner': 'fyri',
    'provide_context': True,
    'start_date': pendulum.parse('2023-07-25', tz='Asia/Almaty'),
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': True,
    'email': ['isanov1712@gmail.com'],
    'retries': 0,
    'retry_delay': 300.0
}

@dag(
    dag_id="bq_abc_feed",
    default_args=default_args,
    schedule='00 8 * * *',
    tags=["GBQ", "S3", "PG", "Athena"],
    start_date = pendulum.parse('2023-07-24', tz='Asia/Almaty'),
    catchup = False,
    render_template_as_native_obj=True,
    jinja_environment_kwargs={
        "keep_trailing_newline": True,
        # some other jinja2 Environment options here
    }
) 

def second_func():
    '''
    #### Формирует feed с запросом из AWS в GBQ
    1. Формирует df: из S3 берет стоки, по запросам из Athena - sku, названия магазинов,
    из pg - abc_etalon
    2. Отправляет df в GoogleBigQuery

    '''
    @task()
    def df_to_bq(**kwargs):
        import boto3
        from google.oauth2 import service_account
        from google.cloud import bigquery
        import io
        import pandas as pd
        from datetime import timedelta
        import numpy as np
        
        date = kwargs["data_interval_start"].strftime('%Y-%m-%d')
        table_date = (str(date)).replace('-','')

        # Формируем df со стоками
        data = boto3.resource(
            service_name='s3',
            region_name=kwargs["aws_region"],
            aws_access_key_id=kwargs["aws_login"],
            aws_secret_access_key=kwargs["aws_password"],
            verify = False
        )
        filepath = 'AthenaQuery/'
        filename = f'{kwargs["query_execution_id"]}.csv'
        request = data.Bucket('athena-qcloudy').Object(filepath + filename).get()
        df_stock = pd.read_csv(
            request['Body'], 
            dtype={
                'items_id':str, 
                'quant': float, 
                'price': float, 
                'store':str,
                'city': str
                }
                )
        df_stock.columns = ['items_id','quant','price','shop_name','city']

        # Формируем df c SKU
        data = boto3.resource(
            service_name='s3',
            region_name=kwargs["aws_region"],
            aws_access_key_id=kwargs["aws_login"],
            aws_secret_access_key=kwargs["aws_password"],
            verify = False
        )
        filepath = 'AthenaQuery/'
        filename = f'{kwargs["query_execution_id_2"]}.csv'
        request = data.Bucket('athena-qcloudy').Object(filepath + filename).get()
        df_sku = pd.read_csv(
            request['Body'], 
            dtype={
                'category_short': str,
                'category_name': str,
                'item_name': str, 
                'items_id': str,
                'webcode': str
                }
                )
        df_sku.columns = ['category_short', 'item_name','items_id', 'webcode', 'category_name']

        # Формируем df c Sales
        data = boto3.resource(
            service_name='s3',
            region_name=kwargs["aws_region"],
            aws_access_key_id=kwargs["aws_login"],
            aws_secret_access_key=kwargs["aws_password"],
            verify = False
        )
        filepath = 'AthenaQuery/'
        filename = f'{kwargs["query_execution_id_3"]}.csv'
        request = data.Bucket('athena-qcloudy').Object(filepath + filename).get()
        df_sales = pd.read_csv(
            request['Body'], 
            dtype={
                'webcode': str, 
                'item_name': str, 
                'shop_name': str, 
                'quantity': float, 
                'item_total': float, 
                'city': str
                }
                )
        df_sales.columns = ['webcode', 'item_name', 'shop_name', 'quantity', 'item_total',  'city']
        
        # Merge SKU товаров к стокам (в т.ч. категорий родительских)   
        df_stock = df_stock.merge(df_sku, on = 'items_id')
        df_stock = df_stock[['webcode', 'item_name', 'price', 'quant', 'city', 'shop_name', 'category_short', 'category_name']]
        df_stock = df_stock.groupby(['webcode', 'item_name', 'city', 'shop_name', 'category_short', 'category_name']).sum().reset_index()

        # Merge стоков, продаж 
        df_total_table = df_stock.merge(df_sales, on = ['webcode', 'item_name', 'shop_name', 'city'], how = 'left')

        # Заменяем NaN на 0
        df_total_table = df_total_table.fillna(0)

        # Фильтр по категориям
        list_of_cat = ['Сертификат гарантии', 'Прочие услуги', 'Сертификат подарочный',  'Гарантийный талон']
        
        df_total_table=df_total_table[~df_total_table['category_name'].isin(list_of_cat)]
        # Из pg берем abc_etalon 
        pg_engine = PostgresHook(postgres_conn_id='pg').get_sqlalchemy_engine()
        with pg_engine.begin() as conn:
            df_abc_etalon = pd.read_sql(f"""
                SELECT 
                    cast(cast("SKU_ID" as int) as varchar) as webcode,
                    "TOTAL_SALES_QUANTITY" as total_sales_quantity,
                    "TOTAL_SALES_VALUE" as total_sales_value,
                    "XYZ_RANK" as xyz_rank,
                    "ABC_RANK" as abc_rank,
                    "CATEGORY_SHORT" as category_short, 
                    "class",
                    class_size,
                    category_sizes,
                    class_category_sizes
                 FROM abc_etalon ae
                 WHERE "timestamp" = (
                    SELECT 
                        max("timestamp") 
                    FROM abc_etalon ae2
                    WHERE
                        "timestamp" <= {int((kwargs["data_interval_start"] + timedelta(hours=6) + timedelta(days=1)).timestamp())})
                       """, con = conn)
        pg_engine.dispose()

        #Merge общую таблицу и etalon
        df_total_table['webcode'] = df_total_table['webcode'].astype(str)
        df_abc_etalon['webcode'] = df_abc_etalon['webcode'].astype(str)
        df_total_table = df_total_table.merge(df_abc_etalon, on = 'webcode', how = 'left')
        df_total_table['class'] = df_total_table['class'].fillna('x')

        # Добавляем столбец с execution_date dag
        df_total_table["date"] = date

        df_total_table = df_total_table[['date', 'webcode', 'item_name', 'category_short_x', 'category_name', 'city', 'shop_name', 'quant',
                                         'quantity', 'item_total', 'total_sales_quantity', 'total_sales_value', 'xyz_rank', 'abc_rank',
                                         'category_short_y', 'class', 'class_size', 'category_sizes', 'class_category_sizes']]
        
        df_total_table.columns = ["date", "sku_id", "sku_name", "category_short_x", "category_name", "city_name",
                                  "shop_name", "stock_value", "sales_quantity", "sales_value", "total_sales_quantity",       
                                  "total_sales_value", "xyz_rank","abc_rank", "category_short_y", "class", 
                                  "class_size", "category_sizes", "class_category_sizes"]
        
        df_total_table = df_total_table.astype({
                                  "date":'datetime64[ns]',      "sku_id": str ,         "sku_name": str,
                                  "category_short_x":str,       "category_name": str,   "city_name":str,
                                  "shop_name": str,             "stock_value":float,    "class_size": float, 
                                  "sales_quantity": float,      "sales_value": float,   "total_sales_quantity": float,
                                  "total_sales_value": float,   "xyz_rank":str,         "abc_rank":str, 
                                  "category_short_y":str,       "class":str,            
                                  "category_sizes": float,      "class_category_sizes": float
                                  })
        # Приводим типы данных
        df_total_table = df_total_table.replace({'None': None})
        df_total_table = df_total_table.replace({np.nan: None})

        # Обновление данных в bq

        credentials = service_account.Credentials.from_service_account_info(kwargs["connection_id"])
        bigquery_client = bigquery.Client(project='bigquery', credentials=credentials)
        dataset_ref = bigquery_client.dataset('schema')
        table_ref = dataset_ref.table(f'abc_by_shop_daily_test_{table_date}')
        schema = [
            bigquery.SchemaField(
                "date",
                "TIMESTAMP",
                mode="NULLABLE",
                description="Date of data insertion in BQ",
            ),
            bigquery.SchemaField(
                "sku_id",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "sku_name",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "category_short_x",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "category_name",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "city_name",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "shop_name",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "stock_value",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "sales_quantity",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "sales_value",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "total_sales_quantity",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "total_sales_value",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "xyz_rank",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "abc_rank",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "category_short_y",
                "STRING",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "class",
                "string",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "class_size",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "category_sizes",
                "FLOAT",
                mode="NULLABLE",
                description="",
            ),
            bigquery.SchemaField(
                "class_category_sizes",
                "FLOAT",
                mode="NULLABLE",
                description="",
            )
            ]
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.write_disposition = 'WRITE_TRUNCATE'
        bigquery_client.load_table_from_dataframe(df_total_table, table_ref, job_config=job_config).result()
        print(df_total_table.tail(10))
        
        return df_total_table.tail(10)
    
    @task_group()
    def get_query():
        from datetime import timedelta
        
        retry_arg = {'retries': 3,
                    'retry_delay': timedelta(seconds=10)}
        run_query_stock = AthenaOperator(
                            task_id = 'run_query_stock',
                            query = 'SQL/bq_abc_feed/query_stock_from_athena.sql',
                            output_location='s3://athena-qcloudy/AthenaQuery/',
                            database='l3_prod_dhl',
                            do_xcom_push=True,
                            aws_conn_id='airflow_aws',
                            default_args = retry_arg
                            )
        run_query_sku = AthenaOperator(
                            task_id = 'run_query_sku',
                            query = 'SQL/bq_abc_feed/query_sku.sql',
                            output_location='s3://athena-qcloudy/AthenaQuery/',
                            database='l3_prod_dlh',
                            do_xcom_push=True,
                            aws_conn_id='airflow_aws',
                            default_args = retry_arg
                                )
        run_query_sales = AthenaOperator(
                            task_id = 'run_query_sales',
                            query =  "SQL/bq_abc_feed/query_sales_from_check.sql",
                            output_location='s3://athena-qcloudy/AthenaQuery/',
                            database='l3_prod_dlh',
                            do_xcom_push=True,
                            aws_conn_id='airflow_aws',
                            default_args = retry_arg
                            )

        return run_query_stock, run_query_sku, run_query_sales
    
    begin = EmptyOperator(task_id="begin")
    get_query_task = get_query()
    end = EmptyOperator(task_id="end")

    df_to_bq_task = df_to_bq(**{
                            "aws_login": "{{ conn.airflow_aws.login }}",
                            "aws_password": "{{ conn.airflow_aws.password }}",
                            "aws_region": "{{ conn.airflow_aws.extra_dejson.region_name }}",
                            "connection_id": "{{ conn.airflow_gcp_connection.extra_dejson.keyfile_dict }}",
                            "query_execution_id": get_query_task[0].output,
                            "query_execution_id_2": get_query_task[1].output,
                            "query_execution_id_3": get_query_task[2].output
                            })

    chain (begin, 
           get_query_task,
           df_to_bq_task,
           end)

second_func()