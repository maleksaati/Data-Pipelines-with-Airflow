from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 field = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.field = field
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        records = redshift_hook.get_records(f"SELECT COUNT({self.field}) FROM {self.table};")
        
        self.log.info(f"Number of Records: " + str(records))

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed.No data found in table {self.table}")
        
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed.Table {self.table} contains 0 rows")
        
        self.log.info(f"Data quality check successed on table  {self.table}, found {records[0][0]} records")