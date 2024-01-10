from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insertTable = "",
                 append_or_delte = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insertTable = insertTable
        self.append_or_delte = append_or_delte
 

    def execute(self, context):
        self.log.info("LoadDimensionOperator for: {}".format(self.table))
        table = self.table
        insertTable = self.insertTable
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Write table {}".format(table))
        if self.append_or_delte:  
            sql = f"TRUNCATE TABLE {table}"
            redshift.run(sql)
            self.log.info("Clean the table: {}".format(table))

        sql = f"INSERT INTO {table} {insertTable}"
        self.log.info("SQL: \n {}".format(sql))

        redshift.run(sql)
