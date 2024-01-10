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
                 deleteLoad = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insertTable = insertTable
        self.deleteLoad = deleteLoad

    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        table = self.table
        insertTable = self.insertTable
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Write table {}".format(table))
        if self.deleteLoad:
            sql = f"DELETE FROM {table}"
            redshift.run(sql)
            self.log.info("Clean the table: {}".format(table))
        sql = f"INSERT INTO {table} {insertTable}"
        self.log.info("SQL LoadDimensionOperator \n {}".format(sql))
        redshift.run(sql)
