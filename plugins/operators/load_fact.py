from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id,
                 table,
                 query,
                #  truncate = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id
        self.table = table
        self.query = query
        # self.truncate = truncate

    def execute(self, context):
        # self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        # if self.truncate:
        #     redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info("Inserting data to fact table {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        self.log.info("Success: Inserting data to fact table {self.table}")

