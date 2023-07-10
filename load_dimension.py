from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.append_only = append_only

    def execute(self, context):
        self.log.info("Starting LoadDimensionOperator")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info(f"Clearing data in Redshift table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info(f"Loading dimension table {self.table} in Redshift")
        insert_sql = f"""
        {self.sql_stmt}
        """
        redshift.run(insert_sql)
        self.log.info("LoadDimensionOperator completed successfully")