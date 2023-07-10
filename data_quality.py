from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for test_case in self.test_cases:
            sql_stmt = test_case.get('check_sql')
            exp_result = test_case.get('expected_result')
            
            self.log.info(f"Running test case: {sql_stmt}")
            records = redshift.get_records(sql_stmt)
            
            if exp_result != records[0][0]:
                raise ValueError(f"Data quality check failed. {sql_stmt} \
                            returned {records[0][0]} but expected {exp_result}")
            
            self.log.info(f"Data quality on SQL {sql_stmt} check passed")