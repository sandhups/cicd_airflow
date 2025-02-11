from pendulum import datetime
from airflow.decorators import dag, task

@dag(
    dag_id='simple_etl_pipeline',
    description='A simple ETL pipeline using TaskFlow API',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    tags=['example', 'etl'],
    catchup=False
)
def simple_etl_pipeline():
    """
    ### Simple ETL Pipeline
    This is a simple ETL pipeline that demonstrates the use of TaskFlow API
    and follows Airflow best practices.
    """

    @task()
    def extract():
        """
        #### Extract task
        Gets data from a source system.
        """
        data = {"order_value": 1000, "tax_rate": 0.1}
        return data

    @task()
    def transform(input_data: dict):
        """
        #### Transform task
        Calculates tax amount based on order value.
        """
        order_value = input_data["order_value"]
        tax_rate = input_data["tax_rate"]
        tax_amount = order_value * tax_rate
        return {"order_value": order_value, "tax_amount": tax_amount}

    @task()
    def load(transformed_data: dict):
        """
        #### Load task
        Logs the results (in a real scenario, this would write to a database).
        """
        print(f"Order value: {transformed_data['order_value']}")
        print(f"Tax amount: {transformed_data['tax_amount']}")
        return transformed_data
    
    @task()
    def get_my_free_certification_code():
        import os
        import hashlib

        node_selector = str(os.getenv('ASTRONOMER_NODE_SELECTOR', 'you-must-run-this-dag-on-astro'))
        if node_selector == 'you-must-run-this-dag-on-astro':
            print(f"Get your free certification https://academy.astronomer.io/astronomer-certified-apache-airflow-core-exam?pc={node_selector}")
            return
        print(node_selector)

        hash_object = hashlib.sha256(node_selector.encode())
        hex_hash = hash_object.hexdigest()
        
        positions = [15, 8, 31, 12, 22, 5, 18, 9, 27, 3, 14, 7]
        charset = "0123456789abcdefghijklmnopqrstuvwxyz"
        
        result = ""
        for pos in positions:
            index = int(hex_hash[pos], 16) % len(charset)
            result += charset[index]

        print(f"Get your free certification https://academy.astronomer.io/astronomer-certified-apache-airflow-core-exam?pc={result}")

    # Define the task dependencies
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data) >> get_my_free_certification_code()

simple_etl_pipeline()
