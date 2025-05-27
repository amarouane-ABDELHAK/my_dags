from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import logging

# Define default arguments
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG without schedule (manual trigger only)
with DAG(
    'example_task_decorator_dag',
    default_args=default_args,
    description='Example DAG using @task decorator',
    schedule=None,  # No automatic schedule - manual trigger only
    catchup=False,
    tags=['example', 'task_decorator'],
    ) as dag:

    @task()
    def extract_data():
        """Extract data from source"""
        logging.info("Starting data extraction...")
        # Simulate data extraction
        data = {
            'users': [
                {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
                {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'},
                {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com'}
            ]
        }
        logging.info(f"Extracted {len(data['users'])} user records")
        return data

    @task()
    def transform_data(raw_data):
        """Transform the extracted data"""
        logging.info("Starting data transformation...")
        
        transformed_users = []
        for user in raw_data['users']:
            transformed_user = {
                'user_id': user['id'],
                'full_name': user['name'].upper(),
                'email_domain': user['email'].split('@')[1],
                'processed_at': datetime.now().isoformat()
            }
            transformed_users.append(transformed_user)
        
        result = {'transformed_users': transformed_users}
        logging.info(f"Transformed {len(transformed_users)} user records")
        return result

    @task()
    def validate_data(transformed_data):
        """Validate the transformed data"""
        logging.info("Starting data validation...")
        
        users = transformed_data['transformed_users']
        
        # Validation checks
        validation_results = {
            'total_records': len(users),
            'valid_records': 0,
            'invalid_records': 0,
            'validation_errors': []
        }
        
        for user in users:
            is_valid = True
            
            # Check if required fields exist
            if not user.get('user_id') or not user.get('full_name'):
                validation_results['validation_errors'].append(
                    f"Missing required fields for user: {user}"
                )
                is_valid = False
            
            # Check email domain
            if not user.get('email_domain'):
                validation_results['validation_errors'].append(
                    f"Missing email domain for user: {user['user_id']}"
                )
                is_valid = False
            
            if is_valid:
                validation_results['valid_records'] += 1
            else:
                validation_results['invalid_records'] += 1
        
        logging.info(f"Validation complete: {validation_results['valid_records']} valid, "
                    f"{validation_results['invalid_records']} invalid records")
        
        return validation_results

    @task()
    def load_data(transformed_data, validation_results):
        """Load the validated data"""
        logging.info("Starting data loading...")
        
        if validation_results['invalid_records'] > 0:
            logging.warning(f"Found {validation_results['invalid_records']} invalid records")
            for error in validation_results['validation_errors']:
                logging.warning(f"Validation error: {error}")
        
        # Simulate loading to database/data warehouse
        users_to_load = transformed_data['transformed_users']
        
        loading_summary = {
            'records_processed': len(users_to_load),
            'load_timestamp': datetime.now().isoformat(),
            'status': 'completed'
        }
        
        logging.info(f"Successfully loaded {len(users_to_load)} records")
        return loading_summary

    @task()
    def send_notification(loading_summary, validation_results):
        """Send completion notification"""
        logging.info("Sending completion notification...")
        
        message = f"""
        ETL Pipeline Completed Successfully!
        
        Summary:
        - Records processed: {loading_summary['records_processed']}
        - Valid records: {validation_results['valid_records']}
        - Invalid records: {validation_results['invalid_records']}
        - Completion time: {loading_summary['load_timestamp']}
        - Status: {loading_summary['status']}
        """
        
        logging.info(message)
        # In real scenario, you might send email, Slack message, etc.
        
        return "Notification sent successfully"


    raw_data_res = extract_data()
    transformed_data_res = transform_data(raw_data_res)
    validate_data_res = validate_data(transformed_data_res)
    loading_summary = load_data(transformed_data_res, validate_data_res)
    send_notification(loading_summary)
