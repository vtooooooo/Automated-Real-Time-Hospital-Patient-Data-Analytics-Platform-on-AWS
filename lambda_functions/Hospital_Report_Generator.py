import boto3
import time
import datetime

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

# === CONFIGURATION ===
ATHENA_DATABASE = 'hospital_data_db'
ATHENA_OUTPUT = 's3://hospital-patient-data-bucket/athena-query-results/'
FINAL_REPORTS_BUCKET = 'hospital-patient-data-bucket'
FINAL_REPORTS_PREFIX = 'hospital-reports/'

# List of (report_name, sql_query)
report_queries = [
    ("top_visit_reasons", "SELECT reason, COUNT(*) AS visit_count FROM hospital_hospital_patient_data_bucket GROUP BY reason ORDER BY visit_count DESC LIMIT 5;"),
    ("department_load", "SELECT department, COUNT(*) AS patient_count FROM hospital_hospital_patient_data_bucket GROUP BY department ORDER BY patient_count DESC;"),
    ("most_common_meds", "SELECT medication FROM hospital_hospital_patient_data_bucket CROSS JOIN UNNEST(medications_administered) AS t(medication) GROUP BY medication ORDER BY COUNT(*) DESC LIMIT 10;"),
    ("bed_type_usage", "SELECT bed_type, COUNT(*) AS count FROM hospital_hospital_patient_data_bucket GROUP BY bed_type;"),
    ("avg_treatment_by_reason", "SELECT reason, AVG(treatment_duration_minutes) AS avg_duration FROM hospital_hospital_patient_data_bucket GROUP BY reason ORDER BY avg_duration DESC;"),
    ("today_patient_count", "SELECT COUNT(*) AS today_patients FROM hospital_hospital_patient_data_bucket WHERE date(from_iso8601_timestamp(timestamp)) = current_date;"),
    ("readmission_rate", "SELECT COUNT(*) AS total, SUM(CASE WHEN readmission_within_30_days THEN 1 ELSE 0 END) AS readmitted, ROUND(100.0 * SUM(CASE WHEN readmission_within_30_days THEN 1 ELSE 0 END) / COUNT(*), 2) AS readmission_rate_percent FROM hospital_hospital_patient_data_bucket;"),
    ("payment_vs_insurance", "SELECT insurance_provider, payment_method, COUNT(*) AS count FROM hospital_hospital_patient_data_bucket GROUP BY insurance_provider, payment_method ORDER BY count DESC;"),
    ("critical_cases_by_department", "SELECT department, COUNT(*) AS critical_cases FROM hospital_hospital_patient_data_bucket WHERE critical_condition = true GROUP BY department ORDER BY critical_cases DESC;"),
    ("low_oxygen_alerts", "SELECT * FROM hospital_hospital_patient_data_bucket WHERE oxygen_level < 92 ORDER BY oxygen_level ASC;"),
    ("doctor_critical_load", "SELECT doctor_name, COUNT(*) AS critical_patients FROM hospital_hospital_patient_data_bucket WHERE critical_condition = true GROUP BY doctor_name ORDER BY critical_patients DESC;"),
    ("branch_comparison", "SELECT hospital_branch, COUNT(*) AS total_patients, SUM(CASE WHEN critical_condition THEN 1 ELSE 0 END) AS critical_cases, ROUND(AVG(days_in_hospital), 2) AS avg_stay FROM hospital_hospital_patient_data_bucket GROUP BY hospital_branch ORDER BY total_patients DESC;"),
    ("longest_stay_patients", "SELECT name, reason, department, days_in_hospital FROM hospital_hospital_patient_data_bucket ORDER BY days_in_hospital DESC LIMIT 10;"),
    ("emergency_outcomes", "SELECT is_emergency, COUNT(*) AS total, AVG(days_in_hospital) AS avg_days, SUM(CASE WHEN readmission_within_30_days THEN 1 ELSE 0 END) AS readmitted FROM hospital_hospital_patient_data_bucket GROUP BY is_emergency;"),
    ("covid_positivity", "SELECT COUNT(*) AS total_tested, SUM(CASE WHEN covid_test_result = 'Positive' THEN 1 ELSE 0 END) AS positives, ROUND(100.0 * SUM(CASE WHEN covid_test_result = 'Positive' THEN 1 ELSE 0 END) / COUNT(*), 2) AS positivity_rate FROM hospital_hospital_patient_data_bucket;"),
    ("uninsured_by_branch", "SELECT hospital_branch, COUNT(*) AS uninsured FROM hospital_hospital_patient_data_bucket WHERE insurance_provider = 'No Insurance' GROUP BY hospital_branch ORDER BY uninsured DESC;"),
    ("bed_stay", "SELECT bed_type, COUNT(*) AS total_patients, ROUND(AVG(days_in_hospital), 2) AS avg_stay FROM hospital_hospital_patient_data_bucket GROUP BY bed_type;")
]

def lambda_handler(event, context):
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    for report_name, query in report_queries:
        print(f"Running query for {report_name}")

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )

        execution_id = response['QueryExecutionId']

        # Wait for the query to finish
        state = 'RUNNING'
        while state in ['RUNNING', 'QUEUED']:
            time.sleep(3)
            state = athena_client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            result_path = f"athena-query-results/{execution_id}.csv"
            new_report_name = f"{FINAL_REPORTS_PREFIX}{report_name}_{today}.csv"
            print(f"Copying {result_path} to {new_report_name}")
            
            # Copy the Athena result to reports folder
            s3_client.copy_object(
                Bucket=FINAL_REPORTS_BUCKET,
                CopySource={ 'Bucket': FINAL_REPORTS_BUCKET, 'Key': result_path },
                Key=new_report_name
            )

        else:
            print(f"Query for {report_name} FAILED with state {state}")

    return {'statusCode': 200, 'body': '✅ All reports generated and saved to S3.'}