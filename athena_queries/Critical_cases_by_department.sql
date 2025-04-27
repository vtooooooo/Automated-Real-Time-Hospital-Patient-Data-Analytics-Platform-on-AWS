SELECT department, COUNT(*) AS critical_cases 
FROM hospital_hospital_patient_data_bucket 
WHERE critical_condition = true 
GROUP BY department 
ORDER BY critical_cases DESC;