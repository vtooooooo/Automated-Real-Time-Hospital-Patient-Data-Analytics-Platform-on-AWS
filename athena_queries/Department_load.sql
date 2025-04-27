SELECT department, COUNT(*) AS patient_count 
FROM hospital_hospital_patient_data_bucket 
GROUP BY department 
ORDER BY patient_count DESC;