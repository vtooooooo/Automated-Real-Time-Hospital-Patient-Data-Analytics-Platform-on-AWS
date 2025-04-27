SELECT doctor_name, COUNT(*) AS critical_patients 
FROM hospital_hospital_patient_data_bucket 
WHERE critical_condition = true 
GROUP BY doctor_name 
ORDER BY critical_patients DESC;