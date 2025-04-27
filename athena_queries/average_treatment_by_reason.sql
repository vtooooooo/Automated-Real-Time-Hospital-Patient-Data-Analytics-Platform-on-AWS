SELECT reason, AVG(treatment_duration_minutes) AS avg_duration 
FROM hospital_hospital_patient_data_bucket 
GROUP BY reason 
ORDER BY avg_duration DESC;