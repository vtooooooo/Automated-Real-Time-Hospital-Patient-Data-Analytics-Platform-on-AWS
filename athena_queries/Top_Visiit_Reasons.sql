SELECT reason, COUNT(*) AS visit_count 
FROM hospital_hospital_patient_data_bucket 
GROUP BY reason 
ORDER BY visit_count 
DESC LIMIT 5;