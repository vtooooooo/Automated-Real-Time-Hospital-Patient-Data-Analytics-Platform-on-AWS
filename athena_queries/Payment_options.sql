SELECT insurance_provider, payment_method, COUNT(*) AS count 
FROM hospital_hospital_patient_data_bucket 
GROUP BY insurance_provider, payment_method 
ORDER BY count DESC;