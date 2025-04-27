SELECT bed_type, COUNT(*) AS total_patients, ROUND(AVG(days_in_hospital), 2) AS avg_stay 
FROM hospital_hospital_patient_data_bucket 
GROUP BY bed_type;