SELECT hospital_branch, COUNT(*) AS total_patients, SUM(CASE WHEN critical_condition THEN 1 ELSE 0 END) AS critical_cases, ROUND(AVG(days_in_hospital), 2) AS avg_stay 
FROM hospital_hospital_patient_data_bucket 
GROUP BY hospital_branch 
ORDER BY total_patients DESC;