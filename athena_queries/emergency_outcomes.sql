SELECT is_emergency, COUNT(*) AS total, AVG(days_in_hospital) AS avg_days, SUM(CASE WHEN readmission_within_30_days THEN 1 ELSE 0 END) AS readmitted 
FROM hospital_hospital_patient_data_bucket 
GROUP BY is_emergency;