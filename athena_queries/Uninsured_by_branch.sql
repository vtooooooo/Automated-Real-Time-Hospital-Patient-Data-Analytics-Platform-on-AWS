SELECT hospital_branch, COUNT(*) AS uninsured 
FROM hospital_hospital_patient_data_bucket 
WHERE insurance_provider = 'No Insurance' 
GROUP BY hospital_branch 
ORDER BY uninsured DESC;