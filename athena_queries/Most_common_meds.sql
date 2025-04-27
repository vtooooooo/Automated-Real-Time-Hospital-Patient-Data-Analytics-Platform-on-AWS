SELECT medication FROM hospital_hospital_patient_data_bucket 
CROSS JOIN UNNEST(medications_administered) AS t(medication) 
GROUP BY medication 
ORDER BY COUNT(*) 
DESC LIMIT 10;