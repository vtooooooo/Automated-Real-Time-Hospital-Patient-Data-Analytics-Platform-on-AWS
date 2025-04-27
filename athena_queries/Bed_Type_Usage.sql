SELECT bed_type, COUNT(*) AS count FROM hospital_hospital_patient_data_bucket 
GROUP BY bed_type;