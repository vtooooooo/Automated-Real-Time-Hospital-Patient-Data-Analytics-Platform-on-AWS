SELECT COUNT(*) AS today_patients 
FROM hospital_hospital_patient_data_bucket 
WHERE date(from_iso8601_timestamp(timestamp)) = current_date;