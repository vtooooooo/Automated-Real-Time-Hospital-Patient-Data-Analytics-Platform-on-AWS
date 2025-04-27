import json
import boto3
import random
import datetime

# Firehose Setup
FIREHOSE_NAME = 'hospital-patient-firehose'
firehose_client = boto3.client('firehose')

# Reference data
departments = ['ER', 'Radiology', 'Surgery', 'ICU', 'Recovery', 'Discharge']
genders = ['Male', 'Female']
first_names = ['John', 'Jane', 'Amit', 'Sara', 'Wei', 'Ali', 'Maria', 'Liam', 'Noah', 'Emma', 'Olivia', 'Aarav', 'Vivaan', 'Zara']
last_names = ['Smith', 'Johnson', 'Patel', 'Ali', 'Chen', 'Garcia', 'Khan', 'Lee', 'Kumar', 'Williams', 'Brown', 'Singh', 'Sharma', 'Thomas']
insurance_companies = ['Aetna', 'Cigna', 'Blue Cross', 'UnitedHealth', 'Humana', 'No Insurance']
hospital_branches = ['Downtown Hospital', 'Uptown Hospital', 'Eastside Clinic']
payment_methods = ['Insurance', 'Cash', 'Credit Card']

# Reason mappings with stay_range and discharge_notes
reason_mapping = {
    "Chest Pain": {
        "symptoms": ["Chest tightness", "Shortness of breath"],
        "tests": ["ECG", "Blood Test", "Chest X-ray"],
        "medications": ["Aspirin", "Nitroglycerin"],
        "critical": True,
        "stay_range": (5, 8),
        "discharge_notes": ["Patient stabilized", "Prescribed heart medication", "Scheduled follow-up ECG"]
    },
    "High Fever": {
        "symptoms": ["Chills", "Sweating", "Body pain"],
        "tests": ["Blood Test", "COVID Test"],
        "medications": ["Paracetamol", "Antibiotics"],
        "critical": False,
        "stay_range": (2, 4),
        "discharge_notes": ["Temperature normalized", "Antibiotics prescribed", "Advised hydration"]
    },
    "Injury": {
        "symptoms": ["Bleeding", "Swelling", "Pain"],
        "tests": ["X-Ray", "CT Scan"],
        "medications": ["Painkillers", "Antibiotics"],
        "critical": False,
        "stay_range": (2, 5),
        "discharge_notes": ["Wound dressed", "Follow-up for stitches", "Referred to orthopedics"]
    },
    "Routine Checkup": {
        "symptoms": ["No symptoms"],
        "tests": ["Blood Test", "ECG"],
        "medications": ["Multivitamins"],
        "critical": False,
        "stay_range": (0, 1),
        "discharge_notes": ["No issues found", "Routine blood work done"]
    },
    "Abdominal Pain": {
        "symptoms": ["Nausea", "Vomiting", "Stomach cramps"],
        "tests": ["Ultrasound", "Blood Test"],
        "medications": ["Antacids", "Painkillers"],
        "critical": False,
        "stay_range": (3, 6),
        "discharge_notes": ["Pain managed with medication", "Recommended endoscopy"]
    },
    "Headache": {
        "symptoms": ["Migraine", "Light sensitivity", "Nausea"],
        "tests": ["MRI", "Blood Pressure Check"],
        "medications": ["Painkillers", "Anti-migraine drugs"],
        "critical": False,
        "stay_range": (1, 2),
        "discharge_notes": ["Migraine treatment prescribed", "Neurologist referral advised"]
    },
    "Shortness of Breath": {
        "symptoms": ["Difficulty breathing", "Wheezing"],
        "tests": ["Chest X-ray", "Pulmonary Function Test"],
        "medications": ["Inhalers", "Steroids"],
        "critical": True,
        "stay_range": (5, 7),
        "discharge_notes": ["Respiration stabilized", "Prescribed inhalers"]
    },
    "Back Pain": {
        "symptoms": ["Stiffness", "Sharp pain"],
        "tests": ["MRI", "X-ray"],
        "medications": ["Painkillers", "Physiotherapy"],
        "critical": False,
        "stay_range": (1, 3),
        "discharge_notes": ["Pain management therapy recommended", "Referred to physiotherapy"]
    },
    "Broken Bone": {
        "symptoms": ["Swelling", "Inability to move limb"],
        "tests": ["X-Ray"],
        "medications": ["Cast Immobilization", "Painkillers"],
        "critical": False,
        "stay_range": (4, 6),
        "discharge_notes": ["Cast applied", "Scheduled follow-up X-Ray"]
    },
    "Diabetes Checkup": {
        "symptoms": ["Increased thirst", "Frequent urination"],
        "tests": ["HbA1c Test", "Blood Sugar Test"],
        "medications": ["Insulin", "Metformin"],
        "critical": False,
        "stay_range": (1, 2),
        "discharge_notes": ["Blood sugar under control", "Adjusted insulin dosage"]
    },
    "High Blood Pressure": {
        "symptoms": ["Headache", "Dizziness"],
        "tests": ["Blood Pressure Monitoring"],
        "medications": ["Antihypertensives"],
        "critical": False,
        "stay_range": (2, 3),
        "discharge_notes": ["BP medication adjusted", "Advised lifestyle changes"]
    },
    "Pregnancy Checkup": {
        "symptoms": ["Morning sickness", "Abdominal discomfort"],
        "tests": ["Ultrasound", "Blood Test"],
        "medications": ["Prenatal Vitamins"],
        "critical": False,
        "stay_range": (1, 3),
        "discharge_notes": ["Healthy fetal heartbeat", "Scheduled next prenatal check"]
    },
    "Skin Rash": {
        "symptoms": ["Itching", "Redness", "Swelling"],
        "tests": ["Allergy Test", "Skin Biopsy"],
        "medications": ["Antihistamines", "Corticosteroids"],
        "critical": False,
        "stay_range": (1, 2),
        "discharge_notes": ["Allergy medications prescribed", "Skin biopsy done"]
    },
    "Kidney Stone": {
        "symptoms": ["Flank pain", "Blood in urine"],
        "tests": ["CT Scan", "Urinalysis"],
        "medications": ["Painkillers", "Alpha blockers"],
        "critical": False,
        "stay_range": (3, 5),
        "discharge_notes": ["Pain under control", "Surgery scheduled if needed"]
    },
    "Asthma Attack": {
        "symptoms": ["Wheezing", "Tight chest"],
        "tests": ["Pulmonary Function Test"],
        "medications": ["Inhalers", "Steroids"],
        "critical": True,
        "stay_range": (4, 6),
        "discharge_notes": ["Breathing stabilized", "Steroid treatment given"]
    },
    "Stroke Symptoms": {
        "symptoms": ["Facial drooping", "Arm weakness"],
        "tests": ["CT Scan", "MRI"],
        "medications": ["Blood thinners"],
        "critical": True,
        "stay_range": (6, 10),
        "discharge_notes": ["Neuro team follow-up", "CT/MRI completed"]
    },
    "COVID Symptoms": {
        "symptoms": ["Cough", "Fever", "Shortness of breath"],
        "tests": ["COVID Test", "Chest X-ray"],
        "medications": ["Antivirals", "Oxygen therapy"],
        "critical": True,
        "stay_range": (5, 9),
        "discharge_notes": ["Isolation advised", "Oxygen therapy given"]
    },
    "Allergic Reaction": {
        "symptoms": ["Hives", "Swelling", "Difficulty breathing"],
        "tests": ["Allergy Test"],
        "medications": ["Antihistamines", "Epinephrine"],
        "critical": True,
        "stay_range": (2, 4),
        "discharge_notes": ["Anaphylaxis reversed", "Allergy testing advised"]
    },
    "Food Poisoning": {
        "symptoms": ["Vomiting", "Diarrhea"],
        "tests": ["Stool Test", "Blood Test"],
        "medications": ["Rehydration therapy", "Antibiotics"],
        "critical": False,
        "stay_range": (2, 3),
        "discharge_notes": ["Dehydration treated", "Antibiotics course started"]
    },
    "Ear Infection": {
        "symptoms": ["Ear pain", "Hearing loss"],
        "tests": ["Ear Examination"],
        "medications": ["Antibiotics", "Painkillers"],
        "critical": False,
        "stay_range": (1, 2),
        "discharge_notes": ["Antibiotics prescribed", "Scheduled hearing test"]
    }
}

def random_name():
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def random_blood_pressure():
    systolic = random.randint(90, 140)
    diastolic = random.randint(60, 90)
    return f"{systolic}/{diastolic}"

def lambda_handler(event, context):
    for _ in range(500):
        reason = random.choice(list(reason_mapping.keys()))
        reason_data = reason_mapping[reason]
        critical = reason_data["critical"]
        arrival_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=random.randint(0, 240))

        # Insurance & payment method logic
        insurance = random.choice(insurance_companies)
        payment_method = random.choice(['Cash', 'Credit Card']) if insurance == "No Insurance" else random.choice(payment_methods)

        days_in_hospital = random.randint(*reason_data["stay_range"])
        treatment_duration = random.randint(90, 300) if critical else random.randint(15, 180)

        patient_event = {
            'patient_id': random.randint(1000, 9999),
            'name': random_name(),
            'age': random.randint(1, 90),
            'gender': random.choice(genders),
            'event': random.choice(['admitted', 'transferred', 'discharged']),
            'reason': reason,
            'symptoms': random.sample(reason_data['symptoms'], k=min(len(reason_data['symptoms']), random.randint(1, 3))),
            'department': random.choice(departments),
            'is_emergency': random.choice([True, False]),
            'critical_condition': critical,
            'blood_pressure': random_blood_pressure(),
            'oxygen_level': random.randint(85, 100),
            'insurance_provider': insurance,
            'payment_method': payment_method,
            'arrival_time': arrival_time.isoformat(),
            'treatment_duration_minutes': treatment_duration,
            'discharge_notes': random.choice(reason_data['discharge_notes']),
            'doctor_id': random.randint(100, 999),
            'doctor_name': f"Dr. {random.choice(first_names)} {random.choice(last_names)}",
            'room_number': f"{random.choice(['ER', 'ICU', 'OPD'])}-{random.randint(1,30)}",
            'triage_level': random.randint(1,5),
            'medications_administered': random.sample(reason_data['medications'], k=min(len(reason_data['medications']), random.randint(1, 2))),
            'tests_conducted': random.sample(reason_data['tests'], k=min(len(reason_data['tests']), random.randint(0, 2))),
            'hospital_branch': random.choice(hospital_branches),
            'bed_type': random.choice(['General', 'Semi-Private', 'Private', 'ICU']),
            'days_in_hospital': days_in_hospital,
            'readmission_within_30_days': random.choice([True, False]),
            'covid_test_result': random.choice(['Positive', 'Negative']),
            'timestamp': datetime.datetime.utcnow().isoformat()
        }

        # 🔥 Push to Firehose
        firehose_client.put_record(
            DeliveryStreamName=FIREHOSE_NAME,
            Record={'Data': json.dumps(patient_event) + '\n'}
        )

    return {
        'statusCode': 200,
        'body': json.dumps('✅ 10 detailed patient events sent to Firehose.')
    }
