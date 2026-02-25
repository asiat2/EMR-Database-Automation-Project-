EMR-DATA-SCRIPT

======================================================================
SECTION 11 – GENERATING SYNTHETIC DATA
import pyodbc
from faker import Faker
import random
from datetime import timedelta

fake = Faker()

# ======================================
# DATABASE CONNECTION
# ======================================

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=EMRDatabase;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()
cursor.fast_executemany = True

print("✅ Connected to SQL Server")

# ======================================
# CLEAN DATABASE (SAFE ORDER)
# ======================================

print("🧹 Deleting previous data...")

tables_delete_order = [
    "Audit_Log",
    "Prescriptions",
    "Lab_Results",
    "Lab_Orders",
    "Vital_Signs",
    "Billing",
    "Admissions",
    "Appointments",
    "Allergies",
    "Visits",
    "Insurance",
    "Providers",
    "Departments",
    "Patients"
]

for table in tables_delete_order:
    try:
        cursor.execute(f"DELETE FROM {table}")
    except:
        pass

conn.commit()

# Reset identity columns
for table in tables_delete_order:
    try:
        cursor.execute(f"DBCC CHECKIDENT ({table}, RESEED, 0)")
    except:
        pass

conn.commit()

print("✅ Previous data deleted")

# ======================================
# INSERT DEPARTMENTS
# ======================================

departments = [
    ("Cardiology", "Floor 2", "615-200-1111"),
    ("Emergency", "Ground Floor", "615-200-2222"),
    ("Pediatrics", "Floor 3", "615-200-3333"),
    ("Orthopedics", "Floor 4", "615-200-4444"),
    ("Neurology", "Floor 5", "615-200-5555")
]

cursor.executemany("""
INSERT INTO Departments (department_name, location, phone)
VALUES (?, ?, ?)
""", departments)

conn.commit()

cursor.execute("SELECT department_id FROM Departments")
department_ids = [row[0] for row in cursor.fetchall()]

print("🏥 Departments inserted")

# ======================================
# INSERT PROVIDERS (FIXED PHONE LENGTH)
# ======================================

providers = []

for _ in range(50):
    providers.append((
        fake.first_name()[:100],
        fake.last_name()[:100],
        random.choice([
            "Cardiologist",
            "ER Physician",
            "Pediatrician",
            "Orthopedic Surgeon",
            "Neurologist"
        ])[:150],
        random.choice(department_ids),
        fake.numerify(text="##########"),  # 10-digit phone (SAFE)
        fake.email()[:150],
        fake.uuid4()[:100]
    ))

cursor.executemany("""
INSERT INTO Providers
(first_name,last_name,specialty,department_id,phone,email,license_number)
VALUES (?, ?, ?, ?, ?, ?, ?)
""", providers)

conn.commit()

cursor.execute("SELECT provider_id FROM Providers")
provider_ids = [row[0] for row in cursor.fetchall()]

print("👨‍⚕️ Providers inserted")

# ======================================
# INSERT 10,000 PATIENTS (SAFE PHONE)
# ======================================

patients = []

for _ in range(10000):
    patients.append((
        fake.uuid4(),
        fake.first_name()[:100],
        fake.last_name()[:100],
        fake.date_of_birth(minimum_age=0, maximum_age=90),
        random.choice(["Male","Female"]),
        random.choice(["Single","Married","Divorced"]),
        random.choice(["A+","A-","B+","B-","O+","O-","AB+","AB-"]),
        fake.numerify(text="##########"),  # SAFE phone
        fake.email()[:150],
        fake.address().replace("\n", ", ")[:400],
        fake.city()[:100],
        fake.state()[:100],
        fake.zipcode()[:20],
        fake.name()[:150],
        fake.numerify(text="##########")  # SAFE emergency phone
    ))

cursor.executemany("""
INSERT INTO Patients
(medical_record_number,first_name,last_name,date_of_birth,gender,
marital_status,blood_type,phone,email,address,city,state,zip_code,
emergency_contact_name,emergency_contact_phone)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", patients)

conn.commit()

cursor.execute("SELECT patient_id FROM Patients")
patient_ids = [row[0] for row in cursor.fetchall()]

print("🧑‍🤝‍🧑 10,000 Patients inserted")

# ======================================
# INSERT VISITS
# ======================================

visits = []

for _ in range(20000):
    visits.append((
        random.choice(patient_ids),
        random.choice(provider_ids),
        random.choice(department_ids),
        fake.date_time_between(start_date='-2y', end_date='now'),
        random.choice(["Inpatient","Outpatient","Emergency"]),
        fake.sentence()[:500],
        fake.sentence()[:500],
        fake.text()[:1000]
    ))

cursor.executemany("""
INSERT INTO Visits
(patient_id,provider_id,department_id,visit_date,
visit_type,chief_complaint,diagnosis,notes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", visits)

conn.commit()

cursor.execute("SELECT visit_id FROM Visits")
visit_ids = [row[0] for row in cursor.fetchall()]

print("🏥 Visits inserted")

# ======================================
# INSERT BILLING
# ======================================

billing = []

for visit_id in visit_ids:
    total = round(random.uniform(200,5000),2)
    insurance = round(total * random.uniform(0.5,0.9),2)
    billing.append((
        random.choice(patient_ids),
        visit_id,
        total,
        insurance,
        total - insurance,
        random.choice(["Paid","Pending","Denied"])
    ))

cursor.executemany("""
INSERT INTO Billing
(patient_id,visit_id,total_amount,
insurance_coverage,patient_balance,payment_status)
VALUES (?, ?, ?, ?, ?, ?)
""", billing)

conn.commit()

print("💰 Billing inserted")

# ======================================
# CLOSE CONNECTION
# ======================================

cursor.close()
conn.close()

print("🎉 FULL EMR DATABASE LOADED SUCCESSFULLY!")




#Remaining data

import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# ======================================
# DATABASE CONNECTION
# ======================================

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=ASHIAT-DELL\\SQL2019;"
    "DATABASE=EMRDatabase;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()
cursor.fast_executemany = True

print("Connected to SQL Server")

# ======================================
# CLEAN TARGET TABLES
# ======================================

tables = [
    "Audit_Log",
    "Lab_Results",
    "Lab_Orders",
    "Prescriptions",
    "Vital_Signs",
    "Allergies",
    "Admissions",
    "Appointments",
    "Insurance",
    "Medications",
    "Date"
]

for table in tables:
    try:
        cursor.execute(f"DELETE FROM {table}")
        cursor.execute(f"DBCC CHECKIDENT ({table}, RESEED, 0)")
    except:
        pass

conn.commit()

print("Old data cleared")

# ======================================
# LOAD DATE DIMENSION (5 YEARS)
# ======================================

start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 12, 31)

dates = []
current = start_date

while current <= end_date:
    dates.append((
        int(current.strftime("%Y%m%d")),
        current.date(),
        current.year,
        (current.month - 1)//3 + 1,
        current.month,
        current.day
    ))
    current += timedelta(days=1)

cursor.executemany("""
INSERT INTO Date (date_key, full_date, year, quarter, month, day)
VALUES (?, ?, ?, ?, ?, ?)
""", dates)

conn.commit()
print("Date dimension loaded")

# ======================================
# FETCH EXISTING IDS
# ======================================

cursor.execute("SELECT patient_id FROM Patients")
patient_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT provider_id FROM Providers")
provider_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT visit_id FROM Visits")
visit_ids = [row[0] for row in cursor.fetchall()]

# ======================================
# APPOINTMENTS
# ======================================

appointments = []

for _ in range(15000):
    appointments.append((
        random.choice(patient_ids),
        random.choice(provider_ids),
        fake.date_time_between(start_date='-1y', end_date='+3m'),
        fake.sentence()[:500],
        random.choice(["Scheduled","Completed","Cancelled"])
    ))

cursor.executemany("""
INSERT INTO Appointments
(patient_id,provider_id,appointment_date,reason,status)
VALUES (?, ?, ?, ?, ?)
""", appointments)

conn.commit()
print("Appointments loaded")

# ======================================
# MEDICATIONS
# ======================================

medications = [
    ("Amoxicillin","Tablet","500mg"),
    ("Ibuprofen","Tablet","200mg"),
    ("Metformin","Tablet","500mg"),
    ("Lisinopril","Tablet","10mg"),
    ("Azithromycin","Tablet","250mg"),
    ("Atorvastatin","Tablet","20mg"),
    ("Omeprazole","Capsule","40mg")
]

cursor.executemany("""
INSERT INTO Medications (medication_name,dosage_form,strength)
VALUES (?, ?, ?)
""", medications)

conn.commit()

cursor.execute("SELECT medication_id FROM Medications")
medication_ids = [row[0] for row in cursor.fetchall()]

print("Medications loaded")

# ======================================
# PRESCRIPTIONS
# ======================================

prescriptions = []

for _ in range(20000):
    prescriptions.append((
        random.choice(visit_ids),
        random.choice(medication_ids),
        "1 tablet",
        "Twice daily",
        "7 days",
        fake.sentence()[:500]
    ))

cursor.executemany("""
INSERT INTO Prescriptions
(visit_id,medication_id,dosage,frequency,duration,instructions)
VALUES (?, ?, ?, ?, ?, ?)
""", prescriptions)

conn.commit()
print("Prescriptions loaded")

# ======================================
# VITAL SIGNS
# ======================================

vitals = []

for _ in range(20000):
    vitals.append((
        random.choice(visit_ids),
        round(random.uniform(97,103),2),
        f"{random.randint(100,140)}/{random.randint(60,90)}",
        random.randint(60,100),
        random.randint(12,20),
        round(random.uniform(95,100),2),
        round(random.uniform(50,120),2),
        round(random.uniform(150,190),2)
    ))

cursor.executemany("""
INSERT INTO Vital_Signs
(visit_id,temperature,blood_pressure,heart_rate,
respiratory_rate,oxygen_saturation,weight,height)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", vitals)

conn.commit()
print("Vital signs loaded")

# ======================================
# ALLERGIES
# ======================================

allergens = ["Peanuts","Penicillin","Latex","Shellfish","Pollen"]

allergies = []

for _ in range(5000):
    allergies.append((
        random.choice(patient_ids),
        random.choice(allergens),
        fake.sentence()[:300],
        random.choice(["Mild","Moderate","Severe"])
    ))

cursor.executemany("""
INSERT INTO Allergies
(patient_id,allergen,reaction,severity)
VALUES (?, ?, ?, ?)
""", allergies)

conn.commit()
print("Allergies loaded")

# ======================================
# LAB ORDERS & RESULTS
# ======================================

lab_orders = []

for _ in range(15000):
    lab_orders.append((
        random.choice(visit_ids),
        random.choice(provider_ids),
        random.choice(["Ordered","Completed","Cancelled"])
    ))

cursor.executemany("""
INSERT INTO Lab_Orders (visit_id,ordered_by,status)
VALUES (?, ?, ?)
""", lab_orders)

conn.commit()

cursor.execute("SELECT lab_order_id FROM Lab_Orders")
lab_order_ids = [row[0] for row in cursor.fetchall()]

lab_results = []

for order_id in lab_order_ids:
    lab_results.append((
        order_id,
        random.choice(["CBC","BMP","Lipid Panel","A1C","TSH"]),
        str(round(random.uniform(1,10),2)),
        "Normal Range",
        random.choice(["Normal","Abnormal"])
    ))

cursor.executemany("""
INSERT INTO Lab_Results
(lab_order_id,test_name,result_value,reference_range,result_status)
VALUES (?, ?, ?, ?, ?)
""", lab_results)

conn.commit()
print("Lab data loaded")

# ======================================
# ADMISSIONS
# ======================================

admissions = []

for _ in range(3000):
    admit = fake.date_time_between(start_date='-2y', end_date='-1d')
    discharge = admit + timedelta(days=random.randint(1,10))
    admissions.append((
        random.choice(patient_ids),
        admit,
        discharge,
        str(random.randint(100,500)),
        random.choice(provider_ids),
        fake.sentence()[:500],
        fake.text()[:1000]
    ))

cursor.executemany("""
INSERT INTO Admissions
(patient_id,admission_date,discharge_date,
room_number,attending_provider,
admission_reason,discharge_summary)
VALUES (?, ?, ?, ?, ?, ?, ?)
""", admissions)

conn.commit()
print("Admissions loaded")

# ======================================
# INSURANCE
# ======================================

insurance = []

for _ in range(8000):
    start = fake.date_between(start_date='-3y', end_date='today')
    end = start + timedelta(days=365)
    insurance.append((
        random.choice(patient_ids),
        random.choice(["BlueCross","Aetna","UnitedHealth","Cigna"]),
        fake.uuid4()[:50],
        fake.uuid4()[:50],
        start,
        end
    ))

cursor.executemany("""
INSERT INTO Insurance
(patient_id,provider_name,policy_number,group_number,coverage_start,coverage_end)
VALUES (?, ?, ?, ?, ?, ?)
""", insurance)

conn.commit()
print("Insurance loaded")

# ======================================
# AUDIT LOG
# ======================================

audit_logs = []

for _ in range(5000):
    audit_logs.append((
        random.choice(["admin","nurse","doctor","system"]),
        random.choice(["INSERT","UPDATE","DELETE","SELECT"]),
        random.choice(["Patients","Visits","Billing","Admissions"]),
        random.randint(1,10000)
    ))

cursor.executemany("""
INSERT INTO Audit_Log
(user_name,action_type,table_name,record_id)
VALUES (?, ?, ?, ?)
""", audit_logs)

conn.commit()
print("Audit log loaded")

cursor.close()
conn.close()

print("🎉 ALL REMAINING TABLES LOADED SUCCESSFULLY!")
