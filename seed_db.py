import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_PORT = int(os.getenv("DB_PORT", 3306))

def seed_database():
    # Connect to MySQL server
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    
    try:
        with conn.cursor() as cursor:
            # Create Database
            cursor.execute("DROP DATABASE IF EXISTS hospital_db")
            cursor.execute("CREATE DATABASE hospital_db")
            cursor.execute("USE hospital_db")
            
            # Create Staffs Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staffs (
                    staff_id INT PRIMARY KEY,
                    role VARCHAR(50),
                    national_id CHAR(12) UNIQUE,
                    full_name VARCHAR(100),
                    dob DATE,
                    age INT GENERATED ALWAYS AS (TIMESTAMPDIFF(YEAR, dob, '2026-01-01')) VIRTUAL,
                    gender VARCHAR(10),
                    address VARCHAR(255),
                    specialization VARCHAR(100),
                    privacy_budget FLOAT DEFAULT 10.0
                )
            """)

            # Create Patients Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id INT PRIMARY KEY,
                    national_id CHAR(12) UNIQUE,
                    full_name VARCHAR(100),
                    dob DATE,
                    age INT GENERATED ALWAYS AS (TIMESTAMPDIFF(YEAR, dob, '2026-01-01')) VIRTUAL,
                    gender VARCHAR(10),
                    address VARCHAR(255)
                )
            """)
            
            # Create Diagnoses Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS diagnoses (
                    diagnosis_id INT PRIMARY KEY,
                    patient_id INT,
                    staff_id INT,
                    disease_name VARCHAR(100),
                    visit_date DATE,
                    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
                    FOREIGN KEY (staff_id) REFERENCES staffs(staff_id)
                )
            """)
            
            # Seed Staffs
            staffs_data = [
                (1, 'doctor', '001080000001', 'Nguyen Van Minh', '1980-01-15', 'M', '123 Le Loi, Hanoi', 'Cardiology', 50.0),
                (2, 'employee', '001082000002', 'Tran Thi Mai', '1982-05-20', 'F', '456 Nguyen Hue, HCM', 'Accountant', 5.0),
                (3, 'researcher', '001075000003', 'Le Van Hung', '1975-11-10', 'M', '789 Tran Hung Dao, Da Nang', 'Data Science', 20.0),
                (4, 'manager', '001088000004', 'Pham Thi Lan', '1988-03-25', 'F', '321 Ba Trieu, Hanoi', 'Management', 100.0),
                (5, 'doctor', '001085000005', 'Hoang Van Tien', '1985-09-09', 'M', '555 Giai Phong, Hanoi', 'Neurology', 10.0),
                (6, 'employee', '001090000006', 'Nguyen Thi Hoa', '1990-12-12', 'F', '888 Lang, Hanoi', 'Cashier', 5.0),
                (7, 'employee', '001092000007', 'Vu Van Nam', '1992-06-15', 'M', '222 Tay Son, Hanoi', 'Security', 0.0),
                (8, 'doctor', '001078000008', 'Do Lan Huong', '1978-04-30', 'F', '101 Kim Ma, Hanoi', 'Pediatrics', 10.0),
                (9, 'employee', '001086000009', 'Le Thi Thu', '1986-07-20', 'F', '999 Giang Vo, Hanoi', 'Accountant', 5.0)
            ]
            
            cursor.executemany("INSERT INTO staffs (staff_id, role, national_id, full_name, dob, gender, address, specialization, privacy_budget) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", staffs_data)
            
            # Seed Patients
            patients_data = []

            # Group 1: Seniors (Age 75) - 20 people
            # IDs 1-20. Born 1950.
            for i in range(1, 21):
                patients_data.append((
                    i, f"0011950000{i:02d}", f"Senior Patient {i}", "1950-06-01", 'M' if i%2==0 else 'F', "Nursing Home A"
                ))

            # Group 2: Adults (Age 45) - 20 people
            # IDs 21-40. Born 1980.
            for i in range(21, 41):
                patients_data.append((
                    i, f"0011980000{i:02d}", f"Adult Patient {i}", "1980-06-01", 'M' if i%2==0 else 'F', "Office Block B"
                ))

            # Group 3: Youth (Age 20) - 17 people
            # IDs 41-57. Born 2005.
            for i in range(41, 58):
                patients_data.append((
                    i, f"0012005000{i:02d}", f"Youth Patient {i}", "2005-06-01", 'M' if i%2==0 else 'F', "School C"
                ))

            # Group 4: Infant (Age 1) - Only 3 people
            # IDs 58-60. Born 2025.
            for i in range(58, 61):
                patients_data.append((
                   i, f"0012025000{i:02d}", f"Infant Patient {i}", "2025-01-01", 'M' if i%2==0 else 'F', "Nursery D"
                ))
            
            cursor.executemany("INSERT INTO patients (patient_id, national_id, full_name, dob, gender, address) VALUES (%s, %s, %s, %s, %s, %s)", patients_data)
            
            # Seed Diagnoses
            diagnoses_data = [
                (1, 1, 1, 'Hypertension', '2023-01-10'),
                (2, 2, 2, 'Migraine', '2023-01-11'),
                (3, 3, 3, 'Flu', '2023-01-12'),
                (4, 4, 4, 'Breast Cancer', '2023-01-13'),
                (5, 5, 5, 'Acne', '2023-01-14'),
                (6, 1, 1, 'Hypertension', '2023-02-10'),
                (7, 2, 2, 'Stroke', '2023-02-11'),
                (8, 3, 3, 'Common Cold', '2023-02-12'),
                (9, 4, 4, 'Lung Cancer', '2023-02-13'),
                (10, 5, 5, 'Eczema', '2023-02-14'),
                (11, 1, 2, 'Headache', '2023-03-01'),
                (12, 2, 1, 'Diabetes Type 2', '2023-03-02'),
                (13, 4, 3, 'Pneumonia', '2023-03-03'),
                (14, 3, 4, 'Fever', '2023-03-04'),
                (15, 1, 5, 'Arrhythmia', '2023-03-05')
            ]

            cursor.executemany("INSERT INTO diagnoses VALUES (%s, %s, %s, %s, %s)", diagnoses_data)
            
        conn.commit()
        print("Database 'hospital_db' seeded successfully.")
        
    except Exception as e:
        print(f"Error seeding database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    seed_database()
