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
    # Connect to MySQL server (no db selected yet)
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
            
            # Create Doctors Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS doctors (
                    doctor_id INT PRIMARY KEY,
                    national_id CHAR(12) UNIQUE,
                    full_name VARCHAR(100),
                    dob DATE,
                    gender VARCHAR(10),
                    address VARCHAR(255),
                    specialization VARCHAR(100)
                )
            """)

            # Create Patients Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id INT PRIMARY KEY,
                    national_id CHAR(12) UNIQUE,
                    full_name VARCHAR(100),
                    dob DATE,
                    gender VARCHAR(10),
                    address VARCHAR(255)
                )
            """)
            
            # Create Diagnoses Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS diagnoses (
                    diagnosis_id INT PRIMARY KEY,
                    patient_id INT,
                    doctor_id INT,
                    disease_name VARCHAR(100),
                    visit_date DATE,
                    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
                    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
                )
            """)
            
            # Seed Doctors
            doctors_data = [
                (1, '001080000001', 'Dr. Nguyen Van A', '1980-01-15', 'M', '123 Le Loi, Hanoi', 'Cardiology'),
                (2, '001082000002', 'Dr. Tran Thi B', '1982-05-20', 'F', '456 Nguyen Hue, HCM', 'Neurology'),
                (3, '001075000003', 'Dr. Le Van C', '1975-11-10', 'M', '789 Tran Hung Dao, Da Nang', 'Pediatrics'),
                (4, '001088000004', 'Dr. Pham Thi D', '1988-03-25', 'F', '321 Ba Trieu, Hanoi', 'Oncology'),
                (5, '001090000005', 'Dr. Hoang Van E', '1990-09-05', 'M', '654 Le Duan, HCM', 'Dermatology')
            ]
            
            cursor.executemany("INSERT INTO doctors VALUES (%s, %s, %s, %s, %s, %s, %s)", doctors_data)
            
            # Seed Patients
            patients_data = [
                (1, '001200000001', 'Nguyen Van An', '2000-01-01', 'M', '101 Pho Hue, Hanoi'),
                (2, '001201000002', 'Tran Thi Binh', '2001-02-02', 'F', '202 Hai Ba Trung, HCM'),
                (3, '001199000003', 'Le Van Cuong', '1999-03-03', 'M', '303 Nguyen Trai, Hanoi'),
                (4, '001198000004', 'Pham Thi Dung', '1998-04-04', 'F', '404 Le Lai, HCM'),
                (5, '001195000005', 'Hoang Van Em', '1995-05-05', 'M', '505 Tran Phu, Da Nang'),
                (6, '001190000006', 'Vu Thi F', '1990-06-06', 'F', '606 Nguyen Van Linh, Da Nang'),
                (7, '001185000007', 'Dang Van G', '1985-07-07', 'M', '707 Dien Bien Phu, HCM'),
                (8, '001180000008', 'Bui Thi H', '1980-08-08', 'F', '808 Xa Dan, Hanoi'),
                (9, '001175000009', 'Do Van I', '1975-09-09', 'M', '909 Lang Ha, Hanoi'),
                (10, '001170000010', 'Ngo Thi K', '1970-10-10', 'F', '010 Nguyen Chi Thanh, Hanoi')
            ]
            
            # Generate extra patients to satisfy cohort size checks (need > 10 for some queries)
            # We need patients born before 1975 for the test query "dob < '1975-01-01'"
            for i in range(11, 60):
                # Alternating gender
                gender = 'M' if i % 2 == 0 else 'F'
                # Year: 1950 + (i % 40) -> 1950 to 1990. 
                # Many will be < 1975.
                year = 1950 + (i % 40)
                dob = f"{year}-01-01"
                patients_data.append((
                    i, 
                    f"001{year}000{i:02d}", 
                    f"Patient {i}", 
                    dob, 
                    gender, 
                    f"{i} Random St"
                ))
            
            cursor.executemany("INSERT INTO patients VALUES (%s, %s, %s, %s, %s, %s)", patients_data)
            
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
