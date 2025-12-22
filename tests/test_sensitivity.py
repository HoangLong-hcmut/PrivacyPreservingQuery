import pytest
import numpy as np
from datetime import date
from src.pipeline.dp_engine import calculate_sensitivity

# Mock Data
MOCK_PATIENTS = [
    {
        'patient_id': i, 
        'national_id': f"{i:012d}",
        'full_name': f"Patient {i}",
        'dob': date(1980, 1, 1), # Dummy DOB
        'gender': 'M' if i % 2 == 0 else 'F',
        'address': '123 Nguyen Hue'
    } 
    for i in range(1, 1001)
]

MOCK_DOCTORS = [
    {
        'doctor_id': i,
        'national_id': f"DOC{i:09d}",
        'full_name': f"Doctor {i}",
        'dob': date(1975, 1, 1),
        'gender': 'F' if i % 2 == 0 else 'M',
        'address': '456 Le Loi',
        'specialty': 'General'
    }
    for i in range(1, 6)
]

MOCK_DIAGNOSES = [
    {
        'diagnosis_id': i,
        'patient_id': (i % 1000) + 1,
        'doctor_id': (i % 5) + 1,
        'disease_name': 'Flu',
        'diagnosis_date': date(2023, 1, 1)
    }
    for i in range(1, 2001)
]

class MockDatabase:
    def __init__(self):
        self.patients = MOCK_PATIENTS.copy()
        self.doctors = MOCK_DOCTORS.copy()
        self.diagnoses = MOCK_DIAGNOSES.copy()

    def execute_query(self, sql, params=None):
        # Simple SQL parser for the mock
        sql = sql.lower()
        if "count(distinct patient_id)" in sql:
            # Cohort size check
            # Filter based on WHERE clause (very basic parsing)
            filtered = self._filter_data(sql)
            return [{'COUNT(DISTINCT patient_id)': len(filtered)}]
        elif "count(*)" in sql:
            filtered = self._filter_data(sql)
            return [{'COUNT(*)': len(filtered)}]
        elif "sum" in sql:
            # Assume SUM(age) - wait, we don't have age anymore.
            # If query is SUM(something), we need to handle it.
            # But user queries might change to use DOB or something else.
            # Let's assume we might count based on DOB year?
            # Or maybe we just return a dummy sum if asked.
            filtered = self._filter_data(sql)
            return [{'SUM(dummy)': 100}] # Placeholder
        return []

    def _filter_data(self, sql):
        data = self.patients
        # Normalize spaces for simple matching
        sql_norm = sql.replace(" ", "")
        if "where" in sql:
            # Very basic WHERE clause handling for the specific tests we expect
            # e.g. "dob < '1975-01-01'", "dob='1924-01-01' and address='12345'"
            if "dob<" in sql_norm:
                # Mock filtering: return half
                return data[:len(data)//2]
            if "dob=" in sql_norm and "address=" in sql_norm:
                # Singling out mock
                return [p for p in data if p['patient_id'] == 9999] # Special case
            if "dob=" in sql_norm: 
                 return [p for p in data if p['patient_id'] == 9999]
        return data

@pytest.fixture
def mock_db():
    return MockDatabase()

def test_verify_sensitivity_bounds(mock_db, metrics_recorder):
    """
    Verifies that the empirical sensitivity of a COUNT query does not exceed the theoretical sensitivity.
    """
    query = "SELECT COUNT(*) FROM patients WHERE dob < '1975-01-01'"
    
    # Theoretical Sensitivity for COUNT is 1
    theoretical_sensitivity = calculate_sensitivity("COUNT")
    
    max_empirical_sensitivity = 0
    n_iterations = 1000
    
    original_data = mock_db.patients.copy()
    
    for _ in range(n_iterations):
        # 1. Run on D
        mock_db.patients = original_data.copy()
        res_d = mock_db.execute_query(query)[0]['COUNT(*)']
        
        # 2. Create Neighbor D' (remove one random record)
        if not original_data:
            break
        
        remove_idx = np.random.randint(0, len(original_data))
        neighbor_data = original_data.copy()
        removed_record = neighbor_data.pop(remove_idx)
        mock_db.patients = neighbor_data
        
        # 3. Run on D'
        res_d_prime = mock_db.execute_query(query)[0]['COUNT(*)']
        
        # 4. Calculate Empirical Sensitivity
        sensitivity = abs(res_d - res_d_prime)
        if sensitivity > max_empirical_sensitivity:
            max_empirical_sensitivity = sensitivity
            
    # Restore DB
    mock_db.patients = original_data
    
    # Record metrics
    metrics_recorder['max_observed_sensitivity'] = float(max_empirical_sensitivity)
    
    print(f"\nMax Empirical Sensitivity: {max_empirical_sensitivity}")
    print(f"Theoretical Sensitivity: {theoretical_sensitivity}")
    
    assert max_empirical_sensitivity <= theoretical_sensitivity, \
        f"Privacy Violation: Empirical sensitivity {max_empirical_sensitivity} > Theoretical {theoretical_sensitivity}"
