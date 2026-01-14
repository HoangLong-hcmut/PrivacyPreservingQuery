import sqlglot
from sqlglot import exp

ALLOWED_TABLES = {'patients', 'diagnoses', 'staff'} # Known system schema
ALLOWED_OPERATORS = {exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.And, exp.Or, exp.Paren}
ALLOWED_AGGREGATES = {exp.Count, exp.Sum, exp.Min, exp.Max, exp.Avg}

# Role-Based Policies
ROLE_POLICIES = {
    "doctor": {
        "allowed_tables": {"patients", "diagnoses"},
        "blocked_columns": {"patient_id"},
        "allow_where": True
    },
    "researcher": {
        "allowed_tables": {"patients", "diagnoses", "staff"},
        "blocked_columns": {"full_name", "address", "national_id", "patient_id", "staff_id", "privacy_budget"},
        "allow_where": True 
    },
    "manager": {
        "allowed_tables": {"patients", "diagnoses", "staff"},
        "blocked_columns": set(),
        "allow_where": True 
    },
    "accountant": {
        "allowed_tables": {"staff"},
        "blocked_columns": {"staff_id", "specialization", "privacy_budget"},
        "allow_where": True 
    },
    "cashier": {
        "allowed_tables": {"diagnoses"},
        "blocked_columns": {"diagnosis_id", "staff_id", "patient_id"},
        "allow_where": True
    },
    "default": { # Fallback / Guest
        "allowed_tables": set(),
        "blocked_columns": set(),
        "allow_where": False
    }
}

class SecurityException(Exception):
    pass

def validate_query(sql: str, user_role: str = "default") -> bool:
    """
    Parses and validates the SQL against the defined schema allowlist and blocklist.
    """
    policy = ROLE_POLICIES.get(user_role.lower(), ROLE_POLICIES["default"])
    
    try:
        parsed_list = sqlglot.parse(sql)
    except Exception as e:
        raise SecurityException(f"Invalid SQL syntax: {e}")

    if len(parsed_list) > 1:
        raise SecurityException("Multiple statements are not allowed.")
        
    parsed = parsed_list[0]

    # Validate against restricted elements
    forbidden_types = (
        exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter, exp.Create
    )
    if isinstance(parsed, forbidden_types):
        raise SecurityException("Modification queries are not allowed.")
    
    # Also traverse to ensure no subqueries do weird things
    for node in parsed.walk():
        if isinstance(node, forbidden_types):
            raise SecurityException(f"Forbidden statement type detected: {type(node)}")

        # Block Joins
        if isinstance(node, exp.Join):
            raise SecurityException("JOIN operations are not allowed due to sensitivity risks.")
            
        # Attribute Level Check (Policy Based)
        if isinstance(node, exp.Column):
            col_name = node.name.lower()
            if col_name in policy["blocked_columns"]:
                raise SecurityException(f"Access to sensitive identifier '{node.name}' is prohibited for role '{user_role}'.")
        
    # Whitelist Validation
    if not isinstance(parsed, exp.Select):
        raise SecurityException("Only SELECT queries are allowed.")

    # Schema Check (Role Based)
    for table in parsed.find_all(exp.Table):
        table_name = table.name.lower() # Normalize table name
        if table_name not in ALLOWED_TABLES:
             raise SecurityException(f"Table '{table_name}' does not exist in the system.")
        
        if table_name not in policy["allowed_tables"]:
            raise SecurityException(f"Access to table '{table_name}' is denied for role '{user_role}'.")

    # Validate WHERE clause specifically for operators
    if parsed.args.get("where"):
        if not policy["allow_where"]:
             raise SecurityException(f"WHERE clauses are not allowed for role '{user_role}'.")
             
        # Tautology Detection (Anti-SQL Injection)
        where_clause = parsed.args.get("where")
        for node in where_clause.walk():
            # Check for Comparisons between literals
            if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
                # Check directly if they are literal values
                if isinstance(node.this, (exp.Literal, exp.Boolean)) and isinstance(node.expression, (exp.Literal, exp.Boolean)):
                    raise SecurityException(f"SQL Injection detected: Literal comparison '{node.sql()}' is forbidden.")

        where_node = parsed.args.get("where")
        for node in where_node.walk():
            # Skip the specific values, columns, and the Where node itself
            if isinstance(node, (exp.Literal, exp.Where, exp.Identifier)):
                continue
            
            if isinstance(node, exp.Column):
                col_name = node.name.lower()
                if col_name in policy["blocked_columns"]:
                     raise SecurityException(f"Access to sensitive identifier '{node.name}' in WHERE is prohibited.")
                continue
                
            # If it's an operator, check if allowed
            if type(node) not in ALLOWED_OPERATORS:
                if isinstance(node, exp.Func):
                    raise SecurityException(f"Functions are not allowed in WHERE clause: {type(node)}")
                
                raise SecurityException(f"Operator or Logic '{type(node).__name__}' is not allowed in WHERE clause.")

    return True
