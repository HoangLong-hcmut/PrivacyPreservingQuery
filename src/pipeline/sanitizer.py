import sqlglot
from sqlglot import exp

ALLOWED_TABLES = {'patients', 'diagnoses', 'doctors'}

class SecurityException(Exception):
    pass

def validate_query(sql: str) -> bool:
    """
    Parses the SQL using sqlglot.
    Validates against whitelist/blacklist and schema.
    """
    try:
        parsed_list = sqlglot.parse(sql)
    except Exception as e:
        raise SecurityException(f"Invalid SQL syntax: {e}")

    if len(parsed_list) > 1:
        raise SecurityException("Multiple statements are not allowed.")
        
    parsed = parsed_list[0]

    # Blacklist Enforcement
    # Check for modification statements
    forbidden_types = (
        exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter, exp.Create
    )
    if isinstance(parsed, forbidden_types):
        raise SecurityException("Modification queries are not allowed.")
    
    # Also traverse to ensure no subqueries do weird things, though parse_one usually gets the root.
    # Let's walk the tree to be safe against nested forbidden ops if sqlglot parses them that way.
    for node in parsed.walk():
        if isinstance(node, forbidden_types):
            raise SecurityException(f"Forbidden statement type detected: {type(node)}")

    # Whitelist Validation (Root level mostly, but we want to ensure it's a SELECT)
    if not isinstance(parsed, exp.Select):
        # It might be a Union, which is also okay usually, but strict requirement says "Only allow SELECT..."
        # If the root is not SELECT, we might want to block. 
        # However, complex SELECTs might be wrapped. 
        # For this strict requirement, let's enforce root is SELECT.
        raise SecurityException("Only SELECT queries are allowed.")

    # Schema Check
    for table in parsed.find_all(exp.Table):
        table_name = table.name
        if table_name not in ALLOWED_TABLES:
            raise SecurityException(f"Table '{table_name}' does not exist in the allowed schema.")

    return True
