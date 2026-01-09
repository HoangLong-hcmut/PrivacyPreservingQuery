import sqlglot
from sqlglot import exp

ALLOWED_TABLES = {'patients', 'diagnoses', 'staff'}
BLOCKED_COLUMNS = {'national_id', 'full_name', 'address', 'name', 'ssn'}
ALLOWED_OPERATORS = {exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.And, exp.Or, exp.Paren}
ALLOWED_AGGREGATES = {exp.Count, exp.Sum}

class SecurityException(Exception):
    pass

def validate_query(sql: str) -> bool:
    """
    Parses and validates the SQL against the defined schema allowlist and blocklist.
    """
    try:
        parsed_list = sqlglot.parse(sql)
    except Exception as e:
        raise SecurityException(f"Invalid SQL syntax: {e}")

    if len(parsed_list) > 1:
        raise SecurityException("Multiple statements are not allowed.")
        
    parsed = parsed_list[0]

    # Validate against restricted elements
    # Check for modification statements
    forbidden_types = (
        exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter, exp.Create
    )
    if isinstance(parsed, forbidden_types):
        raise SecurityException("Modification queries are not allowed.")
    
    # Also traverse to ensure no subqueries do weird things
    for node in parsed.walk():
        if isinstance(node, forbidden_types):
            raise SecurityException(f"Forbidden statement type detected: {type(node)}")
            
        # Attribute Level Check
        if isinstance(node, exp.Column):
            if node.name.lower() in BLOCKED_COLUMNS:
                raise SecurityException(f"Access to sensitive identifier '{node.name}' is prohibited.")
                
        # Predicate Level Check (in WHERE)
        # We need to check if we are inside a WHERE clause.
        # This is a bit complex with pure walk, let's just check nodes generally 
        # or recursively check WHERE expression if it exists.
        
    # Whitelist Validation
    if not isinstance(parsed, exp.Select):
        raise SecurityException("Only SELECT queries are allowed.")

    # Schema Check
    for table in parsed.find_all(exp.Table):
        table_name = table.name
        if table_name not in ALLOWED_TABLES:
            raise SecurityException(f"Table '{table_name}' does not exist in the allowed schema.")

    # Validate WHERE clause specifically for operators
    if parsed.args.get("where"):
        where_node = parsed.args.get("where")
        for node in where_node.walk():
            # Skip the specific values, columns, and the Where node itself
            if isinstance(node, (exp.Column, exp.Literal, exp.Where, exp.Identifier)):
                if isinstance(node, exp.Column) and node.name.lower() in BLOCKED_COLUMNS:
                     raise SecurityException(f"Access to sensitive identifier '{node.name}' in WHERE is prohibited.")
                continue
                
            # If it's an operator, check if allowed
            if type(node) not in ALLOWED_OPERATORS:
                # We also need to allow basic comparisons which might be represented differently
                # But sqlglot uses classes like EQ, GT...
                # Check if it's a function?
                if isinstance(node, exp.Func):
                     raise SecurityException(f"Functions are not allowed in WHERE clause: {type(node)}")
                     
                # Fail on complex logic or unknown ops
                # Note: This is very strict. It might block standard things.
                # Adjusting based on requirement "Predicate Level: ... allow only specific safe operators ... Block complex logic or functions"
                raise SecurityException(f"Operator or Logic '{type(node).__name__}' is not allowed in WHERE clause.")

    return True
