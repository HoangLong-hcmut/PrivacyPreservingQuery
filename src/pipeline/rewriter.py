import sqlglot
from sqlglot import exp

def normalize_query(sql: str) -> str:
    """
    Standardize query format (lowercasing, removing comments).
    """
    # sqlglot.transpile returns a list of strings, we take the first one
    return sqlglot.transpile(sql, read=None, write="mysql")[0]

def rewrite_for_count(sql: str) -> str:
    """
    Strips the user's SELECT columns and replaces them with SELECT COUNT(DISTINCT patient_id).
    """
    parsed = sqlglot.parse_one(sql)
    
    if not isinstance(parsed, exp.Select):
        raise ValueError("Query must be a SELECT statement.")

    # Create the new count expression: COUNT(DISTINCT patient_id)
    count_expr = exp.Count(this=exp.Distinct(expressions=[exp.Column(this=exp.Identifier(this="patient_id", quoted=False))]))

    # Replace existing expressions in the SELECT clause
    parsed.set("expressions", [count_expr])

    # We keep the rest (FROM, WHERE, GROUP BY, HAVING) as is.
    # If there is a GROUP BY, this will return the count for each group.
    # The Privacy Guard will need to iterate through results.
    
    return parsed.sql(dialect="mysql")
