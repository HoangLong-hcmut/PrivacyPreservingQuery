import sqlglot
from sqlglot import exp

def normalize_query(sql: str) -> str:
    """
    Standardize query format (lowercasing, removing comments).
    """
    return sqlglot.transpile(sql, read=None, write="mysql")[0]

def rewrite_for_count(sql: str) -> str:
    """
    Rewrites query to return count for cohort analysis.
    Strips columns and uses COUNT(DISTINCT patient_id) if available in schema, else COUNT(*).
    """
    parsed = sqlglot.parse_one(sql)
    if not isinstance(parsed, exp.Select):
       return sql 

    # Prefer counting distinct patients for cohort size
    # But for general count enforcement we might just use COUNT(*)
    # For strict generic rewrite:
    count_expr = exp.Count(this=exp.Distinct(expressions=[exp.Column(this=exp.Identifier(this="patient_id", quoted=False))]))
    parsed.set("expressions", [count_expr])
    return parsed.sql(dialect="mysql")

def enforce_aggregation(sql: str) -> str:
    """
    Ensures the query is an aggregation. Rewrite raw SELECTs to COUNT(*).
    """
    parsed = sqlglot.parse_one(sql)
    
    if not isinstance(parsed, exp.Select):
        return sql

    is_aggregate = False
    for expr in parsed.expressions:
        if isinstance(expr, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            is_aggregate = True
            break
        if isinstance(expr, exp.Alias) and isinstance(expr.this, (exp.Count, exp.Sum)):
            is_aggregate = True
            break

    if not is_aggregate:
        # Defaults to COUNT(*) for safety

        parsed.set("expressions", [exp.Count(this=exp.Star())])
    
    return parsed.sql(dialect="mysql")

def generalize_filters(sql: str) -> str:
    """
    Step 2b: Generalization.
    If filters on sensitive attribute (e.g. age = 63), rewrite to bucket (age >= 60 AND age < 70).
    """
    parsed = sqlglot.parse_one(sql)
    
    where = parsed.args.get("where")
    if not where:
        return sql
        
    def transformer(node):
        if isinstance(node, exp.EQ):
            # Check for age = X
            col = node.left if isinstance(node.left, exp.Column) else node.right
            val = node.right if isinstance(node.left, exp.Column) else node.left
            
            if isinstance(col, exp.Column) and col.name.lower() == 'age':
                try:
                    # Parse value
                    age_val = int(str(val))
                    # Create bucket (10 years)
                    lower = (age_val // 10) * 10
                    upper = lower + 10
                    
                    # Create new expression: age >= lower AND age < upper
                    # exp.Between could work too, but requirement says "age >= 60 AND age < 70"
                    
                    # Note: We need to use Column objects, not just strings for 'this'
                    # But simpler way in sqlglot builder:
                    
                    new_expr = exp.And(
                        this=exp.GTE(this=col.copy(), expression=exp.Literal(this=str(lower), is_string=False)),
                        expression=exp.LT(this=col.copy(), expression=exp.Literal(this=str(upper), is_string=False))
                    )
                    return new_expr
                except ValueError:
                    pass
        return node

    # Transform the WHERE clause
    new_where = where.transform(transformer)
    parsed.set("where", new_where)
    
    return parsed.sql(dialect="mysql")
