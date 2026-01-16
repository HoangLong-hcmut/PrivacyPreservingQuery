import sqlglot
from sqlglot import exp

def rewrite_for_count(sql: str) -> str:
    """
    Rewrites query to return count for cohort analysis.
    Strips columns and uses COUNT(DISTINCT id) based on table.
    """
    parsed = sqlglot.parse_one(sql)
    if not isinstance(parsed, exp.Select):
       return sql 

    # Identify primary table
    table_name = ""
    for table in parsed.find_all(exp.Table):
        table_name = table.name.lower()
        break
    
    # Map tables to their sensitive entity identifier
    id_map = {
        "staffs": "staff_id",
        "patients": "patient_id",
        "diagnoses": "patient_id"
    }
    
    target_col = id_map.get(table_name, "patient_id")
        
    # Construct COUNT(DISTINCT col)
    count_expr = exp.Count(this=exp.Distinct(expressions=[exp.Column(this=exp.Identifier(this=target_col, quoted=False))]))
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
        if isinstance(expr, exp.Alias) and isinstance(expr.this, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            is_aggregate = True
            break

    if not is_aggregate:
        # Defaults to COUNT(*) for safety
        parsed.set("expressions", [exp.Count(this=exp.Star())])
    
    return parsed.sql(dialect="mysql")

def generalize_filters(sql: str) -> str:
    """
    Generalization.
    """
    parsed = sqlglot.parse_one(sql)
    
    where = parsed.args.get("where")
    if not where:
        return sql
        
    def transformer(node):
        if isinstance(node, (exp.EQ, exp.LT, exp.LTE, exp.GT, exp.GTE)):
            col = node.left if isinstance(node.left, exp.Column) else node.right
            val = node.right if isinstance(node.left, exp.Column) else node.left
            
            if isinstance(col, exp.Column) and (col.name.lower() in ["age", "privacy_budget"]):
                try:
                    # Parse value
                    age_val = int(str(val))
                    
                    # 1. Handle Equality (=): Convert to Bucket Range [x, x+10)
                    if isinstance(node, exp.EQ):
                        lower = (age_val // 10) * 10
                        upper = lower + 10
                        return exp.And(
                            this=exp.GTE(this=col.copy(), expression=exp.Literal(this=str(lower), is_string=False)),
                            expression=exp.LT(this=col.copy(), expression=exp.Literal(this=str(upper), is_string=False))
                        )
                    
                    # 2. Handle Less Than (<, <=): Round Upper Bound UP to nearest 10
                    elif isinstance(node, (exp.LT, exp.LTE)):
                        upper = ((age_val // 10) + 1) * 10
                        return exp.LT(this=col.copy(), expression=exp.Literal(this=str(upper), is_string=False))

                    # 3. Handle Greater Than (>, >=): Round Lower Bound DOWN to nearest 10
                    elif isinstance(node, (exp.GT, exp.GTE)):
                        lower = (age_val // 10) * 10
                        return exp.GTE(this=col.copy(), expression=exp.Literal(this=str(lower), is_string=False))

                except ValueError:
                    pass
        return node

    # Transform the WHERE clause
    new_where = where.transform(transformer)
    parsed.set("where", new_where)
    
    return parsed.sql(dialect="mysql")
