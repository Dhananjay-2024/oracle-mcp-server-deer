# SQL Execution Tools - Quick Reference

## Overview

The Oracle MCP Server provides two complementary tools for executing SQL statements:

1. **`run_sql_query`** - Universal SQL executor (SELECT and write operations)
2. **`execute_write_sql`** - Dedicated tool for write operations with enhanced error handling

## Tools

### 1. run_sql_query

**Purpose:** Execute any SQL statement (SELECT, DML, DDL)

**Syntax:**
```python
run_sql_query(
    database_name: str,
    sql: str,
    max_rows: int = 100  # Only applies to SELECT
)
```

**Supports:**
- ✅ SELECT queries (returns formatted results)
- ✅ INSERT, UPDATE, DELETE (requires write mode)
- ✅ DDL: CREATE, ALTER, DROP (requires write mode)
- ✅ Other statements: TRUNCATE, GRANT, etc. (requires write mode)

**Examples:**

```python
# Read data
run_sql_query(
    database_name="hr",
    sql="SELECT * FROM employees WHERE department_id = 10",
    max_rows=50
)

# Write data (requires write mode)
run_sql_query(
    database_name="hr",
    sql="INSERT INTO departments (department_id, department_name) VALUES (300, 'AI')"
)

# DDL (requires write mode)
run_sql_query(
    database_name="hr",
    sql="CREATE INDEX idx_emp_name ON employees(last_name, first_name)"
)
```

**Returns:**
- SELECT: Formatted table with columns and rows
- Write operations: Affected row count or success message

---

### 2. execute_write_sql

**Purpose:** Dedicated tool for write operations with enhanced error handling

**Syntax:**
```python
execute_write_sql(
    database_name: str,
    sql: str  # DML or DDL only
)
```

**Supports:**
- ✅ INSERT - Add new rows
- ✅ UPDATE - Modify existing rows
- ✅ DELETE - Remove rows
- ✅ DDL - CREATE, ALTER, DROP, TRUNCATE, etc.
- ❌ SELECT - Use run_sql_query instead

**Examples:**

```python
# Insert data
execute_write_sql(
    database_name="hr",
    sql="""
    INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id)
    VALUES (500, 'John', 'Doe', 'JDOE', SYSDATE, 'IT_PROG')
    """
)

# Update data
execute_write_sql(
    database_name="hr",
    sql="UPDATE employees SET salary = salary * 1.1 WHERE job_id = 'IT_PROG'"
)

# Delete data
execute_write_sql(
    database_name="hr",
    sql="DELETE FROM job_history WHERE end_date < DATE '2020-01-01'"
)

# Create table
execute_write_sql(
    database_name="hr",
    sql="""
    CREATE TABLE temp_analysis (
        id NUMBER PRIMARY KEY,
        analysis_date DATE,
        result VARCHAR2(1000)
    )
    """
)

# Add column
execute_write_sql(
    database_name="hr",
    sql="ALTER TABLE employees ADD (middle_name VARCHAR2(20))"
)

# Create index
execute_write_sql(
    database_name="hr",
    sql="CREATE INDEX idx_dept_name ON departments(department_name)"
)

# Drop table
execute_write_sql(
    database_name="hr",
    sql="DROP TABLE temp_analysis"
)
```

**Returns:**
- ✓ Success message with affected row count
- ✗ Enhanced error messages with troubleshooting hints

**Error Handling:**
- Clear permission errors with instructions
- Oracle error codes (ORA-XXXXX)
- Common issue suggestions

---

## Read-Only vs Write Mode

### Read-Only Mode (Default)

**Configuration in mcp.json:**
```json
{
  "databases": {
    "hr": {
      "connection_string": "user/pass@host:port/service",
      "read_only": true
    }
  }
}
```

**Allowed:**
- ✅ SELECT statements
- ✅ Viewing execution plans (may fail on some systems)

**Blocked:**
- ❌ INSERT, UPDATE, DELETE
- ❌ CREATE, ALTER, DROP
- ❌ TRUNCATE, GRANT, REVOKE

**Error Message:**
```
Permission error: Read-only mode: only SELECT statements are permitted.
```

---

### Write Mode (Requires Configuration)

**Configuration in mcp.json:**
```json
{
  "databases": {
    "hr": {
      "connection_string": "user/pass@host:port/service",
      "read_only": false  ← Enable write mode
    }
  }
}
```

**Allowed:**
- ✅ All SELECT statements
- ✅ INSERT, UPDATE, DELETE (auto-commit)
- ✅ CREATE, ALTER, DROP, TRUNCATE
- ✅ Other DDL/DML operations

**Important:**
- Changes are automatically committed
- Cannot be rolled back
- Use with caution in production

---

## Comparison: Which Tool to Use?

| Scenario | Recommended Tool | Reason |
|----------|-----------------|--------|
| Read data | `run_sql_query` | Supports max_rows, formatted output |
| Insert/Update/Delete | Either | Use `execute_write_sql` for clearer intent |
| DDL operations | Either | Use `execute_write_sql` for better errors |
| Both read and write | `run_sql_query` | Single tool for mixed operations |
| Error diagnostics | `execute_write_sql` | Enhanced error messages |

---

## Common Use Cases

### 1. Data Analysis
```python
run_sql_query(
    database_name="hr",
    sql="""
    SELECT department_name, COUNT(*) as emp_count, AVG(salary) as avg_salary
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    GROUP BY department_name
    ORDER BY emp_count DESC
    """,
    max_rows=50
)
```

### 2. Data Correction
```python
execute_write_sql(
    database_name="hr",
    sql="UPDATE employees SET email = UPPER(email) WHERE email != UPPER(email)"
)
```

### 3. Temporary Analysis Table
```python
# Create
execute_write_sql(
    database_name="hr",
    sql="CREATE GLOBAL TEMPORARY TABLE temp_results (id NUMBER, data VARCHAR2(100))"
)

# Populate
execute_write_sql(
    database_name="hr",
    sql="INSERT INTO temp_results SELECT employee_id, last_name FROM employees"
)

# Query
run_sql_query(
    database_name="hr",
    sql="SELECT * FROM temp_results"
)

# Cleanup
execute_write_sql(
    database_name="hr",
    sql="DROP TABLE temp_results"
)
```

### 4. Index Management
```python
# Create index
execute_write_sql(
    database_name="hr",
    sql="CREATE INDEX idx_emp_hire_date ON employees(hire_date)"
)

# Check index usage
run_sql_query(
    database_name="hr",
    sql="""
    SELECT index_name, tablespace_name, status 
    FROM user_indexes 
    WHERE table_name = 'EMPLOYEES'
    """
)

# Drop index if not needed
execute_write_sql(
    database_name="hr",
    sql="DROP INDEX idx_emp_hire_date"
)
```

---

## Error Handling

### Permission Errors
```
[hr] ✗ Permission denied: Read-only mode: only SELECT statements are permitted.

Write operations require the database to be configured with read_only=False in mcp.json.
Contact your database administrator to enable write mode if needed.
```

### Constraint Violations
```
[hr] ✗ Database error (ORA-00001): unique constraint violated

Common issues:
- Insufficient privileges
- Constraint violation (foreign key, unique, check)
- Object already exists or doesn't exist
- Invalid SQL syntax
```

### Syntax Errors
```
[hr] ✗ Database error (ORA-00933): SQL command not properly ended
```

---

## Best Practices

1. **Use Read-Only Mode by Default**
   - Safer for exploration and analysis
   - Enable write mode only when needed

2. **Test Before Executing**
   - Use SELECT to verify data before UPDATE/DELETE
   - Use WHERE clauses carefully
   - Consider using transactions (COMMIT/ROLLBACK) if available

3. **Backup Important Data**
   - Before bulk updates or deletes
   - Before DDL operations

4. **Use execute_write_sql for Clear Intent**
   - Makes code more readable
   - Better error messages
   - Prevents accidental SELECT operations

5. **Handle Errors Gracefully**
   - Check for permission errors
   - Validate constraints before operations
   - Use proper error handling in scripts

---

## Troubleshooting

### Issue: "Read-only mode" error
**Solution:** Set `read_only: false` in database config

### Issue: "Insufficient privileges"
**Solution:** Contact DBA to grant necessary privileges

### Issue: "Unique constraint violated"
**Solution:** Check for duplicate keys before INSERT/UPDATE

### Issue: "Foreign key violation"
**Solution:** Ensure referenced records exist in parent table

### Issue: "Object does not exist"
**Solution:** Verify table/column names and schema

---

## Security Considerations

⚠️ **Important Security Notes:**

1. **Write Mode Risks**
   - Data can be modified or deleted permanently
   - Schema changes can break applications
   - No automatic rollback

2. **Privilege Principle**
   - Grant minimum necessary privileges
   - Use separate accounts for read vs write
   - Audit write operations

3. **Production Safety**
   - Use read-only mode in production by default
   - Require explicit approval for write mode
   - Log all write operations

4. **SQL Injection Prevention**
   - This tool is designed for trusted users
   - Never construct SQL from untrusted input
   - Use bind parameters when possible
