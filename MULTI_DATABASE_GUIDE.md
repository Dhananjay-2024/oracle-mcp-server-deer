# Multi-Database Oracle MCP Server Guide

## Overview

The Oracle MCP Server now supports connecting to **multiple Oracle databases simultaneously**. This enables you to:

- Query different databases (prod, test, dev, etc.) from a single MCP server instance
- Compare data and schemas across environments
- Maintain separate schema caches for each database
- Execute queries with database-specific context

## Configuration

### Environment Variables

The server supports two configuration modes:

#### Mode 1: Multiple Databases (Recommended for multiple connections)

Set the `DB_NAMES` environment variable with comma-separated database names, then configure each database:

```bash
# List all database names
DB_NAMES=prod,test,dev

# Production Database Configuration
DB_PROD_CONNECTION_STRING=prod_user/prod_pass@prod-host:1521/PRODPDB
DB_PROD_SCHEMA=PROD_SCHEMA
DB_PROD_THICK_MODE=0
DB_PROD_CLIENT_LIB_DIR=

# Test Database Configuration
DB_TEST_CONNECTION_STRING=test_user/test_pass@test-host:1521/TESTPDB
DB_TEST_SCHEMA=TEST_SCHEMA
DB_TEST_THICK_MODE=0
DB_TEST_CLIENT_LIB_DIR=

# Development Database Configuration
DB_DEV_CONNECTION_STRING=dev_user/dev_pass@dev-host:1521/DEVPDB
DB_DEV_SCHEMA=DEV_SCHEMA
DB_DEV_THICK_MODE=0
DB_DEV_CLIENT_LIB_DIR=
```

**Configuration Pattern:**
- `DB_{NAME}_CONNECTION_STRING`: Oracle connection string (required)
- `DB_{NAME}_SCHEMA`: Target schema name (optional)
- `DB_{NAME}_THICK_MODE`: Enable thick mode, 0 or 1 (optional)
- `DB_{NAME}_CLIENT_LIB_DIR`: Oracle client library directory (optional)

#### Mode 2: Single Database (Backward Compatible)

If `DB_NAMES` is not set, the server falls back to single-database mode:

```bash
ORACLE_CONNECTION_STRING=user/pass@localhost:1521/mydb
TARGET_SCHEMA=MY_SCHEMA
THICK_MODE=0
ORACLE_CLIENT_LIB_DIR=
```

When using single-database mode, the database is automatically named "default".

### Global Settings

These settings apply to all databases:

```bash
# Cache directory (defaults to .cache)
CACHE_DIR=.cache

# Read-only mode (1 = enabled, 0 = disabled)
READ_ONLY_MODE=1
```

## Available Tools

All existing tools now require a `database_name` parameter as the **first argument**. Additionally, two new tools are available for managing multiple databases.

### New Multi-Database Tools

#### `list_databases`
List all configured databases.

**Example:**
```
list_databases()
```

**Returns:**
```
Available databases:
  - prod
  - test
  - dev
```

#### `get_all_database_info`
Get vendor information for all configured databases.

**Example:**
```
get_all_database_info()
```

**Returns:**
```
Database Information:

prod:
  Vendor: Oracle
  Version: 19.0.0.0.0
  Schema: PROD_SCHEMA

test:
  Vendor: Oracle
  Version: 19.0.0.0.0
  Schema: TEST_SCHEMA

dev:
  Vendor: Oracle
  Version: 21.0.0.0.0
  Schema: DEV_SCHEMA
```

### Updated Existing Tools

All existing tools now require `database_name` as the first parameter:

#### Schema Discovery Tools

- `get_table_schema(database_name, table_name)` - Get schema for a specific table
- `get_tables_schema(database_name, table_names)` - Batch get schemas for multiple tables
- `search_tables_schema(database_name, search_term)` - Search for tables by name pattern
- `search_columns(database_name, search_term)` - Search for columns across tables

#### Database Metadata Tools

- `get_database_vendor_info(database_name)` - Get Oracle version and schema info
- `get_table_constraints(database_name, table_name)` - Get PK, FK, unique, check constraints
- `get_table_indexes(database_name, table_name)` - Get index information
- `get_related_tables(database_name, table_name)` - Get FK-related tables

#### PL/SQL Tools

- `get_pl_sql_objects(database_name, object_type, name_pattern)` - List stored procedures, functions, packages
- `get_object_source(database_name, object_type, object_name)` - Get DDL/source code
- `get_dependent_objects(database_name, object_name)` - Get objects depending on a table/object

#### Advanced Tools

- `get_user_defined_types(database_name, type_pattern)` - List user-defined types
- `run_sql_query(database_name, sql, max_rows=100)` - Execute SQL query
- `explain_query_plan(database_name, sql)` - Get query execution plan
- `rebuild_schema_cache(database_name)` - Force rebuild of schema cache

## Usage Examples

### Example 1: Compare table schemas across environments

```python
# Get CUSTOMERS table from production
prod_schema = get_table_schema("prod", "CUSTOMERS")

# Get CUSTOMERS table from test
test_schema = get_table_schema("test", "CUSTOMERS")

# Compare them...
```

### Example 2: Cross-database data comparison

```python
# Count customers in production
prod_count = run_sql_query("prod", "SELECT COUNT(*) FROM CUSTOMERS")

# Count customers in test
test_count = run_sql_query("test", "SELECT COUNT(*) FROM CUSTOMERS")

# Compare counts
```

### Example 3: Search for tables across all databases

```python
# First, list available databases
databases = list_databases()

# Search for ORDER tables in production
prod_orders = search_tables_schema("prod", "ORDER")

# Search for ORDER tables in test
test_orders = search_tables_schema("test", "ORDER")
```

### Example 4: Check constraints across environments

```python
# Get constraints from production
prod_constraints = get_table_constraints("prod", "ORDERS")

# Get constraints from test
test_constraints = get_table_constraints("test", "ORDERS")

# Verify consistency
```

### Example 5: Query multiple databases in sequence

```python
# Get all database information
all_info = get_all_database_info()

# Run the same query on all databases
for db_name in ["prod", "test", "dev"]:
    result = run_sql_query(db_name, "SELECT COUNT(*) FROM EMPLOYEES")
    print(f"{db_name}: {result}")
```

## Best Practices

### 1. Use Descriptive Database Names
Choose clear, short names that indicate the environment:
- `prod`, `test`, `dev`
- `main`, `backup`
- `warehouse`, `transactional`

### 2. Maintain Consistent Schemas
When possible, keep table structures consistent across databases to simplify comparisons.

### 3. Cache Management
Each database maintains its own schema cache in separate files:
- `{cache_dir}/prod_schema_cache.json`
- `{cache_dir}/test_schema_cache.json`
- `{cache_dir}/dev_schema_cache.json`

Rebuild caches individually as needed:
```python
rebuild_schema_cache("prod")
```

### 4. Security Considerations
- Use read-only mode by default (`READ_ONLY_MODE=1`)
- Use environment variables for credentials
- Never commit `.env` files with actual credentials
- Consider using separate accounts with minimal privileges for each environment

### 5. Error Handling
Always specify the correct database name. If a database doesn't exist, you'll get:
```
Database 'xyz' not found. Available: prod, test, dev
```

## Migration from Single Database

If you're migrating from a single-database setup:

### Option 1: Keep using single database mode
Simply don't set `DB_NAMES`. The server will continue working as before with the "default" database name.

### Option 2: Migrate to multi-database
1. Choose a name for your existing database (e.g., "main")
2. Set `DB_NAMES=main`
3. Rename environment variables:
   - `ORACLE_CONNECTION_STRING` → `DB_MAIN_CONNECTION_STRING`
   - `TARGET_SCHEMA` → `DB_MAIN_SCHEMA`
   - `THICK_MODE` → `DB_MAIN_THICK_MODE`
   - `ORACLE_CLIENT_LIB_DIR` → `DB_MAIN_CLIENT_LIB_DIR`
4. Update all tool calls to include `database_name="main"` as the first parameter

## VS Code Configuration

Example `settings.json` for multi-database setup:

```json
{
  "mcp": {
    "inputs": [
      {
        "id": "prod-db-password",
        "type": "promptString",
        "description": "Production Oracle DB Password",
        "password": true
      },
      {
        "id": "test-db-password",
        "type": "promptString",
        "description": "Test Oracle DB Password",
        "password": true
      },
      {
        "id": "dev-db-password",
        "type": "promptString",
        "description": "Development Oracle DB Password",
        "password": true
      }
    ],
    "servers": {
      "oracle-multi": {
        "command": "uv",
        "args": [
          "--directory",
          "C:\\path\\to\\oracle-mcp-server",
          "run",
          "main.py"
        ],
        "env": {
          "DB_NAMES": "prod,test,dev",
          "DB_PROD_CONNECTION_STRING": "prod_user/${input:prod-db-password}@prod-host:1521/PRODPDB",
          "DB_PROD_SCHEMA": "PROD_SCHEMA",
          "DB_TEST_CONNECTION_STRING": "test_user/${input:test-db-password}@test-host:1521/TESTPDB",
          "DB_TEST_SCHEMA": "TEST_SCHEMA",
          "DB_DEV_CONNECTION_STRING": "dev_user/${input:dev-db-password}@dev-host:1521/DEVPDB",
          "DB_DEV_SCHEMA": "DEV_SCHEMA",
          "CACHE_DIR": ".cache",
          "READ_ONLY_MODE": "1"
        }
      }
    }
  }
}
```

## Troubleshooting

### Issue: "Database 'xyz' not found"
**Solution:** Check that the database name is included in `DB_NAMES` and properly configured.

### Issue: Connection fails for one database
**Solution:** Verify the connection string and credentials for that specific database. Other databases will continue to work.

### Issue: Cache is stale after schema changes
**Solution:** Use `rebuild_schema_cache(database_name)` to refresh the cache for the affected database.

### Issue: Missing configuration error on startup
**Solution:** Ensure all required environment variables are set. For database "xyz", you must have `DB_XYZ_CONNECTION_STRING`.

## Performance Considerations

- Each database maintains its own connection pool
- Schema caches are loaded in parallel during initialization
- Cache files are separate, reducing contention
- Read-only mode (default) prevents accidental data modifications

## Limitations

1. **Global Settings:** `READ_ONLY_MODE` and `CACHE_DIR` apply to all databases
2. **Same Oracle Driver:** All databases must be accessible with the same Oracle driver configuration
3. **Synchronous Operations:** Database discovery and cache initialization happen sequentially during startup

## Advanced Use Cases

### Cross-Database Joins (Manual)
Since Oracle doesn't support cross-database queries directly, you can fetch data from multiple databases and join in your application:

```python
# Get customer IDs from prod
prod_customers = run_sql_query("prod", "SELECT customer_id FROM customers WHERE status='active'")

# Get orders from warehouse
orders = run_sql_query("warehouse", "SELECT * FROM orders WHERE customer_id IN (1,2,3)")
```

### Schema Drift Detection
Compare schemas across environments to detect drift:

```python
# Get table schema from each environment
prod_schema = get_table_schema("prod", "ORDERS")
test_schema = get_table_schema("test", "ORDERS")
dev_schema = get_table_schema("dev", "ORDERS")

# Compare and identify differences
```

### Bulk Operations Across Databases
Execute the same operation on multiple databases:

```python
databases = ["prod", "test", "dev"]
table_name = "EMPLOYEES"

for db in databases:
    constraints = get_table_constraints(db, table_name)
    print(f"{db}: {constraints}")
```

## Support

For issues, questions, or feature requests, please refer to the main README or open an issue on the project repository.
