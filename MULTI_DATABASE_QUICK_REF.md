# Multi-Database Quick Reference

## Configuration Summary

### Environment Variables (Multi-Database)

```bash
# Required: List all database names
DB_NAMES=prod,test,dev

# For each database, configure:
DB_{NAME}_CONNECTION_STRING=user/pass@host:port/service    # Required
DB_{NAME}_SCHEMA=schema_name                               # Optional
DB_{NAME}_THICK_MODE=0                                     # Optional (0 or 1)
DB_{NAME}_CLIENT_LIB_DIR=/path/to/oracle/client           # Optional

# Global settings
CACHE_DIR=.cache                                           # Optional
READ_ONLY_MODE=1                                           # Optional (0 or 1)
```

### Single Database (Backward Compatible)

```bash
ORACLE_CONNECTION_STRING=user/pass@host:port/service
TARGET_SCHEMA=schema_name
THICK_MODE=0
ORACLE_CLIENT_LIB_DIR=/path/to/oracle/client
CACHE_DIR=.cache
READ_ONLY_MODE=1
```

## Tool Reference

### New Multi-Database Tools

| Tool | Description | Example |
|------|-------------|---------|
| `list_databases()` | List all configured databases | `list_databases()` |
| `get_all_database_info()` | Get info for all databases | `get_all_database_info()` |

### Updated Tools (All Now Require `database_name` as First Parameter)

#### Schema Discovery
| Tool | Parameters | Example |
|------|------------|---------|
| `get_table_schema` | `database_name, table_name` | `get_table_schema("prod", "CUSTOMERS")` |
| `get_tables_schema` | `database_name, table_names[]` | `get_tables_schema("test", ["ORDERS", "ITEMS"])` |
| `search_tables_schema` | `database_name, search_term` | `search_tables_schema("dev", "ORDER")` |
| `search_columns` | `database_name, search_term` | `search_columns("prod", "customer_id")` |

#### Database Metadata
| Tool | Parameters | Example |
|------|------------|---------|
| `get_database_vendor_info` | `database_name` | `get_database_vendor_info("prod")` |
| `get_table_constraints` | `database_name, table_name` | `get_table_constraints("prod", "ORDERS")` |
| `get_table_indexes` | `database_name, table_name` | `get_table_indexes("test", "CUSTOMERS")` |
| `get_related_tables` | `database_name, table_name` | `get_related_tables("prod", "ORDERS")` |

#### PL/SQL Objects
| Tool | Parameters | Example |
|------|------------|---------|
| `get_pl_sql_objects` | `database_name, object_type, name_pattern` | `get_pl_sql_objects("prod", "PROCEDURE", "CALC_%")` |
| `get_object_source` | `database_name, object_type, object_name` | `get_object_source("prod", "FUNCTION", "GET_CUSTOMER")` |
| `get_dependent_objects` | `database_name, object_name` | `get_dependent_objects("prod", "CUSTOMERS")` |

#### Advanced
| Tool | Parameters | Example |
|------|------------|---------|
| `get_user_defined_types` | `database_name, type_pattern` | `get_user_defined_types("prod", "CUST%")` |
| `run_sql_query` | `database_name, sql, max_rows` | `run_sql_query("prod", "SELECT * FROM orders", 50)` |
| `explain_query_plan` | `database_name, sql` | `explain_query_plan("prod", "SELECT * FROM big_table")` |
| `rebuild_schema_cache` | `database_name` | `rebuild_schema_cache("prod")` |

## Common Use Cases

### 1. List Available Databases
```python
databases = list_databases()
# Output: "Available databases:\n  - prod\n  - test\n  - dev"
```

### 2. Query Same Table Across Environments
```python
# Production
prod_data = run_sql_query("prod", "SELECT COUNT(*) FROM customers")

# Test
test_data = run_sql_query("test", "SELECT COUNT(*) FROM customers")

# Development
dev_data = run_sql_query("dev", "SELECT COUNT(*) FROM customers")
```

### 3. Compare Schemas
```python
prod_schema = get_table_schema("prod", "ORDERS")
test_schema = get_table_schema("test", "ORDERS")
# Compare the outputs to find differences
```

### 4. Check All Database Versions
```python
info = get_all_database_info()
# Returns info for all databases at once
```

### 5. Search Across Specific Database
```python
# Find all tables containing "customer" in production
results = search_tables_schema("prod", "customer")
```

## Migration Checklist

Migrating from single to multi-database? Follow these steps:

- [ ] Choose database names (e.g., "prod", "test", "dev")
- [ ] Set `DB_NAMES` environment variable
- [ ] Rename connection string variables:
  - `ORACLE_CONNECTION_STRING` → `DB_{NAME}_CONNECTION_STRING`
  - `TARGET_SCHEMA` → `DB_{NAME}_SCHEMA`
  - `THICK_MODE` → `DB_{NAME}_THICK_MODE`
  - `ORACLE_CLIENT_LIB_DIR` → `DB_{NAME}_CLIENT_LIB_DIR`
- [ ] Update all tool calls to include `database_name` as first parameter
- [ ] Test with `list_databases()` to verify configuration
- [ ] Verify caches are created: `.cache/{dbname}_schema_cache.json`

## Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `Database 'xyz' not found` | Invalid database name | Check `DB_NAMES` and ensure database is configured |
| `Missing connection string for database 'xyz'` | Missing `DB_XYZ_CONNECTION_STRING` | Add connection string for that database |
| `Either DB_NAMES or ORACLE_CONNECTION_STRING must be set` | No databases configured | Set either multi or single database config |

## Tips

1. **Database naming**: Use short, descriptive names (prod, test, dev) for clarity
2. **Cache location**: Each database gets its own cache file: `{name}_schema_cache.json`
3. **Performance**: All databases are initialized in parallel at startup
4. **Security**: `READ_ONLY_MODE` applies to all databases globally
5. **Backward compatibility**: Omit `DB_NAMES` to use single-database mode with "default" as the database name

## See Also

- [Complete Multi-Database Guide](MULTI_DATABASE_GUIDE.md) - Comprehensive documentation
- [Main README](README.md) - General server documentation
- [.env.example](.env.example) - Configuration template
