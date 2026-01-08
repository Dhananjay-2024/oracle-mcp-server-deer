# Comprehensive Database Metadata Indexing

## Overview

The Oracle MCP Server now supports comprehensive metadata indexing during schema cache building. This enhancement allows you to fetch **all available metadata** for database tables during the indexing process, rather than just basic table names.

## What Metadata is Now Available?

### Previously (Lazy Loading Only)
- Table names
- Columns (loaded on-demand)
- Relationships/Foreign Keys (loaded on-demand)

### Now Available (Full Metadata Mode)
1. **Column Details**
   - Column names and data types
   - Data length, precision, and scale
   - Nullable constraints
   - Default values

2. **Relationships**
   - Outgoing foreign keys (tables this table references)
   - Incoming foreign keys (tables that reference this table)
   - Source and target columns

3. **Constraints**
   - Primary keys
   - Foreign keys with referenced tables/columns
   - Unique constraints
   - Check constraints with conditions

4. **Indexes**
   - Index names and columns
   - Unique vs non-unique indexes
   - Tablespace information
   - Index status

5. **Table Statistics**
   - Row count
   - Number of blocks
   - Average row length
   - Last analyzed timestamp

6. **Comments**
   - Table-level documentation comments
   - Column-level documentation comments

## Usage

### Option 1: Lazy Loading (Default - Recommended for Large Databases)

```python
# During initialization or cache rebuild
await db_context.rebuild_cache(fetch_all_metadata=False)
```

**Behavior:**
- Indexes only table names initially (fast)
- Loads full metadata for each table when first accessed
- Best for databases with many tables (100+)
- Lower initial overhead

### Option 2: Comprehensive Indexing (Recommended for Smaller Databases or Complete Analysis)

```python
# During initialization or cache rebuild
await db_context.rebuild_cache(fetch_all_metadata=True)
```

**Behavior:**
- Fetches ALL metadata for ALL tables upfront
- Takes longer initially but provides complete information
- Best for smaller databases (< 100 tables) or when you need comprehensive metadata immediately
- Eliminates lazy loading overhead later

### Using the MCP Tool

```python
# Via MCP tool - Basic rebuild (lazy loading)
await rebuild_schema_cache(database_name="hr", fetch_all_metadata=False)

# Via MCP tool - Full metadata indexing
await rebuild_schema_cache(database_name="hr", fetch_all_metadata=True)
```

## Performance Considerations

### Lazy Loading Mode (Default)
- **Initial indexing time:** ~0.5-2 seconds for 100 tables
- **Memory usage:** Minimal initially
- **First access per table:** 50-200ms additional overhead
- **Best for:** Large databases, development environments

### Full Metadata Mode
- **Initial indexing time:** ~5-30 seconds for 100 tables (depends on complexity)
- **Memory usage:** Higher initially, but stable
- **First access per table:** Instant (no overhead)
- **Best for:** Production analysis, comprehensive reporting, smaller databases

## Example: Accessing Enhanced Metadata

### After Full Metadata Indexing

```python
# Get table schema - now includes constraints, indexes, stats
table_info = await db_context.get_schema_info("EMPLOYEES")

# Access new metadata fields
if table_info:
    # Basic info (always available)
    print(f"Columns: {table_info.columns}")
    print(f"Relationships: {table_info.relationships}")
    
    # Extended metadata (available when fully_loaded=True)
    if table_info.constraints:
        print(f"Constraints: {table_info.constraints}")
    
    if table_info.indexes:
        print(f"Indexes: {table_info.indexes}")
    
    if table_info.table_stats:
        print(f"Row count: {table_info.table_stats.get('row_count')}")
        print(f"Last analyzed: {table_info.table_stats.get('last_analyzed')}")
    
    if table_info.comments:
        print(f"Table comment: {table_info.comments.get('table')}")
        print(f"Column comments: {table_info.comments.get('columns')}")
```

## Cache Structure

The enhanced cache now stores the following in JSON format:

```json
{
  "tables": {
    "EMPLOYEES": {
      "table_name": "EMPLOYEES",
      "columns": [
        {
          "name": "EMPLOYEE_ID",
          "type": "NUMBER",
          "nullable": false,
          "precision": 6,
          "scale": 0
        }
      ],
      "relationships": {
        "DEPARTMENTS": [
          {
            "local_column": "DEPARTMENT_ID",
            "foreign_column": "DEPARTMENT_ID",
            "direction": "OUTGOING"
          }
        ]
      },
      "constraints": [
        {
          "name": "EMP_PK",
          "type": "PRIMARY KEY",
          "columns": ["EMPLOYEE_ID"]
        }
      ],
      "indexes": [
        {
          "name": "EMP_PK",
          "unique": true,
          "columns": ["EMPLOYEE_ID"],
          "status": "VALID"
        }
      ],
      "table_stats": {
        "row_count": 107,
        "blocks": 5,
        "avg_row_length": 69,
        "last_analyzed": "2026-01-08"
      },
      "comments": {
        "table": "Employee information table",
        "columns": {
          "EMPLOYEE_ID": "Primary key of employees table"
        }
      },
      "fully_loaded": true
    }
  },
  "last_updated": 1767853516.081895,
  "all_table_names": ["EMPLOYEES", "DEPARTMENTS", ...],
  "object_cache": {...},
  "cache_stats": {...}
}
```

## Migration Notes

### Backward Compatibility
- Existing caches will continue to work (lazy loading behavior)
- No breaking changes to existing code
- New metadata fields are optional (None if not loaded)

### Upgrading from Previous Version
1. Existing cache files will load successfully
2. Use `rebuild_cache(fetch_all_metadata=True)` to populate new metadata fields
3. Or continue with lazy loading - metadata will be fetched on-demand

## Best Practices

1. **For Development/Testing:**
   - Use default lazy loading for faster startup
   - Individual table metadata loads quickly on-demand

2. **For Production/Analysis:**
   - Use `fetch_all_metadata=True` during off-peak hours
   - Cache persists to disk, so full metadata is available immediately on subsequent starts
   - Schedule periodic full rebuilds (e.g., nightly) to keep statistics current

3. **For CI/CD Pipelines:**
   - Use lazy loading for faster pipeline execution
   - Only fetch full metadata when generating documentation

4. **For Data Catalog/Documentation:**
   - Always use `fetch_all_metadata=True`
   - Comments and statistics are essential for documentation

## Monitoring

The cache stats now track metadata loading:

```python
stats = db_context.schema_manager.get_cache_stats()
print(f"Total tables: {stats['size']['tables']}")
print(f"Fully loaded: {sum(1 for t in db_context.schema_manager.cache.tables.values() if t.fully_loaded)}")
print(f"Cache hits: {stats['hits']}")
print(f"Cache misses: {stats['misses']}")
```

## Future Enhancements

Potential future additions to metadata indexing:
- Partitioning information
- Triggers
- Materialized views
- Synonyms
- Privileges and grants
- Query execution history
- Column statistics and histograms
