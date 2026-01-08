# API Changes Summary

## Modified Functions and Methods

### 1. `DatabaseConnector.load_table_details()`

**Before:**
```python
async def load_table_details(self, table_name: str) -> Optional[Dict[str, Any]]:
    # Returns: {"columns": [...], "relationships": {...}}
```

**After:**
```python
async def load_table_details(self, table_name: str, fetch_all_metadata: bool = False) -> Optional[Dict[str, Any]]:
    # Returns: {
    #   "columns": [...],           # Always included
    #   "relationships": {...},     # Always included
    #   "constraints": [...],       # Only if fetch_all_metadata=True
    #   "indexes": [...],          # Only if fetch_all_metadata=True
    #   "table_stats": {...},      # Only if fetch_all_metadata=True
    #   "comments": {...}          # Only if fetch_all_metadata=True
    # }
```

**Enhanced Column Information:**
```python
# Before
{"name": "EMPLOYEE_ID", "type": "NUMBER", "nullable": false}

# After
{
    "name": "EMPLOYEE_ID",
    "type": "NUMBER", 
    "nullable": false,
    "precision": 6,        # NEW
    "scale": 0,           # NEW
    "length": 22,         # NEW
    "default": "1"        # NEW (if has default)
}
```

---

### 2. `SchemaManager.build_schema_index()`

**Before:**
```python
async def build_schema_index(self) -> Dict[str, TableInfo]:
    # Always returns basic table list with fully_loaded=False
```

**After:**
```python
async def build_schema_index(self, fetch_all_metadata: bool = False) -> Dict[str, TableInfo]:
    # If fetch_all_metadata=False: returns basic table list (backward compatible)
    # If fetch_all_metadata=True: returns complete metadata for all tables
```

---

### 3. `SchemaManager.load_or_build_cache()`

**Before:**
```python
async def load_or_build_cache(self, force_rebuild: bool = False) -> SchemaCache:
```

**After:**
```python
async def load_or_build_cache(self, force_rebuild: bool = False, fetch_all_metadata: bool = False) -> SchemaCache:
```

---

### 4. `DatabaseContext.rebuild_cache()`

**Before:**
```python
async def rebuild_cache(self) -> None:
    # Always uses lazy loading
```

**After:**
```python
async def rebuild_cache(self, fetch_all_metadata: bool = False) -> None:
    # fetch_all_metadata=False: lazy loading (default, backward compatible)
    # fetch_all_metadata=True: comprehensive indexing
```

---

### 5. MCP Tool: `rebuild_schema_cache`

**Before:**
```python
@mcp.tool()
async def rebuild_schema_cache(database_name: str, ctx: Context) -> str:
```

**After:**
```python
@mcp.tool()
async def rebuild_schema_cache(
    database_name: str, 
    fetch_all_metadata: bool = False,  # NEW PARAMETER
    ctx: Context = None
) -> str:
```

**Usage:**
```python
# Lazy loading (default)
result = await rebuild_schema_cache(database_name="hr")

# Full metadata indexing
result = await rebuild_schema_cache(database_name="hr", fetch_all_metadata=True)
```

---

## Model Changes

### `TableInfo` Class (db_context/models.py)

**Before:**
```python
@dataclass
class TableInfo:
    table_name: str
    columns: List[Dict[str, Any]]
    relationships: Dict[str, Dict[str, Any]]
    fully_loaded: bool = False
```

**After:**
```python
@dataclass
class TableInfo:
    table_name: str
    columns: List[Dict[str, Any]]
    relationships: Dict[str, Dict[str, Any]]
    fully_loaded: bool = False
    # NEW OPTIONAL FIELDS:
    constraints: Optional[List[Dict[str, Any]]] = None
    indexes: Optional[List[Dict[str, Any]]] = None
    table_stats: Optional[Dict[str, Any]] = None
    comments: Optional[Dict[str, str]] = None
```

---

## Data Structure Examples

### Complete TableInfo Object (with all metadata)

```python
TableInfo(
    table_name="EMPLOYEES",
    columns=[
        {
            "name": "EMPLOYEE_ID",
            "type": "NUMBER",
            "nullable": False,
            "precision": 6,
            "scale": 0,
            "length": 22
        },
        {
            "name": "EMAIL",
            "type": "VARCHAR2",
            "nullable": False,
            "length": 25,
            "default": "'NO_EMAIL'"
        }
    ],
    relationships={
        "DEPARTMENTS": [
            {
                "local_column": "DEPARTMENT_ID",
                "foreign_column": "DEPARTMENT_ID",
                "direction": "OUTGOING"
            }
        ],
        "JOB_HISTORY": [
            {
                "local_column": "EMPLOYEE_ID",
                "foreign_column": "EMPLOYEE_ID",
                "direction": "INCOMING"
            }
        ]
    },
    constraints=[
        {
            "name": "EMP_PK",
            "type": "PRIMARY KEY",
            "columns": ["EMPLOYEE_ID"]
        },
        {
            "name": "EMP_DEPT_FK",
            "type": "FOREIGN KEY",
            "columns": ["DEPARTMENT_ID"],
            "references": {
                "table": "DEPARTMENTS",
                "columns": ["DEPARTMENT_ID"]
            }
        },
        {
            "name": "EMP_EMAIL_UK",
            "type": "UNIQUE",
            "columns": ["EMAIL"]
        },
        {
            "name": "EMP_SALARY_MIN",
            "type": "CHECK",
            "columns": ["SALARY"],
            "condition": "salary > 0"
        }
    ],
    indexes=[
        {
            "name": "EMP_PK",
            "unique": True,
            "columns": ["EMPLOYEE_ID"],
            "tablespace": "USERS",
            "status": "VALID"
        },
        {
            "name": "EMP_NAME_IX",
            "unique": False,
            "columns": ["LAST_NAME", "FIRST_NAME"],
            "status": "VALID"
        }
    ],
    table_stats={
        "row_count": 107,
        "blocks": 5,
        "avg_row_length": 69,
        "last_analyzed": "2026-01-08 10:30:00"
    },
    comments={
        "table": "Employees information including name, job, salary and department",
        "columns": {
            "EMPLOYEE_ID": "Primary key of employees table",
            "EMAIL": "Email address of the employee",
            "SALARY": "Monthly salary of the employee"
        }
    },
    fully_loaded=True
)
```

---

## Migration Guide

### For Existing Code

**No changes required!** All modifications are backward compatible:

```python
# This still works exactly as before
await db_context.rebuild_cache()

# This is equivalent to the above
await db_context.rebuild_cache(fetch_all_metadata=False)

# This is the new enhanced mode
await db_context.rebuild_cache(fetch_all_metadata=True)
```

### Accessing New Metadata

```python
table_info = await db_context.get_schema_info("EMPLOYEES")

# Always available (backward compatible)
print(table_info.columns)
print(table_info.relationships)

# New fields - check if populated
if table_info.constraints:
    print(f"Constraints: {table_info.constraints}")

if table_info.indexes:
    print(f"Indexes: {table_info.indexes}")

if table_info.table_stats:
    print(f"Rows: {table_info.table_stats.get('row_count')}")

if table_info.comments:
    print(f"Table description: {table_info.comments.get('table')}")
```

---

## Breaking Changes

**None!** All changes are additive and backward compatible.

- New parameters have default values
- New fields are optional (None by default)
- Existing behavior unchanged when new parameters not used

---

## Return Value Changes

### `rebuild_schema_cache` MCP Tool

**Before:**
```
"Schema cache for 'hr' rebuilt successfully in 1.23 seconds. Indexed 7 tables."
```

**After (with fetch_all_metadata=False):**
```
"Schema cache for 'hr' rebuilt successfully in 1.23 seconds.
Indexed 7 tables (0 with full metadata)."
```

**After (with fetch_all_metadata=True):**
```
"Schema cache for 'hr' rebuilt successfully in 15.67 seconds.
Indexed 7 tables (7 with full metadata).

Full metadata was fetched including:
- Column details (types, lengths, precision, defaults)
- Relationships (foreign keys in/out)
- Constraints (primary, unique, check, foreign keys)
- Indexes (columns, uniqueness, status)
- Table statistics (row counts, sizes, last analyzed)
- Comments (table and column documentation)"
```

---

## Testing Your Code

### Test 1: Verify Backward Compatibility
```python
# Should work exactly as before
await db_context.rebuild_cache()
table_info = await db_context.get_schema_info("EMPLOYEES")
assert table_info.columns is not None
assert table_info.relationships is not None
```

### Test 2: Verify New Metadata
```python
# Fetch full metadata
await db_context.rebuild_cache(fetch_all_metadata=True)
table_info = await db_context.get_schema_info("EMPLOYEES")

# Check new fields are populated
assert table_info.fully_loaded == True
assert table_info.constraints is not None
assert table_info.indexes is not None
# Note: stats and comments may be None if table has no stats/comments
```

### Test 3: Verify Lazy Loading Still Works
```python
# Lazy load
await db_context.rebuild_cache(fetch_all_metadata=False)
table_info = await db_context.get_schema_info("EMPLOYEES")

# After lazy load, should have full metadata
assert table_info.fully_loaded == True
assert table_info.constraints is not None
```
