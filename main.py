from mcp.server.fastmcp import FastMCP, Context
import json
import os
import sys
from typing import Dict, List, AsyncIterator, Optional, Any
import time
from contextlib import asynccontextmanager
from pathlib import Path
from dotenv import load_dotenv
import uuid  # retained for potential future use elsewhere
import oracledb

from db_context import DatabaseContext, MultiDatabaseContext
from db_context.utils import wrap_untrusted
from db_context.schema.formatter import format_sql_query_result

# Load environment variables from .env file
load_dotenv()

# Parse multiple database configurations from environment
def parse_database_configs() -> Dict[str, Dict[str, Any]]:
    """
    Parse database configurations from environment variables.
    
    Expected format:
    DB_NAMES=prod,test,dev
    DB_PROD_CONNECTION_STRING=user/pass@host:port/service
    DB_PROD_SCHEMA=PROD_SCHEMA
    DB_PROD_THICK_MODE=0
    DB_TEST_CONNECTION_STRING=...
    etc.
    """
    db_names_str = os.getenv('DB_NAMES', '')
    if not db_names_str:
        # Fallback to single database configuration
        single_conn = os.getenv('ORACLE_CONNECTION_STRING')
        if single_conn:
            return {
                "default": {
                    "connection_string": single_conn,
                    "target_schema": os.getenv('TARGET_SCHEMA'),
                    "use_thick_mode": os.getenv('THICK_MODE', '').lower() in ('true', '1', 'yes'),
                    "lib_dir": os.getenv('ORACLE_CLIENT_LIB_DIR')
                }
            }
        raise ValueError("Either DB_NAMES or ORACLE_CONNECTION_STRING must be set")
    
    db_names = [name.strip() for name in db_names_str.split(',')]
    databases = {}
    
    for db_name in db_names:
        prefix = f"DB_{db_name.upper()}_"
        conn_str = os.getenv(f"{prefix}CONNECTION_STRING")
        
        if not conn_str:
            raise ValueError(f"Missing connection string for database '{db_name}': {prefix}CONNECTION_STRING")
        
        databases[db_name] = {
            "connection_string": conn_str,
            "target_schema": os.getenv(f"{prefix}SCHEMA"),
            "use_thick_mode": os.getenv(f"{prefix}THICK_MODE", '').lower() in ('true', '1', 'yes'),
            "lib_dir": os.getenv(f"{prefix}CLIENT_LIB_DIR")
        }
    
    return databases

CACHE_DIR = os.getenv('CACHE_DIR', '.cache')
READ_ONLY_MODE = os.getenv('READ_ONLY_MODE', 'true').lower() not in ('false', '0', 'no')


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[MultiDatabaseContext]:
    """Manage application lifecycle for multiple databases"""
    print("Initializing Multi-Database MCP Server", file=sys.stderr)
    
    databases = parse_database_configs()
    print(f"Configured databases: {list(databases.keys())}", file=sys.stderr)
    
    cache_dir = Path(CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    multi_db_context = MultiDatabaseContext(
        databases=databases,
        cache_base_path=cache_dir,
        read_only=READ_ONLY_MODE
    )
    
    try:
        await multi_db_context.initialize()
        print("All database caches ready!", file=sys.stderr)
        yield multi_db_context
    finally:
        print("Closing database connections...", file=sys.stderr)
        await multi_db_context.close()

# Initialize FastMCP server
mcp = FastMCP("oracle-multi-db", lifespan=app_lifespan)
print("FastMCP Multi-Database server initialized", file=sys.stderr)

# Helper function to validate database name
def validate_database(ctx: Context, db_name: str) -> DatabaseContext:
    """Validate and return database context"""
    multi_ctx: MultiDatabaseContext = ctx.request_context.lifespan_context
    try:
        return multi_ctx.get_database(db_name)
    except ValueError as e:
        raise ValueError(f"{e}\nAvailable databases: {', '.join(multi_ctx.list_databases())}")

# --- CRUD TOOLS ---
@mcp.tool()
async def create_row(database_name: str, table_name: str, data: Dict[str, Any], ctx: Context) -> str:
    """Insert a new row into a table.

    Args:
        database_name: Name of the database
        table_name: Name of the table
        data: Dictionary of column-value pairs to insert
    """
    db_context = validate_database(ctx, database_name)
    columns = ', '.join(data.keys())
    placeholders = ', '.join(f":{k}" for k in data.keys())
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    try:
        result = await db_context.run_sql_query(sql, params=data)
        return wrap_untrusted(f"Inserted row into '{table_name}' in '{database_name}'. {result.get('message', '')}")
    except Exception as e:
        return wrap_untrusted(f"Error inserting row: {e}")

@mcp.tool()
async def read_rows(database_name: str, table_name: str, filters: Optional[Dict[str, Any]], ctx: Context, max_rows: int = 100) -> str:
    """Read rows from a table with optional filters.

    Args:
        database_name: Name of the database
        table_name: Name of the table
        filters: Optional dictionary of column-value pairs for WHERE clause
        max_rows: Maximum number of rows to return
    """
    db_context = validate_database(ctx, database_name)
    where = ''
    params = {}
    if filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :{k}" for k in filters.keys())
        params = filters
    sql = f"SELECT * FROM {table_name}{where}"
    try:
        result = await db_context.run_sql_query(sql, params=params, max_rows=max_rows)
        if not result.get("rows"):
            return wrap_untrusted(f"No rows found in '{table_name}' in '{database_name}'.")
        formatted = format_sql_query_result(result)
        return wrap_untrusted(f"Results from '{table_name}' in '{database_name}':\n\n{formatted}")
    except Exception as e:
        return wrap_untrusted(f"Error reading rows: {e}")

@mcp.tool()
async def update_rows(database_name: str, table_name: str, updates: Dict[str, Any], filters: Optional[Dict[str, Any]], ctx: Context) -> str:
    """Update rows in a table matching filters.

    Args:
        database_name: Name of the database
        table_name: Name of the table
        updates: Dictionary of column-value pairs to update
        filters: Optional dictionary of column-value pairs for WHERE clause
    """
    db_context = validate_database(ctx, database_name)
    set_clause = ', '.join(f"{k} = :set_{k}" for k in updates.keys())
    params = {f"set_{k}": v for k, v in updates.items()}
    where = ''
    if filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :where_{k}" for k in filters.keys())
        params.update({f"where_{k}": v for k, v in filters.items()})
    sql = f"UPDATE {table_name} SET {set_clause}{where}"
    try:
        result = await db_context.run_sql_query(sql, params=params)
        return wrap_untrusted(f"Updated rows in '{table_name}' in '{database_name}'. {result.get('message', '')}")
    except Exception as e:
        return wrap_untrusted(f"Error updating rows: {e}")

@mcp.tool()
async def delete_rows(database_name: str, table_name: str, filters: Optional[Dict[str, Any]], ctx: Context) -> str:
    """Delete rows from a table matching filters.

    Args:
        database_name: Name of the database
        table_name: Name of the table
        filters: Optional dictionary of column-value pairs for WHERE clause
    """
    db_context = validate_database(ctx, database_name)
    where = ''
    params = {}
    if filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :{k}" for k in filters.keys())
        params = filters
    sql = f"DELETE FROM {table_name}{where}"
    try:
        result = await db_context.run_sql_query(sql, params=params)
        return wrap_untrusted(f"Deleted rows from '{table_name}' in '{database_name}'. {result.get('message', '')}")
    except Exception as e:
        return wrap_untrusted(f"Error deleting rows: {e}")

@mcp.tool()
async def list_databases(ctx: Context) -> str:
    """List all available Oracle databases.
    
    Returns the names of all configured databases that you can query.
    """
    multi_ctx: MultiDatabaseContext = ctx.request_context.lifespan_context
    databases = multi_ctx.list_databases()
    return wrap_untrusted(f"Available databases:\n" + "\n".join(f"  - {db}" for db in databases))

@mcp.tool()
async def get_all_database_info(ctx: Context) -> str:
    """Get vendor information for all configured databases.
    
    Returns Oracle version and schema information for each database.
    """
    multi_ctx: MultiDatabaseContext = ctx.request_context.lifespan_context
    info = await multi_ctx.get_all_database_info()
    
    result = ["Database Information:\n"]
    for db_name, db_info in info.items():
        result.append(f"\n{db_name}:")
        if "error" in db_info:
            result.append(f"  Error: {db_info['error']}")
        else:
            result.append(f"  Vendor: {db_info.get('vendor', 'Unknown')}")
            result.append(f"  Version: {db_info.get('version', 'Unknown')}")
            result.append(f"  Schema: {db_info.get('schema', 'Unknown')}")
    
    return wrap_untrusted("\n".join(result))

@mcp.tool()
async def get_table_schema(database_name: str, table_name: str, ctx: Context) -> str:
    """Single-table columns + FK relationships (lazy loads & caches).

    Use: Inspect one table's structure before writing queries / building joins.
    Compose: Pair with get_table_constraints + get_table_indexes for a full profile.
    Avoid: Looping across many tables for ranking (prefer get_related_tables + constraints/indexes directly).

    Args:
        database_name: Name of the database to query
        table_name: Exact table name (case-insensitive).
    """
    db_context = validate_database(ctx, database_name)
    table_info = await db_context.get_schema_info(table_name)
    
    if not table_info:
        return wrap_untrusted(f"Table '{table_name}' not found in database '{database_name}'")
    
    # Delegate formatting to the TableInfo model
    return wrap_untrusted(table_info.format_schema())

@mcp.tool()
async def rebuild_schema_cache(database_name: str, fetch_all_metadata: bool = False, ctx: Context = None) -> str:
    """Rebuild the full schema index (expensive – invalidates caches).

    Use: After DDL changes that add/drop/rename many tables.
    Compose: Run once before bulk analytics if structure changed.
    Avoid: Inside loops or routine per-request flows.
    
    Args:
        database_name: Name of the database to rebuild cache for
        fetch_all_metadata: If True, fetches complete metadata (columns, relationships, constraints, 
                          indexes, statistics, comments) for ALL tables upfront. This is comprehensive 
                          but takes significantly longer. Default is False (lazy loading).
    """
    db_context = validate_database(ctx, database_name)
    try:
        start_time = time.time()
        metadata_mode = "with complete metadata" if fetch_all_metadata else "with lazy loading"
        print(f"Rebuilding cache {metadata_mode}...", file=sys.stderr)
        
        await db_context.rebuild_cache(fetch_all_metadata=fetch_all_metadata)
        
        elapsed = time.time() - start_time
        cache_size = len(db_context.schema_manager.cache.all_table_names) if db_context.schema_manager.cache else 0
        
        # Count fully loaded tables
        fully_loaded = sum(1 for t in db_context.schema_manager.cache.tables.values() if t.fully_loaded) if db_context.schema_manager.cache else 0
        
        result = f"Schema cache for '{database_name}' rebuilt successfully in {elapsed:.2f} seconds.\n"
        result += f"Indexed {cache_size} tables ({fully_loaded} with full metadata)."
        
        if fetch_all_metadata:
            result += f"\n\nFull metadata was fetched including:\n"
            result += f"- Column details (types, lengths, precision, defaults)\n"
            result += f"- Relationships (foreign keys in/out)\n"
            result += f"- Constraints (primary, unique, check, foreign keys)\n"
            result += f"- Indexes (columns, uniqueness, status)\n"
            result += f"- Table statistics (row counts, sizes, last analyzed)\n"
            result += f"- Comments (table and column documentation)"
        
        return wrap_untrusted(result)
    except Exception as e:
        return wrap_untrusted(f"Failed to rebuild cache for '{database_name}': {str(e)}")

@mcp.tool()
async def get_tables_schema(database_name: str, table_names: List[str], ctx: Context) -> str:
    """Batch version of get_table_schema for a small explicit list.

    Use: You already have a short candidate set (< ~25) and need detail.
    Compose: Combine results with constraints / indexes per table if deeper profiling needed.
    Avoid: Broad discovery (use search_tables_schema first).
    
    Args:
        database_name: Name of the database to query
        table_names: List of table names to retrieve schemas for
    """
    db_context = validate_database(ctx, database_name)
    results = []
    
    for table_name in table_names:
        table_info = await db_context.get_schema_info(table_name)
        if not table_info:
            results.append(f"\nTable '{table_name}' not found in database '{database_name}'.")
            continue
        
        # Delegate formatting to the TableInfo model
        results.append(table_info.format_schema())
    
    return wrap_untrusted("\n".join(results))

@mcp.tool()
async def search_tables_schema(database_name: str, search_term: str, ctx: Context) -> str:
    """Find tables by name fragments (multi-term OR) + show their schemas.

    Use: Initial discovery when exact table names unknown.
    Compose: Feed resulting names into deeper profiling (constraints/indexes/dependents).
    Avoid: Acting as a full table list (results capped at 20; filtered).
    
    Args:
        database_name: Name of the database to search
        search_term: Pattern to search for in table names
    """
    db_context = validate_database(ctx, database_name)
    
    # Split search term by commas and whitespace and remove empty strings
    search_terms = [term.strip() for term in search_term.replace(',', ' ').split()]
    search_terms = [term for term in search_terms if term]
    
    if not search_terms:
        return "No valid search terms provided"
    
    # Track all matching tables without duplicates
    matching_tables = set()
    
    # Search for each term
    for term in search_terms:
        tables = await db_context.search_tables(term, limit=20)
        matching_tables.update(tables)
    
    # Convert back to list and limit to 20 results
    matching_tables = list(matching_tables)
    total_matches = len(matching_tables)
    limited_tables = matching_tables[:20]
    
    if not matching_tables:
        return wrap_untrusted(f"No tables found in '{database_name}' matching any of these terms: {', '.join(search_terms)}")
    
    if total_matches > 20:
        results = [f"Found {total_matches} tables in '{database_name}' matching terms ({', '.join(search_terms)}). Returning the first 20 for performance reasons:"]
    else:
        results = [f"Found {total_matches} tables in '{database_name}' matching terms ({', '.join(search_terms)}):"]
    
    matching_tables = limited_tables
    
    # Now load the schema for each matching table
    for table_name in matching_tables:
        table_info = await db_context.get_schema_info(table_name)
        if not table_info:
            continue
        
        # Delegate formatting to the TableInfo model
        results.append(table_info.format_schema())
    
    return wrap_untrusted("\n".join(results))

@mcp.tool()
async def get_database_vendor_info(database_name: str, ctx: Context) -> str:
    """Database/edition + version + active schema context for a specific database.

    Use: Capability gating, logging environment.
    Compose: Call once early; reuse info client-side.
    Avoid: Polling repeatedly (metadata rarely changes per session).
    
    Args:
        database_name: Name of the database to query
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        db_info = await db_context.get_database_info()
        
        if not db_info:
            return wrap_untrusted(f"Could not retrieve database vendor information for '{database_name}'.")
        
        result = [f"Database: {database_name}"]
        result.append(f"Vendor: {db_info.get('vendor', 'Unknown')}")
        result.append(f"Version: {db_info.get('version', 'Unknown')}")
        if "schema" in db_info:
            result.append(f"Schema: {db_info['schema']}")
        
        if "additional_info" in db_info and db_info["additional_info"]:
            result.append("\nAdditional Version Information:")
            for info in db_info["additional_info"]:
                result.append(f"- {info}")
                
        if "error" in db_info:
            result.append(f"\nError: {db_info['error']}")
            
        return wrap_untrusted("\n".join(result))
    except Exception as e:
        return wrap_untrusted(f"Error retrieving database vendor information for '{database_name}': {str(e)}")

@mcp.tool()
async def search_columns(database_name: str, search_term: str, ctx: Context) -> str:
    """Find columns (substring match) and list hosting tables (limit 50).

    Use: Discover where a data attribute lives (e.g. customer_id).
    Compose: Narrow candidate tables before calling per-table tools.
    Avoid: Full structural profiling (use get_table_schema + constraints instead).
    
    Args:
        database_name: Name of the database to search
        search_term: Pattern to search for in column names
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        matching_columns = await db_context.search_columns(search_term, limit=50)
        
        if not matching_columns:
            return wrap_untrusted(f"No columns found in '{database_name}' matching '{search_term}'")
        
        results = [f"Found columns matching '{search_term}' in {len(matching_columns)} tables in '{database_name}':"]
        
        for table_name, columns in matching_columns.items():
            results.append(f"\nTable: {table_name}")
            results.append("Matching columns:")
            for col in columns:
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                results.append(f"  - {col['name']}: {col['type']} {nullable}")
        
        return wrap_untrusted("\n".join(results))
    except Exception as e:
        return wrap_untrusted(f"Error searching columns in '{database_name}': {str(e)}")

@mcp.tool()
async def get_pl_sql_objects(database_name: str, object_type: str, name_pattern: Optional[str], ctx: Context) -> str:
    """List PL/SQL objects (procedures/functions/packages/etc) by type/pattern.

    Use: Inventory logic surface / candidate impact analysis.
    Compose: Follow with get_object_source or get_dependent_objects.
    Avoid: Counting table dependencies (use get_dependent_objects on the table).
    
    Args:
        database_name: Name of the database to query
        object_type: Type of object (PROCEDURE, FUNCTION, PACKAGE, TRIGGER, etc.)
        name_pattern: Optional LIKE pattern for object names
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        objects = await db_context.get_pl_sql_objects(object_type.upper(), name_pattern)
        
        if not objects:
            pattern_msg = f" matching '{name_pattern}'" if name_pattern else ""
            return wrap_untrusted(f"No {object_type.upper()} objects found in '{database_name}'{pattern_msg}")
        
        results = [f"Found {len(objects)} {object_type.upper()} objects:"]
        
        for obj in objects:
            results.append(f"\n{obj['type']}: {obj['name']}")
            if 'owner' in obj:
                results.append(f"Owner: {obj['owner']}")
            if 'status' in obj:
                results.append(f"Status: {obj['status']}")
            if 'created' in obj:
                results.append(f"Created: {obj['created']}")
            if 'last_modified' in obj:
                results.append(f"Last Modified: {obj['last_modified']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving PL/SQL objects: {str(e)}"

@mcp.tool()
async def get_object_source(database_name: str, object_type: str, object_name: str, ctx: Context) -> str:
    """Retrieve full DDL/source text for a single PL/SQL object.

    Use: Deep dive / debugging after identifying object via get_pl_sql_objects.
    Compose: Pair with dependency info for refactor planning.
    Avoid: Bulk enumeration (fetch only what you need).
    
    Args:
        database_name: Name of the database to query
        object_type: Type of object (PROCEDURE, FUNCTION, PACKAGE, etc.)
        object_name: Name of the object
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        source = await db_context.get_object_source(object_type.upper(), object_name.upper())
        
        if not source:
            return wrap_untrusted(f"No source found for {object_type} {object_name} in database '{database_name}'")
        
        return wrap_untrusted(f"Source for {object_type} {object_name} in '{database_name}':\n\n{source}")
    except Exception as e:
        return wrap_untrusted(f"Error retrieving object source from '{database_name}': {str(e)}")

@mcp.tool()
async def get_table_constraints(database_name: str, table_name: str, ctx: Context) -> str:
    """List PK / FK / UNIQUE / CHECK constraints for one table (cached TTL).

    Use: Relationship + integrity analysis; ranking features (FK counts, PK presence).
    Compose: With get_related_tables (quick FK direction) + get_table_indexes.
    Avoid: Manually parsing schema text for constraints elsewhere.
    
    Args:
        database_name: Name of the database to query
        table_name: Name of the table
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        constraints = await db_context.get_table_constraints(table_name)
        
        if not constraints:
            return wrap_untrusted(f"No constraints found for table '{table_name}' in database '{database_name}'")
        
        results = [f"Constraints for table '{table_name}':"]
        
        for constraint in constraints:
            constraint_type = constraint.get('type', 'UNKNOWN')
            name = constraint.get('name', 'UNNAMED')
            
            results.append(f"\n{constraint_type} Constraint: {name}")
            
            if 'columns' in constraint:
                results.append(f"Columns: {', '.join(constraint['columns'])}")
                
            if constraint_type == 'FOREIGN KEY' and 'references' in constraint:
                ref = constraint['references']
                results.append(f"References: {ref['table']}({', '.join(ref['columns'])})")
                
            if 'condition' in constraint:
                results.append(f"Condition: {constraint['condition']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving constraints: {str(e)}"

@mcp.tool()
async def get_table_indexes(database_name: str, table_name: str, ctx: Context) -> str:
    """Enumerate indexes (name, columns, uniqueness, status) for a table.

    Use: Performance hints + structural importance (index density, unique keys).
    Compose: With get_table_constraints (PK/UK) + get_dependent_objects.
    Avoid: Calling just to re-learn column order (columns via get_table_schema).
    
    Args:
        database_name: Name of the database to query
        table_name: Name of the table
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        indexes = await db_context.get_table_indexes(table_name)
        
        if not indexes:
            return wrap_untrusted(f"No indexes found for table '{table_name}' in database '{database_name}'")
        
        results = [f"Indexes for table '{table_name}':"]
        
        for idx in indexes:
            idx_type = "UNIQUE " if idx.get('unique', False) else ""
            results.append(f"\n{idx_type}Index: {idx['name']}")
            results.append(f"Columns: {', '.join(idx['columns'])}")
            
            if 'tablespace' in idx:
                results.append(f"Tablespace: {idx['tablespace']}")
                
            if 'status' in idx:
                results.append(f"Status: {idx['status']}")
        
        return "\n".join(results)
    except Exception as e:
        return f"Error retrieving indexes: {str(e)}"

@mcp.tool()
async def get_dependent_objects(database_name: str, object_name: str, ctx: Context) -> str:
    """List objects (views / PL/SQL / triggers) depending on a table/object.

    Use: Impact analysis & centrality (importance scoring dimension).
    Compose: Combine counts with FK + index metrics for ranking.
    Avoid: Running on every table blindly—filter candidates first.
    
    Args:
        database_name: Name of the database to query
        object_name: Name of the object
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        dependencies = await db_context.get_dependent_objects(object_name.upper())
        
        if not dependencies:
            return wrap_untrusted(f"No objects found that depend on '{object_name}' in database '{database_name}'")
        
        results = [f"Objects that depend on '{object_name}' in '{database_name}':"]
        
        for dep in dependencies:
            results.append(f"\n{dep['type']}: {dep['name']}")
            if 'owner' in dep:
                results.append(f"Owner: {dep['owner']}")
        
        return wrap_untrusted("\n".join(results))
    except Exception as e:
        return wrap_untrusted(f"Error retrieving dependencies from '{database_name}': {str(e)}")

@mcp.tool()
async def get_user_defined_types(database_name: str, type_pattern: Optional[str], ctx: Context) -> str:
    """List user-defined types (+ attributes for OBJECT types).

    Use: Understand custom data modeling / complexity hotspots.
    Compose: Only include in importance scoring if type coupling matters.
    Avoid: Treating as a substitute for table relationship analysis.
    
    Args:
        database_name: Name of the database to query
        type_pattern: Optional LIKE pattern for type names
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        types = await db_context.get_user_defined_types(type_pattern)
        
        if not types:
            pattern_msg = f" matching '{type_pattern}'" if type_pattern else ""
            return wrap_untrusted(f"No user-defined types found in '{database_name}'{pattern_msg}")
        
        results = [f"User-defined types in '{database_name}':"]
        
        for typ in types:
            results.append(f"\nType: {typ['name']}")
            results.append(f"Type category: {typ['type_category']}")
            if 'owner' in typ:
                results.append(f"Owner: {typ['owner']}")
            if 'attributes' in typ and typ['attributes']:
                results.append("Attributes:")
                for attr in typ['attributes']:
                    results.append(f"  - {attr['name']}: {attr['type']}")
        
        return wrap_untrusted("\n".join(results))
    except Exception as e:
        return wrap_untrusted(f"Error retrieving user-defined types from '{database_name}': {str(e)}")

@mcp.tool()
async def get_related_tables(database_name: str, table_name: str, ctx: Context) -> str:
    """FK in/out adjacency for one table (incoming + outgoing lists, cached TTL).

    Use: Quick centrality signals (in-degree/out-degree) for ranking & join design.
    Compose: With get_table_constraints (details) + get_dependent_objects (broader usage).
    Avoid: Deriving FK direction manually from constraints output.
    
    Args:
        database_name: Name of the database to query
        table_name: Name of the table
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        related = await db_context.get_related_tables(table_name)
        
        if not related['referenced_tables'] and not related['referencing_tables']:
            return wrap_untrusted(f"No related tables found for '{table_name}' in database '{database_name}'")
        
        results = [f"Tables related to '{table_name}' in '{database_name}':"]
        
        if related['referenced_tables']:
            results.append("\nTables referenced by this table (outgoing foreign keys):")
            for table in related['referenced_tables']:
                results.append(f"  - {table}")
        
        if related['referencing_tables']:
            results.append("\nTables that reference this table (incoming foreign keys):")
            for table in related['referencing_tables']:
                results.append(f"  - {table}")
        
        return wrap_untrusted("\n".join(results))
        
    except Exception as e:
        return wrap_untrusted(f"Error getting related tables from '{database_name}': {str(e)}")

@mcp.tool()
async def run_sql_query(database_name: str, sql: str, ctx: Context, max_rows: int = 100) -> str:
    """Execute SQL queries (SELECT for reading data, DML/DDL when write mode enabled).

    This tool supports:
    - SELECT queries: Returns formatted result set with columns and rows
    - INSERT/UPDATE/DELETE: Executes and returns affected row count (requires write mode)
    - DDL statements: CREATE, ALTER, DROP, etc. (requires write mode)
    
    Use: Ad hoc data inspection, data modification, or schema changes.
    Compose: Supplement structured metadata tools (e.g. row counts) sparingly.
    Avoid: Rebuilding metadata graphs already available via dedicated tools.
    
    Args:
        database_name: Name of the database to query
        sql: SQL statement to execute (SELECT, INSERT, UPDATE, DELETE, DDL, etc.)
        max_rows: Maximum number of rows to return for SELECT queries (default: 100)
    
    Note: 
    - In read-only mode (default), only SELECT statements are permitted.
    - Write operations (INSERT, UPDATE, DELETE, DDL) require write mode to be enabled.
    - For write operations, changes are automatically committed.
    
    Examples:
    - SELECT: "SELECT * FROM employees WHERE department_id = 10"
    - INSERT: "INSERT INTO departments (department_id, department_name) VALUES (300, 'AI Research')"
    - UPDATE: "UPDATE employees SET salary = salary * 1.1 WHERE job_id = 'IT_PROG'"
    - DELETE: "DELETE FROM job_history WHERE end_date < DATE '2020-01-01'"
    - DDL: "CREATE TABLE temp_data (id NUMBER, name VARCHAR2(100))"
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        result = await db_context.run_sql_query(sql, max_rows=max_rows)
        
        # Handle different result types
        if not result.get("rows"):
            # Write operation or empty read response
            if "message" in result:
                return wrap_untrusted(f"[{database_name}] {result['message']}")
            return wrap_untrusted(f"[{database_name}] Query executed successfully, but returned no rows.")
        
        # Format SELECT results
        formatted_result = format_sql_query_result(result)
        return wrap_untrusted(f"Results from '{database_name}':\n\n{formatted_result}")
        
    except PermissionError as e:
        return wrap_untrusted(f"[{database_name}] Permission error: {e}\n\nHint: Write operations require the database to be configured with read_only=False.")
    except oracledb.Error as e:
        error_msg = str(e)
        return wrap_untrusted(f"[{database_name}] Database error: {error_msg}")
    except Exception as e:
        return wrap_untrusted(f"[{database_name}] Unexpected error executing query: {e}")

@mcp.tool()
async def execute_write_sql(database_name: str, sql: str, ctx: Context) -> str:
    """Execute write operations (INSERT, UPDATE, DELETE, DDL) with auto-commit.
    
    This tool is specifically designed for data modification and schema changes:
    - INSERT: Add new rows to tables
    - UPDATE: Modify existing rows
    - DELETE: Remove rows
    - DDL: CREATE, ALTER, DROP, TRUNCATE tables/indexes/etc.
    
    Use: When you need to modify data or schema in the database.
    Caution: Changes are automatically committed and cannot be rolled back.
    
    Args:
        database_name: Name of the database to modify
        sql: DML or DDL statement to execute (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, etc.)
    
    Requirements:
    - Database must be configured with write mode enabled (read_only=False)
    - User must have appropriate privileges for the operation
    
    Returns:
    - For DML: Number of rows affected
    - For DDL: Success message
    
    Examples:
    - "INSERT INTO employees (employee_id, first_name, last_name, email, hire_date, job_id) 
       VALUES (500, 'John', 'Doe', 'JDOE', SYSDATE, 'IT_PROG')"
    - "UPDATE employees SET salary = 75000 WHERE employee_id = 500"
    - "DELETE FROM employees WHERE employee_id = 500"
    - "CREATE INDEX emp_email_idx ON employees(email)"
    - "ALTER TABLE employees ADD (middle_name VARCHAR2(20))"
    
    Note: This is the same as run_sql_query but with clearer intent for write operations.
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        # Validate it's not a SELECT (use run_sql_query for that)
        sql_upper = sql.strip().upper()
        if sql_upper.startswith('SELECT') or sql_upper.startswith('WITH'):
            return wrap_untrusted(
                f"[{database_name}] Use 'run_sql_query' tool for SELECT statements. "
                "This tool is for write operations only (INSERT, UPDATE, DELETE, DDL)."
            )
        
        result = await db_context.run_sql_query(sql, max_rows=0)
        
        if "message" in result:
            return wrap_untrusted(f"[{database_name}] ✓ {result['message']}")
        
        row_count = result.get("row_count", 0)
        return wrap_untrusted(
            f"[{database_name}] ✓ Statement executed successfully. {row_count} row(s) affected."
        )
        
    except PermissionError as e:
        return wrap_untrusted(
            f"[{database_name}] ✗ Permission denied: {e}\n\n"
            "Write operations require the database to be configured with read_only=False in mcp.json.\n"
            "Contact your database administrator to enable write mode if needed."
        )
    except oracledb.Error as e:
        error_code = getattr(e.args[0] if e.args else None, 'code', 'Unknown')
        error_msg = str(e)
        return wrap_untrusted(
            f"[{database_name}] ✗ Database error (ORA-{error_code}): {error_msg}\n\n"
            "Common issues:\n"
            "- Insufficient privileges\n"
            "- Constraint violation (foreign key, unique, check)\n"
            "- Object already exists or doesn't exist\n"
            "- Invalid SQL syntax"
        )
    except Exception as e:
        return wrap_untrusted(f"[{database_name}] ✗ Unexpected error: {e}")

@mcp.tool()
async def explain_query_plan(database_name: str, sql: str, ctx: Context) -> str:
    """Obtain an execution plan for a SELECT/CTE.

    Read-only mode note: The underlying Oracle mechanism uses PLAN_TABLE and
    cleanup (DELETE) statements; on locked-down or strictly read-only accounts
    this may fail. If so, a structured error is returned instead of a plan.

    Use: Pre-flight cost / access path inspection before running large queries.
    Avoid: High frequency calls in loops.
    
    Args:
        database_name: Name of the database to query
        sql: SQL query to analyze
    """
    db_context = validate_database(ctx, database_name)
    try:
        plan = await db_context.explain_query_plan(sql)
        # Standardize error wrapping
        if plan.get("error"):
            return wrap_untrusted(f"[{database_name}] Explain plan unavailable: {plan['error']}")
        if not plan.get("execution_plan"):
            return wrap_untrusted(f"[{database_name}] No execution plan rows returned.")
        lines = [f"Execution Plan for '{database_name}':"] + [f"  {step}" for step in plan["execution_plan"]]
        if plan.get("optimization_suggestions"):
            lines.append("\nSuggestions:")
            for s in plan["optimization_suggestions"]:
                lines.append(f"  - {s}")
        return wrap_untrusted("\n".join(lines))
    except PermissionError as e:
        return wrap_untrusted(f"[{database_name}] Permission error: {e}")
    except oracledb.Error as e:
        return wrap_untrusted(f"[{database_name}] Database error obtaining plan: {e}")
    except Exception as e:
        return wrap_untrusted(f"[{database_name}] Unexpected error obtaining plan: {e}")

@mcp.tool()
async def generate_sample_queries(database_name: str, ctx: Context) -> str:
    """Generate 10 sample SQL queries from beginner to advanced based on database schema.
    
    Creates contextual, runnable SQL queries using actual tables, columns, and relationships
    from your database. Great for learning SQL, exploring data, and understanding query patterns.
    
    Use: Learning SQL, exploring database structure, getting query examples.
    Compose: Use with run_sql_query to execute the generated queries.
    
    Args:
        database_name: Name of the database to generate queries for
    
    Returns:
        10 sample queries ranging from simple SELECT to complex analytics:
        - Beginner: Simple SELECT, filtering, sorting
        - Intermediate: Joins, aggregations, GROUP BY
        - Advanced: Subqueries, window functions, complex analytics
    """
    db_context = validate_database(ctx, database_name)
    
    try:
        # Get available tables
        all_tables = await db_context.list_tables()
        
        if not all_tables or len(all_tables) == 0:
            return wrap_untrusted(f"[{database_name}] No tables found in database.")
        
        # Load metadata for first few tables to generate queries
        sample_queries = []
        query_num = 1
        
        # Get metadata for up to 5 tables to generate diverse queries
        tables_to_analyze = all_tables[:min(5, len(all_tables))]
        table_metadata = []
        
        for table_name in tables_to_analyze:
            try:
                table_info = await db_context.get_schema_info(table_name)
                if table_info and table_info.columns:
                    table_metadata.append(table_info)
            except Exception as e:
                print(f"Could not load metadata for {table_name}: {e}", file=sys.stderr)
        
        if not table_metadata:
            return wrap_untrusted(f"[{database_name}] Could not load table metadata for query generation.")
        
        # Helper function to get column names by type
        def get_columns_by_type(table, types):
            return [col['name'] for col in table.columns if any(t in col.get('type', '').upper() for t in types)]
        
        # Get primary table for examples
        primary_table = table_metadata[0]
        table_name = primary_table.table_name
        
        # Get different column types
        all_columns = [col['name'] for col in primary_table.columns]
        numeric_cols = get_columns_by_type(primary_table, ['NUMBER', 'INT', 'FLOAT', 'DECIMAL'])
        text_cols = get_columns_by_type(primary_table, ['VARCHAR', 'CHAR', 'CLOB', 'TEXT'])
        date_cols = get_columns_by_type(primary_table, ['DATE', 'TIMESTAMP'])
        
        # ===== BEGINNER QUERIES =====
        
        # 1. Simple SELECT - all columns
        sample_queries.append({
            'level': 'Beginner',
            'number': query_num,
            'title': 'Select All Records',
            'description': 'Retrieve all columns and rows from a table (limited to 10)',
            'query': f"SELECT * FROM {table_name} WHERE ROWNUM <= 10"
        })
        query_num += 1
        
        # 2. SELECT specific columns
        cols_to_select = all_columns[:min(3, len(all_columns))]
        sample_queries.append({
            'level': 'Beginner',
            'number': query_num,
            'title': 'Select Specific Columns',
            'description': 'Retrieve only specific columns',
            'query': f"SELECT {', '.join(cols_to_select)} FROM {table_name}"
        })
        query_num += 1
        
        # 3. WHERE clause with condition
        if numeric_cols:
            sample_queries.append({
                'level': 'Beginner',
                'number': query_num,
                'title': 'Filter with WHERE Clause',
                'description': 'Filter records based on a numeric condition',
                'query': f"SELECT * FROM {table_name} WHERE {numeric_cols[0]} > 0"
            })
        elif text_cols:
            sample_queries.append({
                'level': 'Beginner',
                'number': query_num,
                'title': 'Filter with WHERE Clause',
                'description': 'Filter records based on a text condition',
                'query': f"SELECT * FROM {table_name} WHERE {text_cols[0]} IS NOT NULL"
            })
        else:
            sample_queries.append({
                'level': 'Beginner',
                'number': query_num,
                'title': 'Filter with WHERE Clause',
                'description': 'Filter records with ROWNUM',
                'query': f"SELECT * FROM {table_name} WHERE ROWNUM <= 5"
            })
        query_num += 1
        
        # 4. ORDER BY
        sort_col = all_columns[0]
        sample_queries.append({
            'level': 'Beginner',
            'number': query_num,
            'title': 'Sort Results',
            'description': 'Order results by a specific column',
            'query': f"SELECT * FROM {table_name} ORDER BY {sort_col} DESC"
        })
        query_num += 1
        
        # ===== INTERMEDIATE QUERIES =====
        
        # 5. COUNT and aggregation
        sample_queries.append({
            'level': 'Intermediate',
            'number': query_num,
            'title': 'Count Records',
            'description': 'Count total number of records in table',
            'query': f"SELECT COUNT(*) AS total_records FROM {table_name}"
        })
        query_num += 1
        
        # 6. GROUP BY with aggregate
        if numeric_cols and len(all_columns) > 1:
            group_col = [c for c in all_columns if c not in numeric_cols[:1]][0] if [c for c in all_columns if c not in numeric_cols[:1]] else all_columns[0]
            sample_queries.append({
                'level': 'Intermediate',
                'number': query_num,
                'title': 'Group and Aggregate',
                'description': 'Group records and calculate aggregates',
                'query': f"SELECT {group_col}, COUNT(*) AS count, AVG({numeric_cols[0]}) AS avg_value\nFROM {table_name}\nGROUP BY {group_col}\nORDER BY count DESC"
            })
        else:
            sample_queries.append({
                'level': 'Intermediate',
                'number': query_num,
                'title': 'Group and Count',
                'description': 'Group records and count',
                'query': f"SELECT {all_columns[0]}, COUNT(*) AS count\nFROM {table_name}\nGROUP BY {all_columns[0]}\nORDER BY count DESC"
            })
        query_num += 1
        
        # 7. JOIN query (if relationships exist)
        if primary_table.relationships and len(table_metadata) > 1:
            # Find a related table
            related_table_name = None
            join_condition = None
            
            for rel_table, rels in primary_table.relationships.items():
                if rel_table in [t.table_name for t in table_metadata]:
                    related_table_name = rel_table
                    rel_info = rels[0]
                    join_condition = f"{table_name}.{rel_info['local_column']} = {related_table_name}.{rel_info['foreign_column']}"
                    break
            
            if related_table_name and join_condition:
                sample_queries.append({
                    'level': 'Intermediate',
                    'number': query_num,
                    'title': 'Join Two Tables',
                    'description': f'Join {table_name} with {related_table_name}',
                    'query': f"SELECT t1.*, t2.*\nFROM {table_name} t1\nINNER JOIN {related_table_name} t2 ON {join_condition}\nWHERE ROWNUM <= 10"
                })
            else:
                # Fallback: DISTINCT query
                sample_queries.append({
                    'level': 'Intermediate',
                    'number': query_num,
                    'title': 'Find Distinct Values',
                    'description': 'Get unique values from a column',
                    'query': f"SELECT DISTINCT {all_columns[0]} FROM {table_name} ORDER BY {all_columns[0]}"
                })
        else:
            sample_queries.append({
                'level': 'Intermediate',
                'number': query_num,
                'title': 'Find Distinct Values',
                'description': 'Get unique values from a column',
                'query': f"SELECT DISTINCT {all_columns[0]} FROM {table_name} ORDER BY {all_columns[0]}"
            })
        query_num += 1
        
        # ===== ADVANCED QUERIES =====
        
        # 8. Subquery
        if numeric_cols:
            sample_queries.append({
                'level': 'Advanced',
                'number': query_num,
                'title': 'Subquery - Above Average',
                'description': 'Find records with values above average',
                'query': f"SELECT * FROM {table_name}\nWHERE {numeric_cols[0]} > (SELECT AVG({numeric_cols[0]}) FROM {table_name})"
            })
        else:
            sample_queries.append({
                'level': 'Advanced',
                'number': query_num,
                'title': 'Subquery - IN clause',
                'description': 'Use subquery in IN clause',
                'query': f"SELECT * FROM {table_name}\nWHERE {all_columns[0]} IN (SELECT {all_columns[0]} FROM {table_name} WHERE ROWNUM <= 5)"
            })
        query_num += 1
        
        # 9. Window function (if numeric column exists)
        if numeric_cols and len(all_columns) > 1:
            partition_col = [c for c in all_columns if c != numeric_cols[0]][0]
            sample_queries.append({
                'level': 'Advanced',
                'number': query_num,
                'title': 'Window Function - Ranking',
                'description': 'Use window functions for ranking and analytics',
                'query': f"SELECT {', '.join(all_columns[:3])},\n       ROW_NUMBER() OVER (PARTITION BY {partition_col} ORDER BY {numeric_cols[0]} DESC) AS rank\nFROM {table_name}"
            })
        else:
            sample_queries.append({
                'level': 'Advanced',
                'number': query_num,
                'title': 'Window Function - Row Numbers',
                'description': 'Add row numbers to results',
                'query': f"SELECT {', '.join(all_columns[:3])},\n       ROW_NUMBER() OVER (ORDER BY {all_columns[0]}) AS row_num\nFROM {table_name}"
            })
        query_num += 1
        
        # 10. Complex analytical query
        if numeric_cols and date_cols:
            sample_queries.append({
                'level': 'Advanced',
                'number': query_num,
                'title': 'Complex Analytics with Multiple Aggregates',
                'description': 'Combine multiple aggregate functions and date functions',
                'query': f"SELECT \n    TRUNC({date_cols[0]}) AS date_group,\n    COUNT(*) AS total_count,\n    MIN({numeric_cols[0]}) AS min_value,\n    MAX({numeric_cols[0]}) AS max_value,\n    AVG({numeric_cols[0]}) AS avg_value,\n    STDDEV({numeric_cols[0]}) AS std_dev\nFROM {table_name}\nGROUP BY TRUNC({date_cols[0]})\nORDER BY date_group DESC"
            })
        elif numeric_cols:
            sample_queries.append({
                'level': 'Advanced',
                'number': query_num,
                'title': 'Statistical Analysis',
                'description': 'Calculate statistical measures',
                'query': f"SELECT \n    COUNT(*) AS total_records,\n    MIN({numeric_cols[0]}) AS min_value,\n    MAX({numeric_cols[0]}) AS max_value,\n    AVG({numeric_cols[0]}) AS avg_value,\n    MEDIAN({numeric_cols[0]}) AS median_value,\n    STDDEV({numeric_cols[0]}) AS std_deviation\nFROM {table_name}"
            })
        else:
            # Fallback: Complex join with aggregation
            if len(table_metadata) > 1:
                table2 = table_metadata[1]
                sample_queries.append({
                    'level': 'Advanced',
                    'number': query_num,
                    'title': 'Complex Multi-Table Analysis',
                    'description': f'Analyze data across {table_name} and {table2.table_name}',
                    'query': f"SELECT t1.{all_columns[0]}, COUNT(t2.{table2.columns[0]['name']}) AS related_count\nFROM {table_name} t1\nLEFT JOIN {table2.table_name} t2 ON t1.{all_columns[0]} = t2.{table2.columns[0]['name']}\nGROUP BY t1.{all_columns[0]}\nHAVING COUNT(t2.{table2.columns[0]['name']}) > 0\nORDER BY related_count DESC"
                })
            else:
                sample_queries.append({
                    'level': 'Advanced',
                    'number': query_num,
                    'title': 'Self-Join Analysis',
                    'description': 'Use self-join for comparative analysis',
                    'query': f"SELECT DISTINCT t1.{all_columns[0]}, t2.{all_columns[0]} AS related_{all_columns[0]}\nFROM {table_name} t1\nINNER JOIN {table_name} t2 ON t1.{all_columns[0]} < t2.{all_columns[0]}\nWHERE ROWNUM <= 10"
                })
        
        # Format output
        output = [f"📊 Sample SQL Queries for '{database_name}' Database\n"]
        output.append("=" * 80 + "\n")
        output.append(f"Generated {len(sample_queries)} queries based on your database schema.\n")
        output.append("These queries use actual tables and columns from your database.\n\n")
        
        current_level = None
        for query in sample_queries:
            if query['level'] != current_level:
                current_level = query['level']
                output.append(f"\n{'='*80}\n")
                output.append(f"🎯 {current_level.upper()} LEVEL\n")
                output.append(f"{'='*80}\n\n")
            
            output.append(f"Query #{query['number']}: {query['title']}\n")
            output.append(f"Description: {query['description']}\n")
            output.append(f"\n```sql\n{query['query']}\n```\n")
            output.append(f"\n💡 To run this query: run_sql_query(database_name=\"{database_name}\", sql=\"{query['query'].replace(chr(10), ' ')}\")\n")
            output.append(f"{'-'*80}\n\n")
        
        output.append(f"\n{'='*80}\n")
        output.append("📚 Learning Tips:\n")
        output.append("- Start with beginner queries to understand basic SELECT operations\n")
        output.append("- Practice intermediate queries to learn JOINs and aggregations\n")
        output.append("- Challenge yourself with advanced queries for analytics\n")
        output.append("- Modify these queries to explore different aspects of your data\n")
        output.append("- Use explain_query_plan to understand query performance\n")
        
        return wrap_untrusted("".join(output))
        
    except Exception as e:
        return wrap_untrusted(f"[{database_name}] Error generating sample queries: {str(e)}")


@mcp.tool()
async def generate_sample_dq_rules(database_name: str, table_name: str, ctx: Context, num_rules: int = 10) -> str:
    """Generate sample data quality (DQ) rules based on table schema and column metadata.
    
    Creates contextual data quality validation rules using actual table columns and their 
    data types. Rules span multiple categories including Business Entity Rules, Business
    Attribute Rules, Data Dependency Rules, and Data Validity Rules.
    
    Use: Data quality assessment, validation rule design, DQ framework setup.
    Compose: Use with run_sql_query to validate data against generated rules.
    
    Args:
        database_name: Name of the database to analyze
        table_name: Name of the table to generate DQ rules for
        num_rules: Number of rules to generate (default: 10, max: 50)
    
    Returns:
        JSON structure with rule_set_id and array of rules organized by categories:
        
        BUSINESS ENTITY RULES (ensure core business objects are well-defined):
        - entity_uniqueness: Every entity must be uniquely identifiable (no duplicate records)
        - cardinality: Defines relationship constraints (one-to-many, many-to-many)
        - optionality: Mandatory vs optional relationship enforcement
        
        BUSINESS ATTRIBUTE RULES (individual data element validation):
        - data_inheritance: Attributes consistent across subtypes
        - data_domain: Values conform to allowed formats/ranges (state codes, age ranges)
        - format: Pattern validation (email, phone, date formats)
        - value_in_list: Allowed values validation
        
        DATA DEPENDENCY RULES (logical/conditional relationships):
        - entity_relationship_dependency: Existence depends on conditions
        - attribute_dependency: Value depends on other attributes
        - conditional_mandatory: Dependent field requirements
        - cross_field_validation: Multi-column logical checks
        
        DATA VALIDITY RULES (ensure data is trustworthy):
        - completeness: Required records, relationships, attributes must exist (mandatory)
        - correctness_accuracy: Values reflect real-world truth
        - precision: Data stored with required detail level
        - uniqueness: No duplicate records, keys, or overloaded columns
        - consistency: Duplicate/redundant data must match everywhere
        - compliance: PII and sensitive data validation (SSN, credit card, passport)
        - freshness: Confirms date values are up to date
        - data_type_match: Values match their data type requirements
        - duplicate_rows: Multi-column duplicate detection
        - empty_blank: Null/empty/whitespace checks
        - table_lookup: Referential integrity (value exists in lookup table)
        - expression: Range and cross-column checks
        - custom: Flexible custom business logic rules
    
    Example output:
        {
            "rule_set_id": "dq_customers",
            "rule_category_summary": {
                "business_entity_rules": 3,
                "business_attribute_rules": 5,
                "data_dependency_rules": 4,
                "data_validity_rules": 8
            },
            "rules": [
                {"rule_id": "r_entity_uniqueness_customer_id", "rule_type": "entity_uniqueness", "category": "business_entity", ...},
                {"rule_id": "r_data_domain_state_code", "rule_type": "data_domain", "category": "business_attribute", ...},
                {"rule_id": "r_attribute_dependency_loan", "rule_type": "attribute_dependency", "category": "data_dependency", ...},
                {"rule_id": "r_compliance_ssn", "rule_type": "compliance", "category": "data_validity", ...},
                ...
            ]
        }
    """
    import random
    
    db_context = validate_database(ctx, database_name)
    
    try:
        # Limit rules to reasonable range (increased to 50 for comprehensive rule sets)
        num_rules = max(1, min(num_rules, 50))
        
        # Get table schema info
        table_info = await db_context.get_schema_info(table_name)
        
        if not table_info:
            return wrap_untrusted(f"[{database_name}] Table '{table_name}' not found.")
        
        if not table_info.columns:
            return wrap_untrusted(f"[{database_name}] No columns found for table '{table_name}'.")
        
        columns = table_info.columns
        
        # Helper function to categorize columns by type
        def get_columns_by_type(columns, type_patterns):
            return [col for col in columns if any(t in col.get('type', '').upper() for t in type_patterns)]
        
        # Categorize columns
        numeric_cols = get_columns_by_type(columns, ['NUMBER', 'INT', 'FLOAT', 'DECIMAL', 'NUMERIC'])
        text_cols = get_columns_by_type(columns, ['VARCHAR', 'CHAR', 'CLOB', 'TEXT', 'NVARCHAR', 'NCHAR'])
        date_cols = get_columns_by_type(columns, ['DATE', 'TIMESTAMP'])
        all_cols = columns
        
        # Find specific column patterns
        email_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['EMAIL', 'MAIL'])]
        phone_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['PHONE', 'MOBILE', 'TEL', 'FAX'])]
        name_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['NAME', 'FIRST', 'LAST', 'MIDDLE'])]
        id_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['ID', 'CODE', 'KEY', 'NUM'])]
        status_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['STATUS', 'STATE', 'TYPE', 'FLAG', 'CATEGORY'])]
        amount_cols = [col for col in numeric_cols if any(k in col['name'].upper() for k in ['AMOUNT', 'PRICE', 'COST', 'INCOME', 'SALARY', 'BALANCE', 'TOTAL'])]
        
        # Additional column patterns for comprehensive DQ rules
        address_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['ADDRESS', 'ADDR', 'STREET', 'CITY', 'STATE', 'ZIP', 'POSTAL', 'COUNTRY'])]
        state_code_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['STATE_CODE', 'STATE_CD', 'COUNTRY_CODE', 'COUNTRY_CD'])]
        age_cols = [col for col in numeric_cols if any(k in col['name'].upper() for k in ['AGE', 'YEARS'])]
        rate_cols = [col for col in numeric_cols if any(k in col['name'].upper() for k in ['RATE', 'PERCENT', 'PCT', 'RATIO', 'INTEREST', 'COMMISSION'])]
        account_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['ACCOUNT', 'ACCT'])]
        loan_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['LOAN', 'CREDIT', 'DEBT', 'MORTGAGE'])]
        customer_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['CUSTOMER', 'CUST', 'CLIENT'])]
        pii_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['SSN', 'PASSPORT', 'NATIONAL_ID', 'TAX_ID', 'CREDIT_CARD', 'BANK_ACCOUNT', 'CVV', 'EXPIRY'])]
        fk_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['_ID', '_KEY', '_REF', '_FK', 'PARENT_', 'FOREIGN_'])]
        
        # Build rule set
        rule_set_id = f"dq_{table_name.lower()}"
        rules = []
        rule_counter = 1
        
        # Define rule templates
        rule_templates = []
        
        # 1. Mandatory rules for important-looking columns
        mandatory_candidates = name_cols + id_cols + email_cols
        for col in mandatory_candidates[:3]:
            rule_templates.append({
                "rule_id": f"r_mandatory_{col['name'].lower()}",
                "rule_type": "mandatory",
                "target_columns": [col['name']],
                "enabled": True
            })
        
        # 2. Format rules for email columns
        for col in email_cols[:2]:
            rule_templates.append({
                "rule_id": f"r_format_{col['name'].lower()}",
                "rule_type": "format",
                "target_columns": [col['name']],
                "params": {"pattern": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"},
                "enabled": True
            })
        
        # 3. Format rules for phone columns
        for col in phone_cols[:2]:
            rule_templates.append({
                "rule_id": f"r_format_{col['name'].lower()}",
                "rule_type": "format",
                "target_columns": [col['name']],
                "params": {"pattern": "^[+]?[0-9]{10,15}$"},
                "enabled": True
            })
        
        # 4. Uniqueness rules for ID/key columns and emails
        unique_candidates = id_cols[:2] + email_cols[:1]
        for col in unique_candidates:
            rule_templates.append({
                "rule_id": f"r_unique_{col['name'].lower()}",
                "rule_type": "uniqueness",
                "target_columns": [col['name']],
                "params": {"scope": "table"},
                "enabled": True
            })
        
        # 5. Value in list rules for status/type columns
        sample_values_map = {
            'STATUS': ['Active', 'Inactive', 'Pending', 'Suspended', 'Closed'],
            'STATE': ['Open', 'Closed', 'In Progress', 'Completed', 'Cancelled'],
            'TYPE': ['Standard', 'Premium', 'Basic', 'Enterprise', 'Trial'],
            'CATEGORY': ['Category A', 'Category B', 'Category C', 'Other'],
            'FLAG': ['Y', 'N'],
            'GENDER': ['M', 'F', 'O'],
            'RESIDENCY': ['Resident', 'Non-Resident', 'ROR', 'NRI'],
            'OCCUPATION': ['Engineer', 'Doctor', 'Teacher', 'Business', 'Student', 'Other']
        }
        
        for col in status_cols[:3]:
            col_upper = col['name'].upper()
            # Find matching sample values
            sample_values = ['Value1', 'Value2', 'Value3', 'Value4', 'Value5']
            for key, values in sample_values_map.items():
                if key in col_upper:
                    sample_values = values
                    break
            
            rule_templates.append({
                "rule_id": f"r_value_in_list_{col['name'].lower()}",
                "rule_type": "value_in_list",
                "target_columns": [col['name']],
                "params": {"allowed_values": sample_values},
                "enabled": True
            })
        
        # 6. Expression rules for numeric range checks
        for col in amount_cols[:2]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_range_{col_name.lower()}",
                "rule_type": "expression",
                "target_columns": [col_name],
                "params": {"expression": f"{col_name} BETWEEN 0 AND 10000000"},
                "enabled": True
            })
        
        # 7. Expression rules for non-negative numeric values
        for col in numeric_cols[:2]:
            if col not in amount_cols:
                col_name = col['name']
                rule_templates.append({
                    "rule_id": f"r_positive_{col_name.lower()}",
                    "rule_type": "expression",
                    "target_columns": [col_name],
                    "params": {"expression": f"{col_name} >= 0"},
                    "enabled": True
                })
        
        # 8. Conditional mandatory rules
        if status_cols and name_cols:
            status_col = status_cols[0]['name']
            target_col = name_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_conditional_{target_col.lower()}",
                "rule_type": "conditional_mandatory",
                "target_columns": [target_col],
                "params": {"condition": f"{status_col} = 'Active'"},
                "enabled": True
            })
        
        if phone_cols and status_cols:
            phone_col = phone_cols[0]['name']
            status_col = status_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_phone_required_if_active",
                "rule_type": "conditional_mandatory",
                "target_columns": [phone_col],
                "params": {"condition": f"{status_col} IN ('Active', 'Premium')"},
                "enabled": True
            })
        
        # 9. Cross-column date validation
        if len(date_cols) >= 2:
            date1 = date_cols[0]['name']
            date2 = date_cols[1]['name']
            rule_templates.append({
                "rule_id": f"r_date_order_{date1.lower()}_{date2.lower()}",
                "rule_type": "expression",
                "target_columns": [date1, date2],
                "params": {"expression": f"TO_DATE({date1}, 'YYYY-MM-DD') <= TO_DATE({date2}, 'YYYY-MM-DD')"},
                "enabled": True
            })
        
        # 10. String length validation for text columns
        for col in text_cols[:2]:
            col_name = col['name']
            # Try to extract length from type if available
            col_type = col.get('type', 'VARCHAR2(100)')
            max_len = 100
            if '(' in col_type:
                try:
                    max_len = int(col_type.split('(')[1].split(')')[0].split(',')[0])
                except:
                    max_len = 100
            
            rule_templates.append({
                "rule_id": f"r_length_{col_name.lower()}",
                "rule_type": "expression",
                "target_columns": [col_name],
                "params": {"expression": f"LENGTH({col_name}) <= {max_len}"},
                "enabled": True
            })
        
        # 11. Not null with trimmed check for text
        for col in text_cols[:2]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_not_blank_{col_name.lower()}",
                "rule_type": "expression",
                "target_columns": [col_name],
                "params": {"expression": f"TRIM({col_name}) IS NOT NULL"},
                "enabled": True
            })
        
        # ===== NEW DYNAMIC RULE TYPES =====
        
        # 12. Freshness rules - Confirms that date values are up to date
        for col in date_cols[:2]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_freshness_{col_name.lower()}",
                "rule_type": "freshness",
                "target_columns": [col_name],
                "params": {
                    "max_age_days": 30,
                    "expression": f"{col_name} >= SYSDATE - 30"
                },
                "enabled": True
            })
        
        # 13. Data type match rules - Confirms values match their data type requirements
        for col in numeric_cols[:2]:
            col_name = col['name']
            col_type = col.get('type', 'NUMBER')
            precision = col.get('precision')
            scale = col.get('scale', 0)
            rule_templates.append({
                "rule_id": f"r_datatype_{col_name.lower()}",
                "rule_type": "data_type_match",
                "target_columns": [col_name],
                "params": {
                    "expected_type": col_type,
                    "precision": precision,
                    "scale": scale,
                    "expression": f"REGEXP_LIKE(TO_CHAR({col_name}), '^-?[0-9]+(\\.[0-9]+)?$')"
                },
                "enabled": True
            })
        
        # Data type match for date columns stored as strings
        date_string_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['DATE', 'DOB', 'BIRTH', 'CREATED', 'UPDATED', 'MODIFIED'])]
        for col in date_string_cols[:2]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_datatype_date_{col_name.lower()}",
                "rule_type": "data_type_match",
                "target_columns": [col_name],
                "params": {
                    "expected_type": "DATE_STRING",
                    "format": "YYYY-MM-DD",
                    "expression": f"REGEXP_LIKE({col_name}, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$')"
                },
                "enabled": True
            })
        
        # 14. Duplicate rows check - Checks for duplicate rows across multiple columns
        if len(all_cols) >= 2:
            # Use first few meaningful columns for duplicate check
            dup_check_cols = []
            for col in all_cols:
                if not any(k in col['name'].upper() for k in ['ID', 'KEY', 'SEQ', 'ROWID']):
                    dup_check_cols.append(col['name'])
                if len(dup_check_cols) >= 3:
                    break
            
            if len(dup_check_cols) >= 2:
                rule_templates.append({
                    "rule_id": "r_duplicate_rows",
                    "rule_type": "duplicate_rows",
                    "target_columns": dup_check_cols,
                    "params": {
                        "check_columns": dup_check_cols,
                        "expression": f"SELECT {', '.join(dup_check_cols)}, COUNT(*) FROM {table_name} GROUP BY {', '.join(dup_check_cols)} HAVING COUNT(*) > 1"
                    },
                    "enabled": True
                })
        
        # 15. Empty/blank fields - Looks for blank and empty fields
        for col in text_cols[:3]:
            col_name = col['name']
            nullable = col.get('nullable', True)
            if not nullable:  # Only for columns that shouldn't be empty
                rule_templates.append({
                    "rule_id": f"r_empty_blank_{col_name.lower()}",
                    "rule_type": "empty_blank",
                    "target_columns": [col_name],
                    "params": {
                        "allow_null": False,
                        "allow_empty_string": False,
                        "allow_whitespace_only": False,
                        "expression": f"{col_name} IS NOT NULL AND TRIM({col_name}) IS NOT NULL AND LENGTH(TRIM({col_name})) > 0"
                    },
                    "enabled": True
                })
        
        # Also add for important looking columns regardless of nullable
        important_cols = name_cols + email_cols
        for col in important_cols[:2]:
            col_name = col['name']
            if f"r_empty_blank_{col_name.lower()}" not in [r.get('rule_id') for r in rule_templates]:
                rule_templates.append({
                    "rule_id": f"r_empty_blank_{col_name.lower()}",
                    "rule_type": "empty_blank",
                    "target_columns": [col_name],
                    "params": {
                        "allow_null": False,
                        "allow_empty_string": False,
                        "allow_whitespace_only": False,
                        "expression": f"{col_name} IS NOT NULL AND TRIM({col_name}) IS NOT NULL AND LENGTH(TRIM({col_name})) > 0"
                    },
                    "enabled": True
                })
        
        # 16. Table lookup rules - Confirms value exists in another table
        # Generate lookup rules based on column naming patterns
        fk_pattern_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['_ID', '_CODE', '_KEY', '_REF', '_FK'])]
        for col in fk_pattern_cols[:2]:
            col_name = col['name']
            # Try to infer lookup table from column name
            lookup_table = None
            if '_ID' in col_name.upper():
                lookup_table = col_name.upper().replace('_ID', '')
            elif '_CODE' in col_name.upper():
                lookup_table = col_name.upper().replace('_CODE', '')
            elif '_KEY' in col_name.upper():
                lookup_table = col_name.upper().replace('_KEY', '')
            
            if lookup_table:
                rule_templates.append({
                    "rule_id": f"r_table_lookup_{col_name.lower()}",
                    "rule_type": "table_lookup",
                    "target_columns": [col_name],
                    "params": {
                        "lookup_table": f"{lookup_table}",
                        "lookup_column": "ID",
                        "expression": f"{col_name} IN (SELECT ID FROM {lookup_table})",
                        "allow_null": True
                    },
                    "enabled": True
                })
        
        # 17. String format match rules - Various format validations
        # SSN format
        ssn_cols = [col for col in text_cols if 'SSN' in col['name'].upper()]
        for col in ssn_cols[:1]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_format_ssn_{col_name.lower()}",
                "rule_type": "string_format_match",
                "target_columns": [col_name],
                "params": {
                    "format_name": "SSN",
                    "pattern": "^[0-9]{3}-[0-9]{2}-[0-9]{4}$|^[0-9]{9}$",
                    "description": "Social Security Number format (XXX-XX-XXXX or XXXXXXXXX)"
                },
                "enabled": True
            })
        
        # Credit card format
        cc_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['CREDIT_CARD', 'CARD_NUMBER', 'CC_NUM'])]
        for col in cc_cols[:1]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_format_cc_{col_name.lower()}",
                "rule_type": "string_format_match",
                "target_columns": [col_name],
                "params": {
                    "format_name": "CREDIT_CARD",
                    "pattern": "^[0-9]{13,19}$",
                    "description": "Credit card number (13-19 digits)"
                },
                "enabled": True
            })
        
        # Postal/ZIP code format
        zip_cols = [col for col in text_cols if any(k in col['name'].upper() for k in ['ZIP', 'POSTAL', 'PIN_CODE', 'PINCODE'])]
        for col in zip_cols[:1]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_format_zip_{col_name.lower()}",
                "rule_type": "string_format_match",
                "target_columns": [col_name],
                "params": {
                    "format_name": "POSTAL_CODE",
                    "pattern": "^[0-9]{5,6}(-[0-9]{4})?$",
                    "description": "Postal/ZIP code format"
                },
                "enabled": True
            })
        
        # 18. Custom expression rules - Flexible custom validation
        # Add a sample custom rule for business logic
        if amount_cols and date_cols:
            amount_col = amount_cols[0]['name']
            date_col = date_cols[0]['name']
            rule_templates.append({
                "rule_id": "r_custom_business_logic",
                "rule_type": "custom",
                "category": "data_validity",
                "target_columns": [amount_col, date_col],
                "params": {
                    "expression": f"CASE WHEN {date_col} < SYSDATE - 365 THEN {amount_col} = 0 ELSE 1=1 END",
                    "description": "Custom business rule: Old records should have zero amount",
                    "severity": "warning"
                },
                "enabled": True
            })
        
        # Custom rule for cross-field validation
        if name_cols and len(name_cols) >= 2:
            rule_templates.append({
                "rule_id": "r_custom_name_consistency",
                "rule_type": "custom",
                "category": "data_validity",
                "target_columns": [name_cols[0]['name'], name_cols[1]['name']],
                "params": {
                    "expression": f"{name_cols[0]['name']} IS NOT NULL OR {name_cols[1]['name']} IS NOT NULL",
                    "description": "At least one name field must be populated",
                    "severity": "error"
                },
                "enabled": True
            })
        
        # ===== BUSINESS ENTITY RULES =====
        # These rules ensure core business objects are well-defined and correctly related
        
        # 19. Entity Uniqueness - Every entity must be uniquely identifiable
        pk_cols = [col for col in all_cols if not col.get('nullable', True) and any(k in col['name'].upper() for k in ['ID', 'KEY', 'CODE'])]
        for col in pk_cols[:2]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_entity_uniqueness_{col_name.lower()}",
                "rule_type": "entity_uniqueness",
                "category": "business_entity",
                "target_columns": [col_name],
                "params": {
                    "check_type": "primary_identifier",
                    "allow_null": False,
                    "expression": f"SELECT {col_name}, COUNT(*) FROM {table_name} WHERE {col_name} IS NOT NULL GROUP BY {col_name} HAVING COUNT(*) > 1",
                    "description": f"Every record must have a unique non-null {col_name}"
                },
                "enabled": True
            })
        
        # Entity uniqueness for composite keys
        if customer_cols and account_cols:
            cust_col = customer_cols[0]['name']
            acct_col = account_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_entity_uniqueness_composite_{cust_col.lower()}_{acct_col.lower()}",
                "rule_type": "entity_uniqueness",
                "category": "business_entity",
                "target_columns": [cust_col, acct_col],
                "params": {
                    "check_type": "composite_key",
                    "expression": f"SELECT {cust_col}, {acct_col}, COUNT(*) FROM {table_name} GROUP BY {cust_col}, {acct_col} HAVING COUNT(*) > 1",
                    "description": f"Combination of {cust_col} and {acct_col} must be unique"
                },
                "enabled": True
            })
        
        # 20. Cardinality Rules - Relationship constraints
        if fk_cols:
            for col in fk_cols[:2]:
                col_name = col['name']
                # Infer parent table
                parent_table = None
                for pattern in ['_ID', '_KEY', '_REF', '_FK']:
                    if pattern in col_name.upper():
                        parent_table = col_name.upper().replace(pattern, '')
                        break
                
                if parent_table:
                    rule_templates.append({
                        "rule_id": f"r_cardinality_{col_name.lower()}",
                        "rule_type": "cardinality",
                        "category": "business_entity",
                        "target_columns": [col_name],
                        "params": {
                            "relationship_type": "many_to_one",
                            "parent_table": parent_table,
                            "parent_column": "ID",
                            "min_occurrences": 0,
                            "max_occurrences": None,
                            "expression": f"{col_name} IN (SELECT ID FROM {parent_table})",
                            "description": f"Many {table_name} records can reference one {parent_table} record"
                        },
                        "enabled": True
                    })
        
        # 21. Optionality Rules - Mandatory vs optional relationships
        for col in fk_cols[:2]:
            col_name = col['name']
            nullable = col.get('nullable', True)
            rule_templates.append({
                "rule_id": f"r_optionality_{col_name.lower()}",
                "rule_type": "optionality",
                "category": "business_entity",
                "target_columns": [col_name],
                "params": {
                    "relationship_mandatory": not nullable,
                    "allow_null": nullable,
                    "expression": f"{col_name} IS NOT NULL" if not nullable else f"1=1",
                    "description": f"Relationship via {col_name} is {'mandatory' if not nullable else 'optional'}"
                },
                "enabled": True
            })
        
        # ===== BUSINESS ATTRIBUTE RULES =====
        # Focus on individual data elements within business entities
        
        # 22. Data Inheritance - Attributes consistent across subtypes
        if account_cols:
            for col in account_cols[:1]:
                col_name = col['name']
                rule_templates.append({
                    "rule_id": f"r_data_inheritance_{col_name.lower()}",
                    "rule_type": "data_inheritance",
                    "category": "business_attribute",
                    "target_columns": [col_name],
                    "params": {
                        "supertype": "ACCOUNT",
                        "subtypes": ["CHECKING", "SAVINGS", "LOAN"],
                        "inherited_attribute": col_name,
                        "consistency_check": "format_and_length",
                        "expression": f"LENGTH({col_name}) = (SELECT MAX(LENGTH({col_name})) FROM {table_name})",
                        "description": f"{col_name} format must be consistent across all account types"
                    },
                    "enabled": True
                })
        
        # 23. Data Domain Rules - Values conform to allowed formats/ranges
        # State code domain
        for col in state_code_cols[:1]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_data_domain_{col_name.lower()}",
                "rule_type": "data_domain",
                "category": "business_attribute",
                "target_columns": [col_name],
                "params": {
                    "domain_type": "state_code",
                    "allowed_values": ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"],
                    "expression": f"UPPER({col_name}) IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC')",
                    "description": f"{col_name} must be a valid US state abbreviation"
                },
                "enabled": True
            })
        
        # Age domain (0-120)
        for col in age_cols[:1]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_data_domain_{col_name.lower()}",
                "rule_type": "data_domain",
                "category": "business_attribute",
                "target_columns": [col_name],
                "params": {
                    "domain_type": "numeric_range",
                    "min_value": 0,
                    "max_value": 120,
                    "expression": f"{col_name} BETWEEN 0 AND 120",
                    "description": f"{col_name} must be between 0 and 120"
                },
                "enabled": True
            })
        
        # Rate/percentage domain (0-100 or 0-1)
        for col in rate_cols[:2]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_data_domain_{col_name.lower()}",
                "rule_type": "data_domain",
                "category": "business_attribute",
                "target_columns": [col_name],
                "params": {
                    "domain_type": "percentage",
                    "min_value": 0,
                    "max_value": 100,
                    "expression": f"{col_name} BETWEEN 0 AND 100",
                    "description": f"{col_name} must be a valid percentage (0-100)"
                },
                "enabled": True
            })
        
        # Date format domain
        date_string_cols_domain = [col for col in text_cols if any(k in col['name'].upper() for k in ['DATE', 'DOB', 'BIRTH'])]
        for col in date_string_cols_domain[:1]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_data_domain_date_{col_name.lower()}",
                "rule_type": "data_domain",
                "category": "business_attribute",
                "target_columns": [col_name],
                "params": {
                    "domain_type": "date_format",
                    "format": "CCYY/MM/DD",
                    "pattern": "^[0-9]{4}/[0-9]{2}/[0-9]{2}$",
                    "expression": f"REGEXP_LIKE({col_name}, '^[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}$')",
                    "description": f"{col_name} must follow CCYY/MM/DD format"
                },
                "enabled": True
            })
        
        # ===== DATA DEPENDENCY RULES =====
        # Define logical and conditional relationships
        
        # 24. Entity Relationship Dependency - Existence depends on conditions
        if status_cols and fk_cols:
            status_col = status_cols[0]['name']
            fk_col = fk_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_entity_rel_dependency_{fk_col.lower()}",
                "rule_type": "entity_relationship_dependency",
                "category": "data_dependency",
                "target_columns": [fk_col, status_col],
                "params": {
                    "dependency_type": "conditional_existence",
                    "condition": f"{status_col} NOT IN ('Delinquent', 'Suspended', 'Blocked')",
                    "expression": f"CASE WHEN {status_col} IN ('Delinquent', 'Suspended', 'Blocked') THEN {fk_col} IS NULL ELSE 1=1 END",
                    "description": f"New relationships via {fk_col} cannot be created for records with Delinquent/Suspended/Blocked status"
                },
                "enabled": True
            })
        
        # 25. Attribute Dependency - Value depends on other attributes
        # Loan amount dependency
        if loan_cols and status_cols:
            loan_col = loan_cols[0]['name']
            status_col = status_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_attr_dependency_{loan_col.lower()}_status",
                "rule_type": "attribute_dependency",
                "category": "data_dependency",
                "target_columns": [loan_col, status_col],
                "params": {
                    "dependency_type": "conditional_value",
                    "condition": f"{status_col} = 'Funded'",
                    "required_value": f"{loan_col} > 0",
                    "expression": f"CASE WHEN {status_col} = 'Funded' THEN {loan_col} > 0 ELSE 1=1 END",
                    "description": f"If {status_col} is 'Funded', then {loan_col} must be greater than 0"
                },
                "enabled": True
            })
        
        # Calculated field dependency (Pay = Hours * Rate)
        hours_cols = [col for col in numeric_cols if any(k in col['name'].upper() for k in ['HOURS', 'HRS', 'WORKED'])]
        pay_cols = [col for col in numeric_cols if any(k in col['name'].upper() for k in ['PAY', 'WAGE', 'SALARY'])]
        if hours_cols and rate_cols and pay_cols:
            hours_col = hours_cols[0]['name']
            rate_col = rate_cols[0]['name']
            pay_col = pay_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_attr_dependency_calculated_{pay_col.lower()}",
                "rule_type": "attribute_dependency",
                "category": "data_dependency",
                "target_columns": [pay_col, hours_col, rate_col],
                "params": {
                    "dependency_type": "calculated",
                    "formula": f"{pay_col} = {hours_col} * {rate_col}",
                    "tolerance": 0.01,
                    "expression": f"ABS({pay_col} - ({hours_col} * {rate_col})) < 0.01",
                    "description": f"{pay_col} should equal {hours_col} multiplied by {rate_col}"
                },
                "enabled": True
            })
        
        # Mutual exclusion dependency (salary vs commission)
        salary_cols = [col for col in numeric_cols if 'SALARY' in col['name'].upper()]
        commission_cols = [col for col in numeric_cols if 'COMMISSION' in col['name'].upper()]
        if salary_cols and commission_cols:
            salary_col = salary_cols[0]['name']
            commission_col = commission_cols[0]['name']
            rule_templates.append({
                "rule_id": f"r_attr_dependency_mutual_exclusion",
                "rule_type": "attribute_dependency",
                "category": "data_dependency",
                "target_columns": [salary_col, commission_col],
                "params": {
                    "dependency_type": "mutual_exclusion",
                    "expression": f"NOT ({salary_col} > 0 AND {commission_col} > 0)",
                    "description": f"If {salary_col} > 0, then {commission_col} must be NULL or 0 (mutually exclusive)"
                },
                "enabled": True
            })
        
        # 26. Cross-field validation
        if len(date_cols) >= 2:
            start_date_cols = [col for col in date_cols if any(k in col['name'].upper() for k in ['START', 'BEGIN', 'FROM', 'OPEN', 'CREATED'])]
            end_date_cols = [col for col in date_cols if any(k in col['name'].upper() for k in ['END', 'CLOSE', 'TO', 'COMPLETED', 'EXPIRY'])]
            if start_date_cols and end_date_cols:
                start_col = start_date_cols[0]['name']
                end_col = end_date_cols[0]['name']
                rule_templates.append({
                    "rule_id": f"r_cross_field_{start_col.lower()}_{end_col.lower()}",
                    "rule_type": "cross_field_validation",
                    "category": "data_dependency",
                    "target_columns": [start_col, end_col],
                    "params": {
                        "validation_type": "date_sequence",
                        "expression": f"{start_col} <= {end_col}",
                        "description": f"{start_col} must be on or before {end_col}"
                    },
                    "enabled": True
                })
        
        # ===== DATA VALIDITY RULES =====
        # Ensure data is complete, correct, accurate, precise, unique, and consistent
        
        # 27. Completeness - Required records/attributes must exist
        mandatory_biz_cols = [col for col in all_cols if not col.get('nullable', True)]
        for col in mandatory_biz_cols[:3]:
            col_name = col['name']
            rule_templates.append({
                "rule_id": f"r_completeness_{col_name.lower()}",
                "rule_type": "completeness",
                "category": "data_validity",
                "target_columns": [col_name],
                "params": {
                    "check_type": "not_null",
                    "expression": f"{col_name} IS NOT NULL",
                    "description": f"{col_name} is a required field and must not be NULL"
                },
                "enabled": True
            })
        
        # 28. Correctness & Accuracy - Values reflect real-world truth
        if amount_cols:
            for col in amount_cols[:1]:
                col_name = col['name']
                rule_templates.append({
                    "rule_id": f"r_correctness_{col_name.lower()}",
                    "rule_type": "correctness_accuracy",
                    "category": "data_validity",
                    "target_columns": [col_name],
                    "params": {
                        "validation_type": "business_reasonableness",
                        "min_value": 0,
                        "max_value": 999999999,
                        "expression": f"{col_name} >= 0 AND {col_name} <= 999999999",
                        "description": f"{col_name} must be a reasonable positive value within business limits"
                    },
                    "enabled": True
                })
        
        # 29. Precision - Data stored with required detail level
        for col in rate_cols[:1]:
            col_name = col['name']
            precision = col.get('precision', 10)
            scale = col.get('scale', 4)
            rule_templates.append({
                "rule_id": f"r_precision_{col_name.lower()}",
                "rule_type": "precision",
                "category": "data_validity",
                "target_columns": [col_name],
                "params": {
                    "required_precision": precision,
                    "required_scale": scale,
                    "decimal_places": 4,
                    "expression": f"LENGTH(SUBSTR(TO_CHAR({col_name}), INSTR(TO_CHAR({col_name}), '.') + 1)) <= {scale}",
                    "description": f"{col_name} must be stored with up to {scale} decimal places precision"
                },
                "enabled": True
            })
        
        # 30. Consistency - Duplicate/redundant data must match
        if name_cols and len(name_cols) >= 3:
            # Full name should match first + middle + last
            full_name_cols = [col for col in name_cols if 'FULL' in col['name'].upper()]
            first_name_cols = [col for col in name_cols if 'FIRST' in col['name'].upper()]
            last_name_cols = [col for col in name_cols if 'LAST' in col['name'].upper()]
            if full_name_cols and first_name_cols and last_name_cols:
                full_col = full_name_cols[0]['name']
                first_col = first_name_cols[0]['name']
                last_col = last_name_cols[0]['name']
                rule_templates.append({
                    "rule_id": f"r_consistency_name",
                    "rule_type": "consistency",
                    "category": "data_validity",
                    "target_columns": [full_col, first_col, last_col],
                    "params": {
                        "consistency_type": "derived_value",
                        "expression": f"UPPER({full_col}) LIKE '%' || UPPER({first_col}) || '%' AND UPPER({full_col}) LIKE '%' || UPPER({last_col}) || '%'",
                        "description": f"{full_col} must contain both {first_col} and {last_col}"
                    },
                    "enabled": True
                })
        
        # 31. Compliance - PII and sensitive data validation
        for col in pii_cols[:3]:
            col_name = col['name']
            col_upper = col['name'].upper()
            
            if 'SSN' in col_upper:
                rule_templates.append({
                    "rule_id": f"r_compliance_ssn_{col_name.lower()}",
                    "rule_type": "compliance",
                    "category": "data_validity",
                    "target_columns": [col_name],
                    "params": {
                        "pii_type": "SSN",
                        "format_pattern": "^[0-9]{3}-[0-9]{2}-[0-9]{4}$|^[0-9]{9}$",
                        "masking_required": True,
                        "expression": f"REGEXP_LIKE({col_name}, '^[0-9]{{3}}-[0-9]{{2}}-[0-9]{{4}}$') OR REGEXP_LIKE({col_name}, '^[0-9]{{9}}$')",
                        "description": f"{col_name} must be a valid SSN format (XXX-XX-XXXX or 9 digits)"
                    },
                    "enabled": True
                })
            elif 'PASSPORT' in col_upper:
                rule_templates.append({
                    "rule_id": f"r_compliance_passport_{col_name.lower()}",
                    "rule_type": "compliance",
                    "category": "data_validity",
                    "target_columns": [col_name],
                    "params": {
                        "pii_type": "PASSPORT",
                        "format_pattern": "^[A-Z]{1,2}[0-9]{6,9}$",
                        "masking_required": True,
                        "expression": f"REGEXP_LIKE(UPPER({col_name}), '^[A-Z]{{1,2}}[0-9]{{6,9}}$')",
                        "description": f"{col_name} must be a valid passport number format"
                    },
                    "enabled": True
                })
            elif 'CREDIT_CARD' in col_upper or 'CARD_NUM' in col_upper:
                rule_templates.append({
                    "rule_id": f"r_compliance_creditcard_{col_name.lower()}",
                    "rule_type": "compliance",
                    "category": "data_validity",
                    "target_columns": [col_name],
                    "params": {
                        "pii_type": "CREDIT_CARD",
                        "format_pattern": "^[0-9]{13,19}$",
                        "masking_required": True,
                        "luhn_check": True,
                        "expression": f"REGEXP_LIKE({col_name}, '^[0-9]{{13,19}}$')",
                        "description": f"{col_name} must be a valid credit card number (13-19 digits)"
                    },
                    "enabled": True
                })
            elif 'BANK_ACCOUNT' in col_upper or 'ACCOUNT_NO' in col_upper:
                rule_templates.append({
                    "rule_id": f"r_compliance_bankaccount_{col_name.lower()}",
                    "rule_type": "compliance",
                    "category": "data_validity",
                    "target_columns": [col_name],
                    "params": {
                        "pii_type": "BANK_ACCOUNT",
                        "format_pattern": "^[0-9]{8,17}$",
                        "masking_required": True,
                        "expression": f"REGEXP_LIKE({col_name}, '^[0-9]{{8,17}}$')",
                        "description": f"{col_name} must be a valid bank account number (8-17 digits)"
                    },
                    "enabled": True
                })
            elif 'TAX_ID' in col_upper or 'NATIONAL_ID' in col_upper:
                rule_templates.append({
                    "rule_id": f"r_compliance_taxid_{col_name.lower()}",
                    "rule_type": "compliance",
                    "category": "data_validity",
                    "target_columns": [col_name],
                    "params": {
                        "pii_type": "TAX_ID",
                        "format_pattern": "^[0-9]{9,15}$",
                        "masking_required": True,
                        "expression": f"REGEXP_LIKE(TO_CHAR({col_name}), '^[0-9]{{9,15}}$')",
                        "description": f"{col_name} must be a valid tax/national ID format"
                    },
                    "enabled": True
                })
        
        # Remove duplicates based on rule_id
        seen_ids = set()
        unique_templates = []
        for rule in rule_templates:
            if rule['rule_id'] not in seen_ids:
                seen_ids.add(rule['rule_id'])
                unique_templates.append(rule)
        
        # If we have fewer templates than requested, add some generic ones
        if len(unique_templates) < num_rules and all_cols:
            remaining = num_rules - len(unique_templates)
            for i, col in enumerate(all_cols[:remaining]):
                if f"r_not_null_{col['name'].lower()}" not in seen_ids:
                    unique_templates.append({
                        "rule_id": f"r_not_null_{col['name'].lower()}",
                        "rule_type": "mandatory",
                        "target_columns": [col['name']],
                        "enabled": True
                    })
        
        # Select requested number of rules (randomize if we have more than needed)
        if len(unique_templates) > num_rules:
            rules = random.sample(unique_templates, num_rules)
        else:
            rules = unique_templates[:num_rules]
        
        # Calculate category summary
        category_summary = {
            "business_entity_rules": len([r for r in rules if r.get('category') == 'business_entity']),
            "business_attribute_rules": len([r for r in rules if r.get('category') == 'business_attribute']),
            "data_dependency_rules": len([r for r in rules if r.get('category') == 'data_dependency']),
            "data_validity_rules": len([r for r in rules if r.get('category') == 'data_validity']),
            "uncategorized_rules": len([r for r in rules if not r.get('category')])
        }
        
        # Build final output
        output = {
            "rule_set_id": rule_set_id,
            "table_name": table_name,
            "database_name": database_name,
            "total_rules": len(rules),
            "rule_category_summary": category_summary,
            "rules": rules
        }
        
        # Format as pretty JSON
        result = json.dumps(output, indent=2)
        
        return wrap_untrusted(f"Generated {len(rules)} Data Quality Rules for '{table_name}' in '{database_name}':\n\n{result}")
        
    except Exception as e:
        return wrap_untrusted(f"[{database_name}] Error generating DQ rules: {str(e)}")


@mcp.tool()
async def apply_dq_rules(
    database_name: str, 
    table_name: str, 
    rules_json: str, 
    ctx: Context, 
    store_results: bool = True,
    sample_percent: int = None
) -> str:
    """Apply data quality rules to validate table data and optionally store results.
    
    Executes the specified DQ rules against the table, calculates pass/fail statistics,
    and stores validation results in DQ_VALIDATION_RESULTS table.
    
    Use: Data quality validation, compliance checking, data profiling.
    Compose: Use after generate_sample_dq_rules to validate data quality.
    
    Args:
        database_name: Name of the database containing the table
        table_name: Name of the table to validate
        rules_json: JSON array of rules to apply (from generate_sample_dq_rules output)
        store_results: Whether to store results in DQ_VALIDATION_RESULTS table (default: True)
        sample_percent: Sample percentage for large tables (1-100). If None, validates all rows.
                       Use 10 for 10% sample on large tables.
    
    Returns:
        JSON with validation_run_id, overall status, pass rates, and detailed results per rule.
        Results include: total_rows, passed_rows, failed_rows, pass_rate, sample_failures.
    
    Storage Table Schema (DQ_VALIDATION_RESULTS):
        - VALIDATION_RUN_ID: Unique identifier for validation run
        - RULE_ID: Rule identifier
        - RULE_TYPE: Type of rule (mandatory, format, etc.)
        - RULE_CATEGORY: Category of rule
        - TABLE_NAME: Validated table name
        - TARGET_COLUMNS: Columns being validated
        - TOTAL_ROWS: Total rows checked
        - PASSED_ROWS: Rows that passed
        - FAILED_ROWS: Rows that failed
        - PASS_RATE: Pass percentage (0-100)
        - STATUS: PASSED, FAILED, or ERROR
        - ERROR_MESSAGE: Error details if STATUS is ERROR
        - EXECUTED_AT: Timestamp of validation
        - SAMPLE_FAILURES: CLOB with sample failed rows (JSON)
    
    Example:
        apply_dq_rules(
            database_name="dva_sample",
            table_name="CUSTOMER_PAYMENTS",
            rules_json='[{"rule_id": "r_mandatory_email", "rule_type": "mandatory", ...}]',
            store_results=True,
            sample_percent=10
        )
    """
    import time
    import uuid
    from datetime import datetime
    
    db_context = validate_database(ctx, database_name)
    try:
        # Parse rules JSON
        try:
            rules = json.loads(rules_json)
            if isinstance(rules, dict) and 'rules' in rules:
                rules = rules['rules']
        except json.JSONDecodeError as e:
            return wrap_untrusted(f"[{database_name}] Invalid JSON format for rules: {str(e)}")
        if not rules or not isinstance(rules, list):
            return wrap_untrusted(f"[{database_name}] No valid rules provided. Expected JSON array of rules.")
        validation_run_id = f"vr_{uuid.uuid4().hex[:12]}"
        validation_timestamp = datetime.now().isoformat()
        # --- Write mode override logic ---
        storage_warning = None
        can_store = store_results
        original_read_only = True
        connector = None
        if store_results:
            try:
                if hasattr(db_context, 'db_connector'):
                    connector = db_context.db_connector
                    original_read_only = getattr(connector, 'read_only', True)
                    if original_read_only:
                        print(f"[DQ Storage] Temporarily enabling write mode for storing rules and results", file=sys.stderr)
                        connector.read_only = False
                else:
                    storage_warning = "Could not access database connector to enable write mode"
                    can_store = False
            except Exception as e:
                storage_warning = f"Could not configure write mode: {str(e)}"
                can_store = False
        sample_clause = ""
        if sample_percent and 1 <= sample_percent <= 100:
            sample_clause = f" SAMPLE({sample_percent})"
        # --- Create storage tables if needed ---
        storage_table = "DQ_VALIDATION_RESULTS"
        rules_table = "DQ_RULES_STORE"
        if can_store:
            try:
                # Create DQ_RULES_STORE table
                create_rules_table_sql = f"""
                DECLARE
                    table_exists NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO table_exists FROM user_tables WHERE table_name = '{rules_table}';
                    IF table_exists = 0 THEN
                        EXECUTE IMMEDIATE '
                            CREATE TABLE {rules_table} (
                                RULE_ID VARCHAR2(100) NOT NULL,
                                RULE_TYPE VARCHAR2(50),
                                RULE_CATEGORY VARCHAR2(50),
                                TABLE_NAME VARCHAR2(128),
                                TARGET_COLUMNS VARCHAR2(500),
                                PARAMS CLOB,
                                ENABLED NUMBER(1) DEFAULT 1,
                                CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                                UPDATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                                CONSTRAINT PK_DQ_RULES_STORE PRIMARY KEY (RULE_ID)
                            )
                        ';
                    END IF;
                END;
                """
                await db_context.run_sql_query(create_rules_table_sql)
                # Create DQ_VALIDATION_RESULTS table
                create_table_sql = f"""
                DECLARE
                    table_exists NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO table_exists FROM user_tables WHERE table_name = '{storage_table}';
                    IF table_exists = 0 THEN
                        EXECUTE IMMEDIATE '
                            CREATE TABLE {storage_table} (
                                VALIDATION_RUN_ID VARCHAR2(50) NOT NULL,
                                RULE_ID VARCHAR2(100) NOT NULL,
                                RULE_TYPE VARCHAR2(50),
                                RULE_CATEGORY VARCHAR2(50),
                                TABLE_NAME VARCHAR2(128) NOT NULL,
                                TARGET_COLUMNS VARCHAR2(500),
                                TOTAL_ROWS NUMBER DEFAULT 0,
                                PASSED_ROWS NUMBER DEFAULT 0,
                                FAILED_ROWS NUMBER DEFAULT 0,
                                PASS_RATE NUMBER(5,2) DEFAULT 0,
                                STATUS VARCHAR2(20) NOT NULL,
                                ERROR_MESSAGE VARCHAR2(4000),
                                EXECUTED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                                SAMPLE_FAILURES CLOB,
                                CONSTRAINT PK_DQ_VALIDATION_RESULTS PRIMARY KEY (VALIDATION_RUN_ID, RULE_ID)
                            )
                        ';
                    END IF;
                END;
                """
                await db_context.run_sql_query(create_table_sql)
            except Exception as e:
                can_store = False
                storage_warning = f"Could not create/access storage table: {str(e)}"
        
        # Helper function to build validation SQL based on rule type
        def build_validation_sql(rule, table, sample_clause):
            rule_type = rule.get('rule_type', '').lower()
            target_cols = rule.get('target_columns', [])
            params = rule.get('params', {}) or {}
            
            if not target_cols:
                return None, None, "No target columns specified"
            
            col = target_cols[0]  # Primary column
            cols_str = ', '.join(target_cols)
            
            # Total count SQL
            total_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause}"
            
            # Failed count SQL based on rule type
            if rule_type in ('mandatory', 'completeness'):
                failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NULL"
                sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NULL AND ROWNUM <= 5"
                
            elif rule_type in ('empty_blank',):
                failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NULL OR TRIM({col}) IS NULL OR LENGTH(TRIM({col})) = 0"
                sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE ({col} IS NULL OR TRIM({col}) IS NULL OR LENGTH(TRIM({col})) = 0) AND ROWNUM <= 5"
                
            elif rule_type in ('uniqueness', 'entity_uniqueness'):
                failed_sql = f"SELECT COUNT(*) as cnt FROM (SELECT {col} FROM {table}{sample_clause} GROUP BY {col} HAVING COUNT(*) > 1)"
                sample_sql = f"SELECT {col}, COUNT(*) as dup_count FROM {table}{sample_clause} GROUP BY {col} HAVING COUNT(*) > 1 FETCH FIRST 5 ROWS ONLY"
                
            elif rule_type == 'duplicate_rows':
                if len(target_cols) > 1:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM (SELECT {cols_str} FROM {table}{sample_clause} GROUP BY {cols_str} HAVING COUNT(*) > 1)"
                    sample_sql = f"SELECT {cols_str}, COUNT(*) as dup_count FROM {table}{sample_clause} GROUP BY {cols_str} HAVING COUNT(*) > 1 FETCH FIRST 5 ROWS ONLY"
                else:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM (SELECT {col} FROM {table}{sample_clause} GROUP BY {col} HAVING COUNT(*) > 1)"
                    sample_sql = f"SELECT {col}, COUNT(*) as dup_count FROM {table}{sample_clause} GROUP BY {col} HAVING COUNT(*) > 1 FETCH FIRST 5 ROWS ONLY"
                    
            elif rule_type in ('format', 'string_format_match', 'compliance'):
                pattern = params.get('pattern', params.get('format_pattern', '.*'))
                pattern = pattern.replace("'", "''")
                failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND NOT REGEXP_LIKE({col}, '{pattern}')"
                sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND NOT REGEXP_LIKE({col}, '{pattern}') AND ROWNUM <= 5"
                
            elif rule_type == 'value_in_list':
                allowed = params.get('allowed_values', [])
                if allowed:
                    values_str = ', '.join([f"'{v}'" for v in allowed])
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} NOT IN ({values_str})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} NOT IN ({values_str}) AND ROWNUM <= 5"
                else:
                    return None, None, "No allowed_values specified"
                    
            elif rule_type in ('data_domain',):
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND NOT ({expression}) AND ROWNUM <= 5"
                else:
                    min_val = params.get('min_value')
                    max_val = params.get('max_value')
                    if min_val is not None and max_val is not None:
                        failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND ({col} < {min_val} OR {col} > {max_val})"
                        sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND ({col} < {min_val} OR {col} > {max_val}) AND ROWNUM <= 5"
                    else:
                        return None, None, "No expression or min/max values specified"
                        
            elif rule_type in ('expression', 'cross_field_validation', 'attribute_dependency', 'correctness_accuracy', 'data_inheritance'):
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE NOT ({expression}) AND ROWNUM <= 5"
                else:
                    return None, None, "No expression specified"
                    
            elif rule_type == 'freshness':
                max_days = params.get('max_age_days', 30)
                failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} < SYSDATE - {max_days}"
                sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} < SYSDATE - {max_days} AND ROWNUM <= 5"
                
            elif rule_type == 'data_type_match':
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND NOT ({expression}) AND ROWNUM <= 5"
                else:
                    return None, None, "No expression specified for data_type_match"
                    
            elif rule_type == 'table_lookup':
                lookup_table = params.get('lookup_table', '')
                lookup_col = params.get('lookup_column', 'ID')
                if lookup_table:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT {lookup_col} FROM {lookup_table})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT {lookup_col} FROM {lookup_table}) AND ROWNUM <= 5"
                else:
                    return None, None, "No lookup_table specified"
                    
            elif rule_type == 'cardinality':
                parent_table = params.get('parent_table', '')
                parent_col = params.get('parent_column', 'ID')
                if parent_table:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT {parent_col} FROM {parent_table})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT {parent_col} FROM {parent_table}) AND ROWNUM <= 5"
                else:
                    return None, None, "No parent_table specified"
                    
            elif rule_type == 'optionality':
                if not params.get('allow_null', True):
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NULL"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NULL AND ROWNUM <= 5"
                else:
                    failed_sql = f"SELECT 0 as cnt FROM DUAL"
                    sample_sql = None
                    
            elif rule_type == 'conditional_mandatory':
                condition = params.get('condition', '')
                if condition:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE ({condition}) AND {col} IS NULL"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE ({condition}) AND {col} IS NULL AND ROWNUM <= 5"
                else:
                    return None, None, "No condition specified"
                    
            elif rule_type in ('precision',):
                scale = params.get('required_scale', params.get('decimal_places', 4))
                failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND LENGTH(SUBSTR(TO_CHAR({col}), INSTR(TO_CHAR({col}), '.') + 1)) > {scale}"
                sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE {col} IS NOT NULL AND LENGTH(SUBSTR(TO_CHAR({col}), INSTR(TO_CHAR({col}), '.') + 1)) > {scale} AND ROWNUM <= 5"
                
            elif rule_type == 'consistency':
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE NOT ({expression}) AND ROWNUM <= 5"
                else:
                    return None, None, "No expression specified for consistency check"
                    
            elif rule_type == 'custom':
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE NOT ({expression}) AND ROWNUM <= 5"
                else:
                    return None, None, "No expression specified for custom rule"
                    
            elif rule_type == 'entity_relationship_dependency':
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE NOT ({expression}) AND ROWNUM <= 5"
                else:
                    return None, None, "No expression specified"
                    
            else:
                expression = params.get('expression', '')
                if expression:
                    failed_sql = f"SELECT COUNT(*) as cnt FROM {table}{sample_clause} WHERE NOT ({expression})"
                    sample_sql = f"SELECT {cols_str} FROM {table}{sample_clause} WHERE NOT ({expression}) AND ROWNUM <= 5"
                else:
                    return None, None, f"Unsupported rule type: {rule_type}"
            
            return total_sql, failed_sql, sample_sql
        
        # Execute validation for each rule
        results = []
        rules_passed = 0
        rules_failed = 0
        rules_errored = 0
        total_pass_rate = 0.0
        
        for rule in rules:
            rule_id = rule.get('rule_id', 'unknown')
            rule_type = rule.get('rule_type', 'unknown')
            category = rule.get('category')
            target_cols = rule.get('target_columns', [])
            params = rule.get('params', {})
            start_time = time.time()
            try:
                # Store rule definition first
                if can_store:
                    try:
                        params_json = json.dumps(params) if params else None
                        merge_rule_sql = f"""
                        MERGE INTO {rules_table} dest
                        USING (SELECT '{rule_id}' as RULE_ID FROM DUAL) src
                        ON (dest.RULE_ID = src.RULE_ID)
                        WHEN MATCHED THEN
                            UPDATE SET 
                                RULE_TYPE = '{rule_type}',
                                RULE_CATEGORY = {f"'{category}'" if category else 'NULL'},
                                TABLE_NAME = '{table_name}',
                                TARGET_COLUMNS = '{','.join(target_cols)}',
                                PARAMS = {f"'{params_json}'" if params_json else 'NULL'},
                                UPDATED_AT = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN
                            INSERT (RULE_ID, RULE_TYPE, RULE_CATEGORY, TABLE_NAME, TARGET_COLUMNS, PARAMS, ENABLED, CREATED_AT, UPDATED_AT)
                            VALUES ('{rule_id}', '{rule_type}', {f"'{category}'" if category else 'NULL'}, '{table_name}', '{','.join(target_cols)}', {f"'{params_json}'" if params_json else 'NULL'}, 1, SYSTIMESTAMP, SYSTIMESTAMP)
                        """
                        await db_context.run_sql_query(merge_rule_sql)
                    except Exception as rule_store_err:
                        if not storage_warning:
                            storage_warning = f"Could not store some rules: {str(rule_store_err)}"
                total_sql, failed_sql, sample_sql = build_validation_sql(rule, table_name, sample_clause)
                if total_sql is None:
                    error_msg = failed_sql if failed_sql else sample_sql
                    results.append({
                        "rule_id": rule_id,
                        "rule_type": rule_type,
                        "category": category,
                        "target_columns": target_cols,
                        "status": "ERROR",
                        "total_rows": 0,
                        "passed_rows": 0,
                        "failed_rows": 0,
                        "pass_rate": 0.0,
                        "error_message": error_msg,
                        "sample_failures": None,
                        "execution_time_ms": (time.time() - start_time) * 1000
                    })
                    rules_errored += 1
                    continue
                total_result = await db_context.run_sql_query(total_sql)
                total_rows = total_result.get('rows', [{}])[0].get('CNT', 0) if total_result.get('rows') else 0
                failed_result = await db_context.run_sql_query(failed_sql)
                failed_rows = failed_result.get('rows', [{}])[0].get('CNT', 0) if failed_result.get('rows') else 0
                passed_rows = total_rows - failed_rows
                pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100.0
                sample_failures = None
                if failed_rows > 0 and sample_sql:
                    try:
                        sample_result = await db_context.run_sql_query(sample_sql)
                        sample_failures = sample_result.get('rows', [])[:5]
                    except:
                        pass
                status = "PASSED" if failed_rows == 0 else "FAILED"
                if status == "PASSED":
                    rules_passed += 1
                else:
                    rules_failed += 1
                total_pass_rate += pass_rate
                execution_time = (time.time() - start_time) * 1000
                result_entry = {
                    "rule_id": rule_id,
                    "rule_type": rule_type,
                    "category": category,
                    "target_columns": target_cols,
                    "status": status,
                    "total_rows": total_rows,
                    "passed_rows": passed_rows,
                    "failed_rows": failed_rows,
                    "pass_rate": round(pass_rate, 2),
                    "error_message": None,
                    "sample_failures": sample_failures,
                    "execution_time_ms": round(execution_time, 2)
                }
                results.append(result_entry)
                # Store validation result
                if can_store:
                    try:
                        sample_json = json.dumps(sample_failures, default=str) if sample_failures else None
                        if sample_json:
                            sample_json = sample_json.replace("'", "''")
                        insert_sql = f"""
                        INSERT INTO {storage_table} (
                            VALIDATION_RUN_ID, RULE_ID, RULE_TYPE, RULE_CATEGORY, TABLE_NAME,
                            TARGET_COLUMNS, TOTAL_ROWS, PASSED_ROWS, FAILED_ROWS, PASS_RATE,
                            STATUS, ERROR_MESSAGE, EXECUTED_AT, SAMPLE_FAILURES
                        ) VALUES (
                            '{validation_run_id}', '{rule_id}', '{rule_type}', 
                            {f"'{category}'" if category else 'NULL'}, '{table_name}',
                            '{','.join(target_cols)}', {total_rows}, {passed_rows}, {failed_rows}, {round(pass_rate, 2)},
                            '{status}', NULL, SYSTIMESTAMP, 
                            {f"'{sample_json}'" if sample_json else 'NULL'}
                        )
                        """
                        await db_context.run_sql_query(insert_sql)
                    except Exception as store_err:
                        if not storage_warning:
                            storage_warning = f"Some results could not be stored: {str(store_err)}"
            except Exception as e:
                execution_time = (time.time() - start_time) * 1000
                results.append({
                    "rule_id": rule_id,
                    "rule_type": rule_type,
                    "category": category,
                    "target_columns": target_cols,
                    "status": "ERROR",
                    "total_rows": 0,
                    "passed_rows": 0,
                    "failed_rows": 0,
                    "pass_rate": 0.0,
                    "error_message": str(e),
                    "sample_failures": None,
                    "execution_time_ms": round(execution_time, 2)
                })
                rules_errored += 1
        # Restore original read-only mode
        if connector and original_read_only:
            try:
                connector.read_only = True
            except:
                pass
        
        # Calculate overall status and pass rate
        total_rules = len(results)
        overall_pass_rate = (total_pass_rate / total_rules) if total_rules > 0 else 0.0
        
        if rules_errored == total_rules:
            overall_status = "ERROR"
        elif rules_failed > 0 or rules_errored > 0:
            overall_status = "FAILED"
        else:
            overall_status = "PASSED"
        
        # Build output
        output = {
            "validation_run_id": validation_run_id,
            "table_name": table_name,
            "database_name": database_name,
            "validation_timestamp": validation_timestamp,
            "total_rules_applied": total_rules,
            "rules_passed": rules_passed,
            "rules_failed": rules_failed,
            "rules_errored": rules_errored,
            "overall_status": overall_status,
            "overall_pass_rate": round(overall_pass_rate, 2),
            "sample_percent_used": sample_percent,
            "results_stored": can_store and not storage_warning,
            "storage_table": storage_table if can_store else None,
            "storage_warning": storage_warning,
            "results": results
        }
        
        result_json = json.dumps(output, indent=2, default=str)
        
        summary = f"DQ Validation Complete for '{table_name}' in '{database_name}':\n"
        summary += f"  - Run ID: {validation_run_id}\n"
        summary += f"  - Rules Applied: {total_rules}\n"
        summary += f"  - Passed: {rules_passed}, Failed: {rules_failed}, Errors: {rules_errored}\n"
        summary += f"  - Overall Status: {overall_status}\n"
        summary += f"  - Overall Pass Rate: {round(overall_pass_rate, 2)}%\n"
        if sample_percent:
            summary += f"  - Sample: {sample_percent}% of rows\n"
        if can_store and not storage_warning:
            summary += f"  - Results stored in: {storage_table}\n"
        elif storage_warning:
            summary += f"  - Warning: {storage_warning}\n"
        
        return wrap_untrusted(f"{summary}\n{result_json}")
        
    except Exception as e:
        # Restore read-only mode even on error
        if connector and original_read_only:
            try:
                connector.read_only = True
            except:
                pass
        return wrap_untrusted(f"[{database_name}] Error applying DQ rules: {str(e)}")


async def run_http_server(host: str, port: int):
    """Run server with HTTP/SSE transport supporting both POST and GET"""
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.responses import JSONResponse
    from mcp.server.sse import SseServerTransport
    import uvicorn
    
    # Create SSE transport
    sse_transport = SseServerTransport("/sse")
    
    async def handle_sse(request):
        """Handle SSE connections (GET requests)"""
        try:
            async with sse_transport.connect_sse(
                request.scope, request.receive, request._send
            ) as streams:
                await mcp._mcp_server.run(
                    streams[0], streams[1], mcp._mcp_server.create_initialization_options()
                )
        except Exception as e:
            print(f"SSE connection error: {e}", file=sys.stderr)
            return JSONResponse({"error": str(e)}, status_code=500)
    
    async def handle_post(request):
        """Handle HTTP POST requests for streamable HTTP"""
        try:
            async with sse_transport.connect_sse(
                request.scope, request.receive, request._send
            ) as streams:
                await mcp._mcp_server.run(
                    streams[0], streams[1], mcp._mcp_server.create_initialization_options()
                )
        except Exception as e:
            print(f"POST connection error: {e}", file=sys.stderr)
            return JSONResponse({"error": str(e)}, status_code=500)
    
    async def health_check(request):
        """Health check endpoint"""
        return JSONResponse({"status": "healthy"})
    
    # Create Starlette app with both GET and POST routes
    app = Starlette(
        routes=[
            Route("/health", health_check, methods=["GET"]),
            Route("/sse", handle_sse, methods=["GET"]),
            Route("/sse", handle_post, methods=["POST"]),
        ]
    )
    
    print(f"Starting HTTP server on {host}:{port}", file=sys.stderr)
    print(f"SSE endpoint: http://{host}:{port}/sse", file=sys.stderr)
    print(f"Health check: http://{host}:{port}/health", file=sys.stderr)
    
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    import argparse
    import asyncio
    
    parser = argparse.ArgumentParser(description="Oracle MCP Server")
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse", "http", "http-api"],
        default="stdio",
        help="Transport protocol to use (stdio for standard input/output, sse/http for HTTP Server-Sent Events, http-api for REST API)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to use for SSE/HTTP/API transport (default: 8000)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to for SSE/HTTP/API transport (default: 0.0.0.0)"
    )
    args = parser.parse_args()
    
    # Handle different transport modes
    if args.transport == "http-api":
        # Run FastAPI REST API server
        from api.app import run_fastapi_server
        asyncio.run(run_fastapi_server(args.host, args.port))
    elif args.transport in ("sse", "http"):
        # Run HTTP/SSE transport for MCP
        asyncio.run(run_http_server(args.host, args.port))
    else:
        # Run stdio transport for MCP
        mcp.run(transport="stdio")
