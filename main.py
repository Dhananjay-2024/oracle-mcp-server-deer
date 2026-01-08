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
        choices=["stdio", "sse", "http"],
        default="stdio",
        help="Transport protocol to use (stdio for standard input/output, sse/http for HTTP Server-Sent Events)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to use for SSE/HTTP transport (default: 8000)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to for SSE/HTTP transport (default: 0.0.0.0)"
    )
    args = parser.parse_args()
    
    # Handle HTTP/SSE transports with custom implementation
    if args.transport in ("sse", "http"):
        asyncio.run(run_http_server(args.host, args.port))
    else:
        mcp.run(transport="stdio")
