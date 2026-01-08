"""Schema discovery routes - table schemas, search, cache management."""

import time
from fastapi import APIRouter, HTTPException

from api.models import (
    APIResponse,
    GetTableSchemaRequest,
    GetBatchTableSchemasRequest,
    SearchTablesRequest,
    SearchColumnsRequest,
    RebuildCacheRequest,
    ColumnInfo,
    TableSchemaResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context

router = APIRouter(prefix="/schema", tags=["Schema Discovery"])


def table_info_to_response(table_info) -> TableSchemaResponse:
    """Convert TableInfo to TableSchemaResponse."""
    columns = [
        ColumnInfo(
            name=col.get("name", ""),
            type=col.get("type", ""),
            nullable=col.get("nullable", True),
            length=col.get("length"),
            precision=col.get("precision"),
            scale=col.get("scale"),
            default=col.get("default"),
        )
        for col in table_info.columns
    ]
    
    return TableSchemaResponse(
        table_name=table_info.table_name,
        columns=columns,
        relationships=table_info.relationships or {},
        constraints=table_info.constraints,
        indexes=table_info.indexes,
        table_stats=table_info.table_stats,
        comments=table_info.comments,
    )


@router.get("/{database_name}/table/{table_name}", response_model=APIResponse)
async def get_table_schema(
    database_name: str,
    table_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get schema for a single table.
    
    Returns columns, relationships, and cached metadata for the specified table.
    Lazy loads and caches the schema on first access.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    table_info = await db_context.get_schema_info(table_name)
    
    if not table_info:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found in database '{database_name}'"
        )
    
    return APIResponse(
        success=True,
        data=table_info_to_response(table_info)
    )


@router.post("/{database_name}/tables", response_model=APIResponse)
async def get_batch_table_schemas(
    database_name: str,
    request: GetBatchTableSchemasRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get schemas for multiple tables at once.
    
    Returns schema information for all specified tables.
    More efficient than calling get_table_schema multiple times.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    results = {}
    not_found = []
    
    for table_name in request.table_names:
        table_info = await db_context.get_schema_info(table_name)
        if table_info:
            results[table_name] = table_info_to_response(table_info)
        else:
            not_found.append(table_name)
    
    return APIResponse(
        success=True,
        data={
            "tables": results,
            "not_found": not_found
        }
    )


@router.get("/{database_name}/search/tables", response_model=APIResponse)
async def search_tables(
    database_name: str,
    search_term: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Search for tables by name pattern.
    
    Finds tables matching the search term (supports comma-separated terms).
    Returns up to 20 matching tables with their schemas.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    # Split search term by commas and whitespace
    search_terms = [term.strip() for term in search_term.replace(',', ' ').split()]
    search_terms = [term for term in search_terms if term]
    
    if not search_terms:
        raise HTTPException(status_code=400, detail="No valid search terms provided")
    
    # Track matching tables
    matching_tables = set()
    
    for term in search_terms:
        tables = await db_context.search_tables(term, limit=20)
        matching_tables.update(tables)
    
    matching_tables = list(matching_tables)[:20]
    
    if not matching_tables:
        return APIResponse(
            success=True,
            message=f"No tables found matching: {', '.join(search_terms)}",
            data={"tables": [], "total": 0}
        )
    
    # Load schemas for matching tables
    results = {}
    for table_name in matching_tables:
        table_info = await db_context.get_schema_info(table_name)
        if table_info:
            results[table_name] = table_info_to_response(table_info)
    
    return APIResponse(
        success=True,
        data={
            "tables": results,
            "total": len(results),
            "search_terms": search_terms
        }
    )


@router.get("/{database_name}/search/columns", response_model=APIResponse)
async def search_columns(
    database_name: str,
    search_term: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Search for columns across all tables.
    
    Finds columns matching the search term (substring match).
    Returns up to 50 matches with their hosting tables.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        matching_columns = await db_context.search_columns(search_term, limit=50)
        
        if not matching_columns:
            return APIResponse(
                success=True,
                message=f"No columns found matching '{search_term}'",
                data={"columns": {}, "total": 0}
            )
        
        return APIResponse(
            success=True,
            data={
                "columns": matching_columns,
                "total": len(matching_columns),
                "search_term": search_term
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error searching columns: {str(e)}")


@router.get("/{database_name}/info", response_model=APIResponse)
async def get_db_info(
    database_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get database version and schema information.
    
    Returns Oracle version, schema context, and additional version details.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        db_info = await db_context.get_database_info()
        return APIResponse(success=True, data=db_info)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error getting database info: {str(e)}")


@router.post("/{database_name}/rebuild-cache", response_model=APIResponse)
async def rebuild_schema_cache(
    database_name: str,
    request: RebuildCacheRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Rebuild the schema cache for a database.
    
    Forces a full refresh of the schema index.
    Use after DDL changes that add/drop/rename tables.
    
    Set fetch_all_metadata=True for comprehensive indexing (slower but complete).
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        start_time = time.time()
        
        await db_context.rebuild_cache(fetch_all_metadata=request.fetch_all_metadata)
        
        elapsed = time.time() - start_time
        cache_size = len(db_context.schema_manager.cache.all_table_names) if db_context.schema_manager.cache else 0
        fully_loaded = sum(1 for t in db_context.schema_manager.cache.tables.values() if t.fully_loaded) if db_context.schema_manager.cache else 0
        
        return APIResponse(
            success=True,
            message=f"Schema cache rebuilt in {elapsed:.2f} seconds",
            data={
                "tables_indexed": cache_size,
                "fully_loaded": fully_loaded,
                "fetch_all_metadata": request.fetch_all_metadata,
                "elapsed_seconds": round(elapsed, 2)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rebuild cache: {str(e)}")
