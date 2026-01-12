
"""CRUD operation routes - create, read, update, delete rows."""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict

from api.models import (
    APIResponse,
    CreateRowRequest,
    ReadRowsRequest,
    UpdateRowsRequest,
    DeleteRowsRequest,
    SQLResultResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context
from db_context.schema.formatter import format_sql_query_result

router = APIRouter(prefix="/crud", tags=["CRUD Operations"])


@router.post("/{database_name}/create", response_model=APIResponse)
async def create_row(
    database_name: str,
    request: CreateRowRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Insert a new row into a table.
    
    Creates a single row with the provided column-value pairs.
    Requires write mode to be enabled for the database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    columns = ', '.join(request.data.keys())
    placeholders = ', '.join(f":{k}" for k in request.data.keys())
    sql = f"INSERT INTO {request.table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        result = await db_context.run_sql_query(sql, params=request.data)
        return APIResponse(
            success=True,
            message=f"Inserted row into '{request.table_name}' in '{database_name}'. {result.get('message', '')}",
            data={"row_count": result.get("row_count", 1)}
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error inserting row: {str(e)}")


@router.post("/{database_name}/read", response_model=APIResponse)
async def read_rows(
    database_name: str,
    request: ReadRowsRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Read rows from a table with optional filters.
    
    Returns matching rows based on the provided filter conditions.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    where = ''
    params: Dict[str, Any] = {}
    if request.filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :{k}" for k in request.filters.keys())
        params = request.filters
    
    sql = f"SELECT * FROM {request.table_name}{where}"
    
    try:
        result = await db_context.run_sql_query(sql, params=params, max_rows=request.max_rows)
        
        if not result.get("rows"):
            return APIResponse(
                success=True,
                message=f"No rows found in '{request.table_name}' in '{database_name}'.",
                data=SQLResultResponse(columns=result.get("columns", []), rows=[], row_count=0)
            )
        
        return APIResponse(
            success=True,
            data=SQLResultResponse(
                columns=result.get("columns", []),
                rows=result.get("rows", []),
                row_count=len(result.get("rows", []))
            )
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading rows: {str(e)}")


@router.post("/{database_name}/update", response_model=APIResponse)
async def update_rows(
    database_name: str,
    request: UpdateRowsRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Update rows in a table matching filters.
    
    Updates matching rows with the provided column-value pairs.
    Requires write mode to be enabled for the database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    set_clause = ', '.join(f"{k} = :set_{k}" for k in request.updates.keys())
    params = {f"set_{k}": v for k, v in request.updates.items()}
    
    where = ''
    if request.filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :where_{k}" for k in request.filters.keys())
        params.update({f"where_{k}": v for k, v in request.filters.items()})
    
    sql = f"UPDATE {request.table_name} SET {set_clause}{where}"
    
    try:
        result = await db_context.run_sql_query(sql, params=params)
        return APIResponse(
            success=True,
            message=f"Updated rows in '{request.table_name}' in '{database_name}'. {result.get('message', '')}",
            data={"row_count": result.get("row_count", 0)}
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error updating rows: {str(e)}")


@router.post("/{database_name}/delete", response_model=APIResponse)
async def delete_rows(
    database_name: str,
    request: DeleteRowsRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Delete rows from a table matching filters.
    
    Deletes matching rows based on the provided filter conditions.
    Requires write mode to be enabled for the database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    where = ''
    params: Dict[str, Any] = {}
    if request.filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :{k}" for k in request.filters.keys())
        params = request.filters
    
    sql = f"DELETE FROM {request.table_name}{where}"
    
    try:
        result = await db_context.run_sql_query(sql, params=params)
        return APIResponse(
            success=True,
            message=f"Deleted rows from '{request.table_name}' in '{database_name}'. {result.get('message', '')}",
            data={"row_count": result.get("row_count", 0)}
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error deleting rows: {str(e)}")
"""CRUD operation routes - create, read, update, delete rows."""

from fastapi import APIRouter, HTTPException
from typing import Any, Dict

from api.models import (
    APIResponse,
    CreateRowRequest,
    ReadRowsRequest,
    UpdateRowsRequest,
    DeleteRowsRequest,
    SQLResultResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context
from db_context.schema.formatter import format_sql_query_result

router = APIRouter(prefix="/crud", tags=["CRUD Operations"])


@router.post("/{database_name}/create", response_model=APIResponse)
async def create_row(
    database_name: str,
    request: CreateRowRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Insert a new row into a table.
    
    Creates a single row with the provided column-value pairs.
    Requires write mode to be enabled for the database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    columns = ', '.join(request.data.keys())
    placeholders = ', '.join(f":{k}" for k in request.data.keys())
    sql = f"INSERT INTO {request.table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        result = await db_context.run_sql_query(sql, params=request.data)
        return APIResponse(
            success=True,
            message=f"Inserted row into '{request.table_name}' in '{database_name}'. {result.get('message', '')}",
            data={"row_count": result.get("row_count", 1)}
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error inserting row: {str(e)}")


@router.post("/{database_name}/read", response_model=APIResponse)
async def read_rows(
    database_name: str,
    request: ReadRowsRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Read rows from a table with optional filters.
    
    Returns matching rows based on the provided filter conditions.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    where = ''
    params: Dict[str, Any] = {}
    if request.filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :{k}" for k in request.filters.keys())
        params = request.filters
    
    sql = f"SELECT * FROM {request.table_name}{where}"
    
    try:
        result = await db_context.run_sql_query(sql, params=params, max_rows=request.max_rows)
        
        if not result.get("rows"):
            return APIResponse(
                success=True,
                message=f"No rows found in '{request.table_name}' in '{database_name}'.",
                data=SQLResultResponse(columns=result.get("columns", []), rows=[], row_count=0)
            )
        
        return APIResponse(
            success=True,
            data=SQLResultResponse(
                columns=result.get("columns", []),
                rows=result.get("rows", []),
                row_count=len(result.get("rows", []))
            )
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading rows: {str(e)}")


@router.post("/{database_name}/update", response_model=APIResponse)
async def update_rows(
    database_name: str,
    request: UpdateRowsRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Update rows in a table matching filters.
    
    Updates matching rows with the provided column-value pairs.
    Requires write mode to be enabled for the database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    set_clause = ', '.join(f"{k} = :set_{k}" for k in request.updates.keys())
    params = {f"set_{k}": v for k, v in request.updates.items()}
    
    where = ''
    if request.filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :where_{k}" for k in request.filters.keys())
        params.update({f"where_{k}": v for k, v in request.filters.items()})
    
    sql = f"UPDATE {request.table_name} SET {set_clause}{where}"
    
    try:
        result = await db_context.run_sql_query(sql, params=params)
        return APIResponse(
            success=True,
            message=f"Updated rows in '{request.table_name}' in '{database_name}'. {result.get('message', '')}",
            data={"row_count": result.get("row_count", 0)}
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error updating rows: {str(e)}")


@router.post("/{database_name}/delete", response_model=APIResponse)
async def delete_rows(
    database_name: str,
    request: DeleteRowsRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Delete rows from a table matching filters.
    
    Deletes matching rows based on the provided filter conditions.
    Requires write mode to be enabled for the database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    where = ''
    params: Dict[str, Any] = {}
    if request.filters:
        where = ' WHERE ' + ' AND '.join(f"{k} = :{k}" for k in request.filters.keys())
        params = request.filters
    
    sql = f"DELETE FROM {request.table_name}{where}"
    
    try:
        result = await db_context.run_sql_query(sql, params=params)
        return APIResponse(
            success=True,
            message=f"Deleted rows from '{request.table_name}' in '{database_name}'. {result.get('message', '')}",
            data={"row_count": result.get("row_count", 0)}
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error deleting rows: {str(e)}")
