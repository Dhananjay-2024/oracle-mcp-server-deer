"""Database management routes - list databases and get info."""

from fastapi import APIRouter

from api.models import (
    APIResponse,
    DatabaseInfo,
    DatabaseListResponse,
    AllDatabaseInfoResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context

router = APIRouter(prefix="/databases", tags=["Database Management"])


@router.get("", response_model=APIResponse)
async def list_databases(multi_ctx: MultiDBContextDep) -> APIResponse:
    """List all available Oracle databases.
    
    Returns the names of all configured databases that you can query.
    """
    databases = multi_ctx.list_databases()
    return APIResponse(
        success=True,
        data=DatabaseListResponse(databases=databases)
    )


@router.get("/info", response_model=APIResponse)
async def get_all_database_info(multi_ctx: MultiDBContextDep) -> APIResponse:
    """Get vendor information for all configured databases.
    
    Returns Oracle version and schema information for each database.
    """
    info = await multi_ctx.get_all_database_info()
    
    databases_info = {}
    for db_name, db_info in info.items():
        databases_info[db_name] = DatabaseInfo(
            name=db_name,
            vendor=db_info.get("vendor"),
            version=db_info.get("version"),
            db_schema=db_info.get("schema"),
            error=db_info.get("error")
        )
    
    return APIResponse(
        success=True,
        data=AllDatabaseInfoResponse(databases=databases_info)
    )


@router.get("/{database_name}/info", response_model=APIResponse)
async def get_database_vendor_info(
    database_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get vendor information for a specific database.
    
    Returns Oracle version and schema information.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        db_info = await db_context.get_database_info()
        
        return APIResponse(
            success=True,
            data=DatabaseInfo(
                name=database_name,
                vendor=db_info.get("vendor"),
                version=db_info.get("version"),
                db_schema=db_info.get("schema"),
                error=db_info.get("error")
            )
        )
    except Exception as e:
        return APIResponse(
            success=False,
            error=f"Error retrieving database info: {str(e)}"
        )
