"""Table metadata routes - constraints, indexes, relationships, dependencies."""

from fastapi import APIRouter, HTTPException

from api.models import (
    APIResponse,
    ConstraintInfo,
    IndexInfo,
    RelatedTablesResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context

router = APIRouter(prefix="/metadata", tags=["Table Metadata"])


@router.get("/{database_name}/constraints/{table_name}", response_model=APIResponse)
async def get_table_constraints(
    database_name: str,
    table_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get constraints for a table.
    
    Returns PK, FK, UNIQUE, and CHECK constraints.
    Results are cached with TTL for performance.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        constraints = await db_context.get_table_constraints(table_name)
        
        if not constraints:
            return APIResponse(
                success=True,
                message=f"No constraints found for table '{table_name}'",
                data={"constraints": [], "table_name": table_name}
            )
        
        constraint_list = [
            ConstraintInfo(
                name=c.get("name", "UNNAMED"),
                type=c.get("type", "UNKNOWN"),
                columns=c.get("columns", []),
                references=c.get("references"),
                condition=c.get("condition"),
            )
            for c in constraints
        ]
        
        return APIResponse(
            success=True,
            data={
                "constraints": constraint_list,
                "table_name": table_name,
                "count": len(constraint_list)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving constraints: {str(e)}")


@router.get("/{database_name}/indexes/{table_name}", response_model=APIResponse)
async def get_table_indexes(
    database_name: str,
    table_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get indexes for a table.
    
    Returns index names, columns, uniqueness, and status.
    Results are cached with TTL for performance.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        indexes = await db_context.get_table_indexes(table_name)
        
        if not indexes:
            return APIResponse(
                success=True,
                message=f"No indexes found for table '{table_name}'",
                data={"indexes": [], "table_name": table_name}
            )
        
        index_list = [
            IndexInfo(
                name=idx.get("name", "UNNAMED"),
                columns=idx.get("columns", []),
                unique=idx.get("unique", False),
                tablespace=idx.get("tablespace"),
                status=idx.get("status"),
            )
            for idx in indexes
        ]
        
        return APIResponse(
            success=True,
            data={
                "indexes": index_list,
                "table_name": table_name,
                "count": len(index_list)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving indexes: {str(e)}")


@router.get("/{database_name}/related/{table_name}", response_model=APIResponse)
async def get_related_tables(
    database_name: str,
    table_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get tables related by foreign keys.
    
    Returns tables referenced by this table (outgoing FK) and
    tables that reference this table (incoming FK).
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        related = await db_context.get_related_tables(table_name)
        
        response_data = RelatedTablesResponse(
            table_name=table_name,
            referenced_tables=related.get("referenced_tables", []),
            referencing_tables=related.get("referencing_tables", []),
        )
        
        if not response_data.referenced_tables and not response_data.referencing_tables:
            return APIResponse(
                success=True,
                message=f"No related tables found for '{table_name}'",
                data=response_data
            )
        
        return APIResponse(success=True, data=response_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error getting related tables: {str(e)}")


@router.get("/{database_name}/dependencies/{object_name}", response_model=APIResponse)
async def get_dependent_objects(
    database_name: str,
    object_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get objects that depend on a table or object.
    
    Returns views, PL/SQL objects, and triggers that reference the specified object.
    Useful for impact analysis before making changes.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        dependencies = await db_context.get_dependent_objects(object_name.upper())
        
        if not dependencies:
            return APIResponse(
                success=True,
                message=f"No objects found that depend on '{object_name}'",
                data={"dependencies": [], "object_name": object_name}
            )
        
        return APIResponse(
            success=True,
            data={
                "dependencies": dependencies,
                "object_name": object_name,
                "count": len(dependencies)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving dependencies: {str(e)}")
