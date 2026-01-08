"""PL/SQL and user types routes."""

from typing import Optional
from fastapi import APIRouter, HTTPException

from api.models import (
    APIResponse,
    PLSQLObjectInfo,
    UserTypeInfo,
)
from api.dependencies import MultiDBContextDep, get_database_context

router = APIRouter(prefix="/plsql", tags=["PL/SQL & Types"])


@router.get("/{database_name}/objects/{object_type}", response_model=APIResponse)
async def get_pl_sql_objects(
    database_name: str,
    object_type: str,
    name_pattern: Optional[str] = None,
    multi_ctx: MultiDBContextDep = None,
) -> APIResponse:
    """List PL/SQL objects by type.
    
    Supported object types: PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER, TYPE, etc.
    Optionally filter by name pattern (LIKE syntax).
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        objects = await db_context.get_pl_sql_objects(object_type.upper(), name_pattern)
        
        if not objects:
            pattern_msg = f" matching '{name_pattern}'" if name_pattern else ""
            return APIResponse(
                success=True,
                message=f"No {object_type.upper()} objects found{pattern_msg}",
                data={"objects": [], "object_type": object_type.upper()}
            )
        
        object_list = [
            PLSQLObjectInfo(
                name=obj.get("name", ""),
                type=obj.get("type", object_type.upper()),
                owner=obj.get("owner"),
                status=obj.get("status"),
                created=obj.get("created"),
                last_modified=obj.get("last_modified"),
            )
            for obj in objects
        ]
        
        return APIResponse(
            success=True,
            data={
                "objects": object_list,
                "object_type": object_type.upper(),
                "count": len(object_list),
                "name_pattern": name_pattern
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving PL/SQL objects: {str(e)}")


@router.get("/{database_name}/source/{object_type}/{object_name}", response_model=APIResponse)
async def get_object_source(
    database_name: str,
    object_type: str,
    object_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get DDL/source code for a PL/SQL object.
    
    Returns the complete source code or DDL for the specified object.
    Useful for reviewing logic or exporting definitions.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        source = await db_context.get_object_source(object_type.upper(), object_name.upper())
        
        if not source:
            raise HTTPException(
                status_code=404,
                detail=f"No source found for {object_type} {object_name}"
            )
        
        return APIResponse(
            success=True,
            data={
                "source": source,
                "object_type": object_type.upper(),
                "object_name": object_name.upper()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving object source: {str(e)}")


@router.get("/{database_name}/types", response_model=APIResponse)
async def get_user_types(
    database_name: str,
    type_pattern: Optional[str] = None,
    multi_ctx: MultiDBContextDep = None,
) -> APIResponse:
    """List user-defined types.
    
    Returns custom types including OBJECT types with their attributes.
    Optionally filter by name pattern (LIKE syntax).
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        types = await db_context.get_user_defined_types(type_pattern)
        
        if not types:
            pattern_msg = f" matching '{type_pattern}'" if type_pattern else ""
            return APIResponse(
                success=True,
                message=f"No user-defined types found{pattern_msg}",
                data={"types": []}
            )
        
        type_list = [
            UserTypeInfo(
                name=typ.get("name", ""),
                type_category=typ.get("type_category", ""),
                owner=typ.get("owner"),
                attributes=typ.get("attributes"),
            )
            for typ in types
        ]
        
        return APIResponse(
            success=True,
            data={
                "types": type_list,
                "count": len(type_list),
                "type_pattern": type_pattern
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving user types: {str(e)}")
