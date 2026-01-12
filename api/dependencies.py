"""Dependencies for FastAPI routes."""

from typing import Annotated
from fastapi import Depends, HTTPException, Request

from db_context import MultiDatabaseContext, DatabaseContext


async def get_multi_db_context(request: Request) -> MultiDatabaseContext:
    """Get the MultiDatabaseContext from app state."""
    multi_ctx = getattr(request.app.state, "multi_db_context", None)
    if multi_ctx is None:
        raise HTTPException(
            status_code=503,
            detail="Database context not initialized. Server may still be starting up."
        )
    return multi_ctx


def get_database_context(db_name: str, multi_ctx: MultiDatabaseContext) -> DatabaseContext:
    """Validate and return a specific database context."""
    try:
        return multi_ctx.get_database(db_name)
    except ValueError as e:
        available = ", ".join(multi_ctx.list_databases())
        raise HTTPException(
            status_code=404,
            detail=f"{str(e)}. Available databases: {available}"
        )


# Type aliases for dependency injection
MultiDBContextDep = Annotated[MultiDatabaseContext, Depends(get_multi_db_context)]
"""Dependencies for FastAPI routes."""

from typing import Annotated
from fastapi import Depends, HTTPException, Request

from db_context import MultiDatabaseContext, DatabaseContext


async def get_multi_db_context(request: Request) -> MultiDatabaseContext:
    """Get the MultiDatabaseContext from app state."""
    multi_ctx = getattr(request.app.state, "multi_db_context", None)
    if multi_ctx is None:
        raise HTTPException(
            status_code=503,
            detail="Database context not initialized. Server may still be starting up."
        )
    return multi_ctx


def get_database_context(db_name: str, multi_ctx: MultiDatabaseContext) -> DatabaseContext:
    """Validate and return a specific database context."""
    try:
        return multi_ctx.get_database(db_name)
    except ValueError as e:
        available = ", ".join(multi_ctx.list_databases())
        raise HTTPException(
            status_code=404,
            detail=f"{str(e)}. Available databases: {available}"
        )


# Type aliases for dependency injection
MultiDBContextDep = Annotated[MultiDatabaseContext, Depends(get_multi_db_context)]
