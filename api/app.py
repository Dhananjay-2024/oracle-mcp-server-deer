"""FastAPI application wrapping all MCP server tools as REST API endpoints."""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from db_context import MultiDatabaseContext

# Import routers
from api.routes import crud, databases, schema, metadata, plsql, sql

# Load environment variables
load_dotenv()


def parse_database_configs():
    """Parse database configurations from environment variables."""
    from typing import Dict, Any
    
    db_names_str = os.getenv('DB_NAMES', '')
    if not db_names_str:
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
    databases: Dict[str, Dict[str, Any]] = {}
    
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
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage FastAPI application lifecycle."""
    print("Initializing FastAPI Oracle Database Server", file=sys.stderr)
    
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
        
        # Store context in app state for access by routes
        app.state.multi_db_context = multi_db_context
        
        yield
    finally:
        print("Closing database connections...", file=sys.stderr)
        await multi_db_context.close()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Oracle MCP Server API",
        description="""
REST API wrapper for Oracle MCP Server tools.

This API provides programmatic access to all Oracle database tools available in the MCP server,
including:

- **CRUD Operations**: Create, read, update, delete rows in tables
- **Database Management**: List databases, get database info
- **Schema Discovery**: Get table schemas, search tables/columns, rebuild cache
- **Table Metadata**: Get constraints, indexes, relationships, dependencies
- **PL/SQL & Types**: List procedures/functions, get source code, list user types
- **SQL Execution**: Execute queries, write operations, explain plans, sample queries

All endpoints require the database name as a path parameter.
        """,
        version="0.1.5",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers
    app.include_router(databases.router)
    app.include_router(crud.router)
    app.include_router(schema.router)
    app.include_router(metadata.router)
    app.include_router(plsql.router)
    app.include_router(sql.router)
    
    # Health check endpoint
    @app.get("/health", tags=["Health"])
    async def health_check():
        """Health check endpoint."""
        return JSONResponse({"status": "healthy"})
    
    # Root endpoint
    @app.get("/", tags=["Root"])
    async def root():
        """Root endpoint with API information."""
        return {
            "name": "Oracle MCP Server API",
            "version": "0.1.5",
            "docs": "/docs",
            "openapi": "/openapi.json",
            "health": "/health"
        }
    
    return app


# Create the app instance for uvicorn
app = create_app()


async def run_fastapi_server(host: str = "127.0.0.1", port: int = 8080):
    """Run the FastAPI server with uvicorn."""
    import uvicorn
    
    print(f"Starting FastAPI server on {host}:{port}", file=sys.stderr)
    print(f"API Documentation: http://{host}:{port}/docs", file=sys.stderr)
    print(f"Health check: http://{host}:{port}/health", file=sys.stderr)
    
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    import asyncio
    import argparse
    
    parser = argparse.ArgumentParser(description="Oracle MCP Server FastAPI")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    args = parser.parse_args()
    
    asyncio.run(run_fastapi_server(args.host, args.port))
