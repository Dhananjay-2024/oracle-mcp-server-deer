"""Pydantic models for FastAPI request/response validation."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ============================================================================
# Common Response Models
# ============================================================================

class APIResponse(BaseModel):
    """Standard API response wrapper."""
    success: bool = True
    data: Any = None
    message: Optional[str] = None
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    """Error response model."""
    success: bool = False
    error: str
    detail: Optional[str] = None


# ============================================================================
# CRUD Request Models
# ============================================================================

class CreateRowRequest(BaseModel):
    """Request model for creating a row."""
    table_name: str = Field(..., description="Name of the table")
    data: Dict[str, Any] = Field(..., description="Column-value pairs to insert")


class ReadRowsRequest(BaseModel):
    """Request model for reading rows."""
    table_name: str = Field(..., description="Name of the table")
    filters: Optional[Dict[str, Any]] = Field(None, description="Column-value pairs for WHERE clause")
    max_rows: int = Field(100, description="Maximum number of rows to return", ge=1, le=10000)


class UpdateRowsRequest(BaseModel):
    """Request model for updating rows."""
    table_name: str = Field(..., description="Name of the table")
    updates: Dict[str, Any] = Field(..., description="Column-value pairs to update")
    filters: Optional[Dict[str, Any]] = Field(None, description="Column-value pairs for WHERE clause")


class DeleteRowsRequest(BaseModel):
    """Request model for deleting rows."""
    table_name: str = Field(..., description="Name of the table")
    filters: Optional[Dict[str, Any]] = Field(None, description="Column-value pairs for WHERE clause")


# ============================================================================
# Schema Request Models
# ============================================================================

class GetTableSchemaRequest(BaseModel):
    """Request model for getting table schema."""
    table_name: str = Field(..., description="Name of the table")


class GetBatchTableSchemasRequest(BaseModel):
    """Request model for getting multiple table schemas."""
    table_names: List[str] = Field(..., description="List of table names", min_length=1, max_length=50)


class SearchTablesRequest(BaseModel):
    """Request model for searching tables."""
    search_term: str = Field(..., description="Pattern to search for in table names")


class SearchColumnsRequest(BaseModel):
    """Request model for searching columns."""
    search_term: str = Field(..., description="Pattern to search for in column names")


class RebuildCacheRequest(BaseModel):
    """Request model for rebuilding schema cache."""
    fetch_all_metadata: bool = Field(False, description="Fetch complete metadata for all tables")


# ============================================================================
# Table Metadata Request Models
# ============================================================================

class GetTableConstraintsRequest(BaseModel):
    """Request model for getting table constraints."""
    table_name: str = Field(..., description="Name of the table")


class GetTableIndexesRequest(BaseModel):
    """Request model for getting table indexes."""
    table_name: str = Field(..., description="Name of the table")


class GetRelatedTablesRequest(BaseModel):
    """Request model for getting related tables."""
    table_name: str = Field(..., description="Name of the table")


class GetDependentObjectsRequest(BaseModel):
    """Request model for getting dependent objects."""
    object_name: str = Field(..., description="Name of the object")


# ============================================================================
# PL/SQL Request Models
# ============================================================================

class GetPLSQLObjectsRequest(BaseModel):
    """Request model for listing PL/SQL objects."""
    object_type: str = Field(..., description="Type of object (PROCEDURE, FUNCTION, PACKAGE, TRIGGER, etc.)")
    name_pattern: Optional[str] = Field(None, description="LIKE pattern for object names")


class GetObjectSourceRequest(BaseModel):
    """Request model for getting object source."""
    object_type: str = Field(..., description="Type of object (PROCEDURE, FUNCTION, PACKAGE, etc.)")
    object_name: str = Field(..., description="Name of the object")


class GetUserTypesRequest(BaseModel):
    """Request model for listing user-defined types."""
    type_pattern: Optional[str] = Field(None, description="LIKE pattern for type names")


# ============================================================================
# SQL Execution Request Models
# ============================================================================

class ExecuteSQLRequest(BaseModel):
    """Request model for executing SQL."""
    sql: str = Field(..., description="SQL statement to execute")
    max_rows: int = Field(100, description="Maximum rows to return for SELECT", ge=1, le=10000)


class ExecuteWriteSQLRequest(BaseModel):
    """Request model for executing write SQL."""
    sql: str = Field(..., description="DML or DDL statement to execute")


class ExplainQueryRequest(BaseModel):
    """Request model for explaining query plan."""
    sql: str = Field(..., description="SQL query to analyze")


# ============================================================================
# Response Data Models
# ============================================================================

class DatabaseInfo(BaseModel):
    """Database information model."""
    name: str
    vendor: Optional[str] = None
    version: Optional[str] = None
    db_schema: Optional[str] = Field(None, alias="schema")
    error: Optional[str] = None
    
    model_config = {"populate_by_name": True}


class DatabaseListResponse(BaseModel):
    """Response model for listing databases."""
    databases: List[str]


class AllDatabaseInfoResponse(BaseModel):
    """Response model for all database info."""
    databases: Dict[str, DatabaseInfo]


class ColumnInfo(BaseModel):
    """Column information model."""
    name: str
    type: str
    nullable: bool
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default: Optional[str] = None


class ConstraintInfo(BaseModel):
    """Constraint information model."""
    name: str
    type: str
    columns: List[str]
    references: Optional[Dict[str, Any]] = None
    condition: Optional[str] = None


class IndexInfo(BaseModel):
    """Index information model."""
    name: str
    columns: List[str]
    unique: bool = False
    tablespace: Optional[str] = None
    status: Optional[str] = None


class TableSchemaResponse(BaseModel):
    """Response model for table schema."""
    table_name: str
    columns: List[ColumnInfo]
    relationships: Dict[str, List[Dict[str, Any]]] = {}
    constraints: Optional[List[ConstraintInfo]] = None
    indexes: Optional[List[IndexInfo]] = None
    table_stats: Optional[Dict[str, Any]] = None
    comments: Optional[Dict[str, Any]] = None


class SQLResultResponse(BaseModel):
    """Response model for SQL query results."""
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    row_count: int = 0
    message: Optional[str] = None


class ExecutionPlanResponse(BaseModel):
    """Response model for query execution plan."""
    execution_plan: List[str]
    optimization_suggestions: Optional[List[str]] = None


class PLSQLObjectInfo(BaseModel):
    """PL/SQL object information model."""
    name: str
    type: str
    owner: Optional[str] = None
    status: Optional[str] = None
    created: Optional[str] = None
    last_modified: Optional[str] = None


class UserTypeInfo(BaseModel):
    """User-defined type information model."""
    name: str
    type_category: str
    owner: Optional[str] = None
    attributes: Optional[List[Dict[str, str]]] = None


class RelatedTablesResponse(BaseModel):
    """Response model for related tables."""
    table_name: str
    referenced_tables: List[str]
    referencing_tables: List[str]


class SampleQueryInfo(BaseModel):
    """Sample query information model."""
    level: str
    number: int
    title: str
    description: str
    query: str


class SampleQueriesResponse(BaseModel):
    """Response model for sample queries."""
    database_name: str
    queries: List[SampleQueryInfo]


# ============================================================================
# Data Quality Rules Models
# ============================================================================

class GenerateDQRulesRequest(BaseModel):
    """Request model for generating DQ rules."""
    table_name: str = Field(..., description="Name of the table to generate DQ rules for")
    num_rules: int = Field(10, description="Number of rules to generate (1-50)", ge=1, le=50)


class DQRuleInfo(BaseModel):
    """Data quality rule information model."""
    rule_id: str
    rule_type: str
    category: Optional[str] = Field(None, description="Rule category: business_entity, business_attribute, data_dependency, data_validity")
    target_columns: List[str]
    params: Optional[Dict[str, Any]] = None
    enabled: bool = True


class DQRulesResponse(BaseModel):
    """Response model for DQ rules generation."""
    rule_set_id: str
    table_name: str
    database_name: str
    total_rules: int
    rule_category_summary: Optional[Dict[str, int]] = Field(None, description="Summary of rules by category")
    rules: List[DQRuleInfo]

# ============================================================================
# DQ Rule Validation Models
# ============================================================================

class ApplyDQRulesRequest(BaseModel):
    """Request model for applying DQ rules validation."""
    table_name: str = Field(..., description="Name of the table to validate")
    rules: List[DQRuleInfo] = Field(..., description="List of DQ rules to apply for validation")
    store_results: bool = Field(True, description="Whether to store results in DQ_VALIDATION_RESULTS table")
    sample_percent: Optional[int] = Field(None, description="Sample percentage for large tables (1-100). If None, validates all rows", ge=1, le=100)
    sample_failed_rows: int = Field(5, description="Number of sample failed rows to include per rule", ge=0, le=100)


class DQValidationResultInfo(BaseModel):
    """Individual rule validation result."""
    rule_id: str
    rule_type: str
    category: Optional[str] = None
    target_columns: List[str]
    status: str = Field(..., description="PASSED, FAILED, or ERROR")
    total_rows: int = Field(0, description="Total rows checked")
    passed_rows: int = Field(0, description="Rows that passed validation")
    failed_rows: int = Field(0, description="Rows that failed validation")
    pass_rate: float = Field(0.0, description="Percentage of rows that passed (0-100)")
    error_message: Optional[str] = Field(None, description="Error message if status is ERROR")
    sample_failures: Optional[List[Dict[str, Any]]] = Field(None, description="Sample of failed rows")
    execution_time_ms: float = Field(0.0, description="Time taken to execute validation in milliseconds")


class DQValidationResponse(BaseModel):
    """Response model for DQ rules validation."""
    validation_run_id: str
    table_name: str
    database_name: str
    validation_timestamp: str
    total_rules_applied: int
    rules_passed: int
    rules_failed: int
    rules_errored: int
    overall_status: str = Field(..., description="PASSED (all rules passed), FAILED (some failed), or ERROR")
    overall_pass_rate: float = Field(0.0, description="Average pass rate across all rules")
    sample_percent_used: Optional[int] = Field(None, description="Sample percentage used for validation")
    results_stored: bool = Field(False, description="Whether results were stored in the database")
    storage_table: Optional[str] = Field(None, description="Name of the table where results are stored")
    storage_warning: Optional[str] = Field(None, description="Warning message if storage was requested but not possible")
"""Pydantic models for FastAPI request/response validation."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ============================================================================
# Common Response Models
# ============================================================================

class APIResponse(BaseModel):
    """Standard API response wrapper."""
    success: bool = True
    data: Any = None
    message: Optional[str] = None
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    """Error response model."""
    success: bool = False
    error: str
    detail: Optional[str] = None


# ============================================================================
# CRUD Request Models
# ============================================================================

class CreateRowRequest(BaseModel):
    """Request model for creating a row."""
    table_name: str = Field(..., description="Name of the table")
    data: Dict[str, Any] = Field(..., description="Column-value pairs to insert")


class ReadRowsRequest(BaseModel):
    """Request model for reading rows."""
    table_name: str = Field(..., description="Name of the table")
    filters: Optional[Dict[str, Any]] = Field(None, description="Column-value pairs for WHERE clause")
    max_rows: int = Field(100, description="Maximum number of rows to return", ge=1, le=10000)


class UpdateRowsRequest(BaseModel):
    """Request model for updating rows."""
    table_name: str = Field(..., description="Name of the table")
    updates: Dict[str, Any] = Field(..., description="Column-value pairs to update")
    filters: Optional[Dict[str, Any]] = Field(None, description="Column-value pairs for WHERE clause")


class DeleteRowsRequest(BaseModel):
    """Request model for deleting rows."""
    table_name: str = Field(..., description="Name of the table")
    filters: Optional[Dict[str, Any]] = Field(None, description="Column-value pairs for WHERE clause")


# ============================================================================
# Schema Request Models
# ============================================================================

class GetTableSchemaRequest(BaseModel):
    """Request model for getting table schema."""
    table_name: str = Field(..., description="Name of the table")


class GetBatchTableSchemasRequest(BaseModel):
    """Request model for getting multiple table schemas."""
    table_names: List[str] = Field(..., description="List of table names", min_length=1, max_length=50)


class SearchTablesRequest(BaseModel):
    """Request model for searching tables."""
    search_term: str = Field(..., description="Pattern to search for in table names")


class SearchColumnsRequest(BaseModel):
    """Request model for searching columns."""
    search_term: str = Field(..., description="Pattern to search for in column names")


class RebuildCacheRequest(BaseModel):
    """Request model for rebuilding schema cache."""
    fetch_all_metadata: bool = Field(False, description="Fetch complete metadata for all tables")


# ============================================================================
# Table Metadata Request Models
# ============================================================================

class GetTableConstraintsRequest(BaseModel):
    """Request model for getting table constraints."""
    table_name: str = Field(..., description="Name of the table")


class GetTableIndexesRequest(BaseModel):
    """Request model for getting table indexes."""
    table_name: str = Field(..., description="Name of the table")


class GetRelatedTablesRequest(BaseModel):
    """Request model for getting related tables."""
    table_name: str = Field(..., description="Name of the table")


class GetDependentObjectsRequest(BaseModel):
    """Request model for getting dependent objects."""
    object_name: str = Field(..., description="Name of the object")


# ============================================================================
# PL/SQL Request Models
# ============================================================================

class GetPLSQLObjectsRequest(BaseModel):
    """Request model for listing PL/SQL objects."""
    object_type: str = Field(..., description="Type of object (PROCEDURE, FUNCTION, PACKAGE, TRIGGER, etc.)")
    name_pattern: Optional[str] = Field(None, description="LIKE pattern for object names")


class GetObjectSourceRequest(BaseModel):
    """Request model for getting object source."""
    object_type: str = Field(..., description="Type of object (PROCEDURE, FUNCTION, PACKAGE, etc.)")
    object_name: str = Field(..., description="Name of the object")


class GetUserTypesRequest(BaseModel):
    """Request model for listing user-defined types."""
    type_pattern: Optional[str] = Field(None, description="LIKE pattern for type names")


# ============================================================================
# SQL Execution Request Models
# ============================================================================

class ExecuteSQLRequest(BaseModel):
    """Request model for executing SQL."""
    sql: str = Field(..., description="SQL statement to execute")
    max_rows: int = Field(100, description="Maximum rows to return for SELECT", ge=1, le=10000)


class ExecuteWriteSQLRequest(BaseModel):
    """Request model for executing write SQL."""
    sql: str = Field(..., description="DML or DDL statement to execute")


class ExplainQueryRequest(BaseModel):
    """Request model for explaining query plan."""
    sql: str = Field(..., description="SQL query to analyze")


# ============================================================================
# Response Data Models
# ============================================================================

class DatabaseInfo(BaseModel):
    """Database information model."""
    name: str
    vendor: Optional[str] = None
    version: Optional[str] = None
    db_schema: Optional[str] = Field(None, alias="schema")
    error: Optional[str] = None
    
    model_config = {"populate_by_name": True}


class DatabaseListResponse(BaseModel):
    """Response model for listing databases."""
    databases: List[str]


class AllDatabaseInfoResponse(BaseModel):
    """Response model for all database info."""
    databases: Dict[str, DatabaseInfo]


class ColumnInfo(BaseModel):
    """Column information model."""
    name: str
    type: str
    nullable: bool
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default: Optional[str] = None


class ConstraintInfo(BaseModel):
    """Constraint information model."""
    name: str
    type: str
    columns: List[str]
    references: Optional[Dict[str, Any]] = None
    condition: Optional[str] = None


class IndexInfo(BaseModel):
    """Index information model."""
    name: str
    columns: List[str]
    unique: bool = False
    tablespace: Optional[str] = None
    status: Optional[str] = None


class TableSchemaResponse(BaseModel):
    """Response model for table schema."""
    table_name: str
    columns: List[ColumnInfo]
    relationships: Dict[str, List[Dict[str, Any]]] = {}
    constraints: Optional[List[ConstraintInfo]] = None
    indexes: Optional[List[IndexInfo]] = None
    table_stats: Optional[Dict[str, Any]] = None
    comments: Optional[Dict[str, Any]] = None


class SQLResultResponse(BaseModel):
    """Response model for SQL query results."""
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    row_count: int = 0
    message: Optional[str] = None


class ExecutionPlanResponse(BaseModel):
    """Response model for query execution plan."""
    execution_plan: List[str]
    optimization_suggestions: Optional[List[str]] = None


class PLSQLObjectInfo(BaseModel):
    """PL/SQL object information model."""
    name: str
    type: str
    owner: Optional[str] = None
    status: Optional[str] = None
    created: Optional[str] = None
    last_modified: Optional[str] = None


class UserTypeInfo(BaseModel):
    """User-defined type information model."""
    name: str
    type_category: str
    owner: Optional[str] = None
    attributes: Optional[List[Dict[str, str]]] = None


class RelatedTablesResponse(BaseModel):
    """Response model for related tables."""
    table_name: str
    referenced_tables: List[str]
    referencing_tables: List[str]


class SampleQueryInfo(BaseModel):
    """Sample query information model."""
    level: str
    number: int
    title: str
    description: str
    query: str


class SampleQueriesResponse(BaseModel):
    """Response model for sample queries."""
    database_name: str
    queries: List[SampleQueryInfo]


# ============================================================================
# Data Quality Rules Models
# ============================================================================

class GenerateDQRulesRequest(BaseModel):
    """Request model for generating DQ rules."""
    table_name: str = Field(..., description="Name of the table to generate DQ rules for")
    num_rules: int = Field(10, description="Number of rules to generate (1-50)", ge=1, le=50)


class DQRuleInfo(BaseModel):
    """Data quality rule information model."""
    rule_id: str
    rule_type: str
    category: Optional[str] = Field(None, description="Rule category: business_entity, business_attribute, data_dependency, data_validity")
    target_columns: List[str]
    params: Optional[Dict[str, Any]] = None
    enabled: bool = True


class DQRulesResponse(BaseModel):
    """Response model for DQ rules generation."""
    rule_set_id: str
    table_name: str
    database_name: str
    total_rules: int
    rule_category_summary: Optional[Dict[str, int]] = Field(None, description="Summary of rules by category")
    rules: List[DQRuleInfo]

# ============================================================================
# DQ Rule Validation Models
# ============================================================================

class ApplyDQRulesRequest(BaseModel):
    """Request model for applying DQ rules validation."""
    table_name: str = Field(..., description="Name of the table to validate")
    rules: List[DQRuleInfo] = Field(..., description="List of DQ rules to apply for validation")
    store_results: bool = Field(True, description="Whether to store results in DQ_VALIDATION_RESULTS table")
    sample_percent: Optional[int] = Field(None, description="Sample percentage for large tables (1-100). If None, validates all rows", ge=1, le=100)
    sample_failed_rows: int = Field(5, description="Number of sample failed rows to include per rule", ge=0, le=100)


class DQValidationResultInfo(BaseModel):
    """Individual rule validation result."""
    rule_id: str
    rule_type: str
    category: Optional[str] = None
    target_columns: List[str]
    status: str = Field(..., description="PASSED, FAILED, or ERROR")
    total_rows: int = Field(0, description="Total rows checked")
    passed_rows: int = Field(0, description="Rows that passed validation")
    failed_rows: int = Field(0, description="Rows that failed validation")
    pass_rate: float = Field(0.0, description="Percentage of rows that passed (0-100)")
    error_message: Optional[str] = Field(None, description="Error message if status is ERROR")
    sample_failures: Optional[List[Dict[str, Any]]] = Field(None, description="Sample of failed rows")
    execution_time_ms: float = Field(0.0, description="Time taken to execute validation in milliseconds")


class DQValidationResponse(BaseModel):
    """Response model for DQ rules validation."""
    validation_run_id: str
    table_name: str
    database_name: str
    validation_timestamp: str
    total_rules_applied: int
    rules_passed: int
    rules_failed: int
    rules_errored: int
    overall_status: str = Field(..., description="PASSED (all rules passed), FAILED (some failed), or ERROR")
    overall_pass_rate: float = Field(0.0, description="Average pass rate across all rules")
    sample_percent_used: Optional[int] = Field(None, description="Sample percentage used for validation")
    results_stored: bool = Field(False, description="Whether results were stored in the database")
    storage_table: Optional[str] = Field(None, description="Name of the table where results are stored")
    storage_warning: Optional[str] = Field(None, description="Warning message if storage was requested but not possible")
    results: List[DQValidationResultInfo]