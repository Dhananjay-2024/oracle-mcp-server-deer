
"""SQL execution routes - query, write, explain, samples."""

import sys
import oracledb
from fastapi import APIRouter, HTTPException

from api.models import (
    APIResponse,
    ExecuteSQLRequest,
    ExecuteWriteSQLRequest,
    ExplainQueryRequest,
    SQLResultResponse,
    ExecutionPlanResponse,
    SampleQueryInfo,
    SampleQueriesResponse,
    GenerateDQRulesRequest,
    DQRuleInfo,
    DQRulesResponse,
    ApplyDQRulesRequest,
    DQValidationResultInfo,
    DQValidationResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context
from db_context.schema.formatter import format_sql_query_result

router = APIRouter(prefix="/sql", tags=["SQL Execution"])


@router.post("/{database_name}/execute", response_model=APIResponse)
async def execute_sql(
    database_name: str,
    request: ExecuteSQLRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Execute a SQL query.
    
    Supports SELECT, INSERT, UPDATE, DELETE, and DDL statements.
    In read-only mode (default), only SELECT statements are permitted.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        result = await db_context.run_sql_query(request.sql, max_rows=request.max_rows)
        
        # Handle different result types
        if not result.get("rows"):
            return APIResponse(
                success=True,
                message=result.get("message", "Query executed successfully"),
                data=SQLResultResponse(
                    columns=result.get("columns", []),
                    rows=[],
                    row_count=result.get("row_count", 0),
                    message=result.get("message")
                )
            )
        
        return APIResponse(
            success=True,
            data=SQLResultResponse(
                columns=result.get("columns", []),
                rows=result.get("rows", []),
                row_count=len(result.get("rows", [])),
            )
        )
    except PermissionError as e:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: {str(e)}. Write operations require read_only=False."
        )
    except oracledb.Error as e:
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error executing query: {str(e)}")


@router.post("/{database_name}/write", response_model=APIResponse)
async def execute_write_sql(
    database_name: str,
    request: ExecuteWriteSQLRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Execute a write operation (INSERT, UPDATE, DELETE, DDL).
    
    Changes are automatically committed.
    Requires the database to be configured with read_only=False.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    # Validate it's not a SELECT
    sql_upper = request.sql.strip().upper()
    if sql_upper.startswith('SELECT') or sql_upper.startswith('WITH'):
        raise HTTPException(
            status_code=400,
            detail="Use the /execute endpoint for SELECT statements. This endpoint is for write operations only."
        )
    
    try:
        result = await db_context.run_sql_query(request.sql, max_rows=0)
        
        return APIResponse(
            success=True,
            message=result.get("message", "Statement executed successfully"),
            data={
                "row_count": result.get("row_count", 0),
                "message": result.get("message")
            }
        )
    except PermissionError as e:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: {str(e)}. Write operations require read_only=False in configuration."
        )
    except oracledb.Error as e:
        error_code = getattr(e.args[0] if e.args else None, 'code', 'Unknown')
        raise HTTPException(
            status_code=400,
            detail=f"Database error (ORA-{error_code}): {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error executing statement: {str(e)}")


@router.post("/{database_name}/explain", response_model=APIResponse)
async def explain_query_plan(
    database_name: str,
    request: ExplainQueryRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get execution plan for a SQL query.
    
    Returns the Oracle execution plan with optimization suggestions.
    Useful for understanding query performance before execution.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        plan = await db_context.explain_query_plan(request.sql)
        
        if plan.get("error"):
            return APIResponse(
                success=False,
                error=f"Explain plan unavailable: {plan['error']}"
            )
        
        if not plan.get("execution_plan"):
            return APIResponse(
                success=False,
                error="No execution plan rows returned"
            )
        
        return APIResponse(
            success=True,
            data=ExecutionPlanResponse(
                execution_plan=plan.get("execution_plan", []),
                optimization_suggestions=plan.get("optimization_suggestions")
            )
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission error: {str(e)}")
    except oracledb.Error as e:
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error obtaining plan: {str(e)}")


@router.get("/{database_name}/samples", response_model=APIResponse)
async def generate_sample_queries(
    database_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Generate sample SQL queries based on database schema.
    
    Creates 10 runnable SQL queries from beginner to advanced level
    using actual tables and columns from your database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        # Get available tables
        all_tables = await db_context.list_tables()
        
        if not all_tables or len(all_tables) == 0:
            return APIResponse(
                success=False,
                error="No tables found in database"
            )
        
        # Load metadata for first few tables
        tables_to_analyze = all_tables[:min(5, len(all_tables))]
        table_metadata = []
        
        for table_name in tables_to_analyze:
            try:
                table_info = await db_context.get_schema_info(table_name)
                if table_info and table_info.columns:
                    table_metadata.append(table_info)
            except Exception:
                continue
        
        if not table_metadata:
            return APIResponse(
                success=False,
                error="Could not load table metadata for query generation"
            )
        
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
        
        sample_queries = []
        
        # Beginner queries
        sample_queries.append(SampleQueryInfo(
            level="Beginner",
            number=1,
            title="Select All Records",
            description="Retrieve all columns and rows (limited to 10)",
            query=f"SELECT * FROM {table_name} WHERE ROWNUM <= 10"
        ))
        
        cols_to_select = all_columns[:min(3, len(all_columns))]
        sample_queries.append(SampleQueryInfo(
            level="Beginner",
            number=2,
            title="Select Specific Columns",
            description="Retrieve only specific columns",
            query=f"SELECT {', '.join(cols_to_select)} FROM {table_name}"
        ))
        
        if numeric_cols:
            sample_queries.append(SampleQueryInfo(
                level="Beginner",
                number=3,
                title="Filter with WHERE Clause",
                description="Filter records based on a numeric condition",
                query=f"SELECT * FROM {table_name} WHERE {numeric_cols[0]} > 0"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Beginner",
                number=3,
                title="Filter with WHERE Clause",
                description="Filter records with ROWNUM",
                query=f"SELECT * FROM {table_name} WHERE ROWNUM <= 5"
            ))
        
        sample_queries.append(SampleQueryInfo(
            level="Beginner",
            number=4,
            title="Sort Results",
            description="Order results by a specific column",
            query=f"SELECT * FROM {table_name} ORDER BY {all_columns[0]} DESC"
        ))
        
        # Intermediate queries
        sample_queries.append(SampleQueryInfo(
            level="Intermediate",
            number=5,
            title="Count Records",
            description="Count total number of records",
            query=f"SELECT COUNT(*) AS total_records FROM {table_name}"
        ))
        
        if numeric_cols and len(all_columns) > 1:
            group_col = [c for c in all_columns if c not in numeric_cols[:1]]
            group_col = group_col[0] if group_col else all_columns[0]
            sample_queries.append(SampleQueryInfo(
                level="Intermediate",
                number=6,
                title="Group and Aggregate",
                description="Group records and calculate aggregates",
                query=f"SELECT {group_col}, COUNT(*) AS count, AVG({numeric_cols[0]}) AS avg_value FROM {table_name} GROUP BY {group_col} ORDER BY count DESC"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Intermediate",
                number=6,
                title="Group and Count",
                description="Group records and count",
                query=f"SELECT {all_columns[0]}, COUNT(*) AS count FROM {table_name} GROUP BY {all_columns[0]} ORDER BY count DESC"
            ))
        
        sample_queries.append(SampleQueryInfo(
            level="Intermediate",
            number=7,
            title="Find Distinct Values",
            description="Get unique values from a column",
            query=f"SELECT DISTINCT {all_columns[0]} FROM {table_name} ORDER BY {all_columns[0]}"
        ))
        
        # Advanced queries
        if numeric_cols:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=8,
                title="Subquery - Above Average",
                description="Find records with values above average",
                query=f"SELECT * FROM {table_name} WHERE {numeric_cols[0]} > (SELECT AVG({numeric_cols[0]}) FROM {table_name})"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=8,
                title="Subquery - IN clause",
                description="Use subquery in IN clause",
                query=f"SELECT * FROM {table_name} WHERE {all_columns[0]} IN (SELECT {all_columns[0]} FROM {table_name} WHERE ROWNUM <= 5)"
            ))
        
        if numeric_cols and len(all_columns) > 1:
            partition_col = [c for c in all_columns if c != numeric_cols[0]][0]
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=9,
                title="Window Function - Ranking",
                description="Use window functions for ranking",
                query=f"SELECT {', '.join(all_columns[:3])}, ROW_NUMBER() OVER (PARTITION BY {partition_col} ORDER BY {numeric_cols[0]} DESC) AS rank FROM {table_name}"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=9,
                title="Window Function - Row Numbers",
                description="Add row numbers to results",
                query=f"SELECT {', '.join(all_columns[:3])}, ROW_NUMBER() OVER (ORDER BY {all_columns[0]}) AS row_num FROM {table_name}"
            ))
        
        if numeric_cols:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=10,
                title="Statistical Analysis",
                description="Calculate statistical measures",
                query=f"SELECT COUNT(*) AS total_records, MIN({numeric_cols[0]}) AS min_value, MAX({numeric_cols[0]}) AS max_value, AVG({numeric_cols[0]}) AS avg_value, STDDEV({numeric_cols[0]}) AS std_deviation FROM {table_name}"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=10,
                title="Analytical Query",
                description="Use analytical functions",
                query=f"SELECT {all_columns[0]}, COUNT(*) OVER () AS total_count, ROW_NUMBER() OVER (ORDER BY {all_columns[0]}) AS row_num FROM {table_name} WHERE ROWNUM <= 20"
            ))
        
        return APIResponse(
            success=True,
            data=SampleQueriesResponse(
                database_name=database_name,
                queries=sample_queries
            )
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating sample queries: {str(e)}")


@router.post("/{database_name}/dq-rules", response_model=APIResponse)
async def generate_sample_dq_rules(
    database_name: str,
    request: GenerateDQRulesRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Generate sample data quality (DQ) rules based on table schema.
    
    Creates contextual data quality validation rules using actual table columns and their
    data types. Rules span multiple categories including Business Entity Rules, Business
    Attribute Rules, Data Dependency Rules, and Data Validity Rules.
    
    Rule Categories:
    
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
    """
    import random
    
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        num_rules = max(1, min(request.num_rules, 50))
        
        # Get table schema info
        table_info = await db_context.get_schema_info(request.table_name)
        
        if not table_info:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{request.table_name}' not found in database '{database_name}'"
            )
        
        if not table_info.columns:
            raise HTTPException(
                status_code=400,
                detail=f"No columns found for table '{request.table_name}'"
            )
        
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
        
        # Build rule templates
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
                "rule_id": "r_phone_required_if_active",
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
            col_type = col.get('type', 'VARCHAR2(100)')
            max_len = 100
            if '(' in col_type:
                try:
                    max_len = int(col_type.split('(')[1].split(')')[0].split(',')[0])
                except:
                    max_len = col.get('length', 100)
            else:
                max_len = col.get('length', 100)
            
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
                        "expression": f"SELECT {', '.join(dup_check_cols)}, COUNT(*) FROM {request.table_name} GROUP BY {', '.join(dup_check_cols)} HAVING COUNT(*) > 1"
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
        fk_pattern_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['_ID', '_CODE', '_KEY', '_REF', '_FK'])]
        for col in fk_pattern_cols[:2]:
            col_name = col['name']
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
                    "expression": f"SELECT {col_name}, COUNT(*) FROM {request.table_name} WHERE {col_name} IS NOT NULL GROUP BY {col_name} HAVING COUNT(*) > 1",
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
                    "expression": f"SELECT {cust_col}, {acct_col}, COUNT(*) FROM {request.table_name} GROUP BY {cust_col}, {acct_col} HAVING COUNT(*) > 1",
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
                            "description": f"Many {request.table_name} records can reference one {parent_table} record"
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
                        "expression": f"LENGTH({col_name}) = (SELECT MAX(LENGTH({col_name})) FROM {request.table_name})",
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
        
        # If we have fewer templates than requested, add generic ones
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
        
        # Select requested number of rules
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
        
        # Convert to DQRuleInfo objects
        dq_rules = [
            DQRuleInfo(
                rule_id=rule['rule_id'],
                rule_type=rule['rule_type'],
                category=rule.get('category'),
                target_columns=rule['target_columns'],
                params=rule.get('params'),
                enabled=rule['enabled']
            )
            for rule in rules
        ]
        
        return APIResponse(
            success=True,
            data=DQRulesResponse(
                rule_set_id=f"dq_{request.table_name.lower()}",
                table_name=request.table_name,
                database_name=database_name,
                total_rules=len(dq_rules),
                rule_category_summary=category_summary,
                rules=dq_rules
            )
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating DQ rules: {str(e)}")


@router.post("/{database_name}/dq-rules/validate", response_model=APIResponse)
async def apply_dq_rules(
    database_name: str,
    request: ApplyDQRulesRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Apply data quality rules to validate table data and optionally store results.
    
    Executes the specified DQ rules against the table, calculates pass/fail statistics,
    and stores validation results in DQ_VALIDATION_RESULTS table.
    
    Args:
        database_name: Name of the database containing the table
        request: Validation request with table_name, rules, store_results, sample_percent
        
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
    """
    import json
    import time
    import uuid
    from datetime import datetime
    
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        rules = request.rules
        if not rules or len(rules) == 0:
            raise HTTPException(status_code=400, detail="No rules provided for validation")
        validation_run_id = f"vr_{uuid.uuid4().hex[:12]}"
        validation_timestamp = datetime.now().isoformat()
        # --- Write mode override logic ---
        storage_warning = None
        can_store = request.store_results
        original_read_only = True
        connector = None
        if request.store_results:
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
        if request.sample_percent and 1 <= request.sample_percent <= 100:
            sample_clause = f" SAMPLE({request.sample_percent})"
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
            rule_type = rule.rule_type.lower() if rule.rule_type else 'unknown'
            target_cols = rule.target_columns or []
            params = rule.params or {}
            
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
            rule_id = rule.rule_id
            rule_type = rule.rule_type
            category = rule.category
            target_cols = rule.target_columns or []
            params = rule.params or {}
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
                                TABLE_NAME = '{request.table_name}',
                                TARGET_COLUMNS = '{','.join(target_cols)}',
                                PARAMS = {f"'{params_json}'" if params_json else 'NULL'},
                                UPDATED_AT = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN
                            INSERT (RULE_ID, RULE_TYPE, RULE_CATEGORY, TABLE_NAME, TARGET_COLUMNS, PARAMS, ENABLED, CREATED_AT, UPDATED_AT)
                            VALUES ('{rule_id}', '{rule_type}', {f"'{category}'" if category else 'NULL'}, '{request.table_name}', '{','.join(target_cols)}', {f"'{params_json}'" if params_json else 'NULL'}, 1, SYSTIMESTAMP, SYSTIMESTAMP)
                        """
                        await db_context.run_sql_query(merge_rule_sql)
                    except Exception as rule_store_err:
                        if not storage_warning:
                            storage_warning = f"Could not store some rules: {str(rule_store_err)}"
                total_sql, failed_sql, sample_sql = build_validation_sql(rule, request.table_name, sample_clause)
                if total_sql is None:
                    error_msg = failed_sql if failed_sql else sample_sql
                    results.append(DQValidationResultInfo(
                        rule_id=rule_id,
                        rule_type=rule_type,
                        category=category,
                        target_columns=target_cols,
                        status="ERROR",
                        total_rows=0,
                        passed_rows=0,
                        failed_rows=0,
                        pass_rate=0.0,
                        error_message=error_msg,
                        sample_failures=None,
                        execution_time_ms=(time.time() - start_time) * 1000
                    ))
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
                result_entry = DQValidationResultInfo(
                    rule_id=rule_id,
                    rule_type=rule_type,
                    category=category,
                    target_columns=target_cols,
                    status=status,
                    total_rows=total_rows,
                    passed_rows=passed_rows,
                    failed_rows=failed_rows,
                    pass_rate=round(pass_rate, 2),
                    error_message=None,
                    sample_failures=sample_failures,
                    execution_time_ms=round(execution_time, 2)
                )
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
                            {f"'{category}'" if category else 'NULL'}, '{request.table_name}',
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
                results.append(DQValidationResultInfo(
                    rule_id=rule_id,
                    rule_type=rule_type,
                    category=category,
                    target_columns=target_cols,
                    status="ERROR",
                    total_rows=0,
                    passed_rows=0,
                    failed_rows=0,
                    pass_rate=0.0,
                    error_message=str(e),
                    sample_failures=None,
                    execution_time_ms=round(execution_time, 2)
                ))
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
        
        return APIResponse(
            success=True,
            data=DQValidationResponse(
                validation_run_id=validation_run_id,
                table_name=request.table_name,
                database_name=database_name,
                validation_timestamp=validation_timestamp,
                total_rules_applied=total_rules,
                rules_passed=rules_passed,
                rules_failed=rules_failed,
                rules_errored=rules_errored,
                overall_status=overall_status,
                overall_pass_rate=round(overall_pass_rate, 2),
                sample_percent_used=request.sample_percent,
                results_stored=can_store and not storage_warning,
                storage_table=storage_table if can_store else None,
                storage_warning=storage_warning,
                results=results
            )
        )
        
    except HTTPException:
        # Restore read-only mode on HTTP exception
        if connector and original_read_only:
            try:
                connector.read_only = True
            except:
                pass
        raise
    except Exception as e:
        # Restore read-only mode on error
        if connector and original_read_only:
            try:
                connector.read_only = True
            except:
                pass
        raise HTTPException(status_code=400, detail=f"Error applying DQ rules: {str(e)}")
"""SQL execution routes - query, write, explain, samples."""

import sys
import oracledb
from fastapi import APIRouter, HTTPException

from api.models import (
    APIResponse,
    ExecuteSQLRequest,
    ExecuteWriteSQLRequest,
    ExplainQueryRequest,
    SQLResultResponse,
    ExecutionPlanResponse,
    SampleQueryInfo,
    SampleQueriesResponse,
    GenerateDQRulesRequest,
    DQRuleInfo,
    DQRulesResponse,
    ApplyDQRulesRequest,
    DQValidationResultInfo,
    DQValidationResponse,
)
from api.dependencies import MultiDBContextDep, get_database_context
from db_context.schema.formatter import format_sql_query_result

router = APIRouter(prefix="/sql", tags=["SQL Execution"])


@router.post("/{database_name}/execute", response_model=APIResponse)
async def execute_sql(
    database_name: str,
    request: ExecuteSQLRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Execute a SQL query.
    
    Supports SELECT, INSERT, UPDATE, DELETE, and DDL statements.
    In read-only mode (default), only SELECT statements are permitted.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        result = await db_context.run_sql_query(request.sql, max_rows=request.max_rows)
        
        # Handle different result types
        if not result.get("rows"):
            return APIResponse(
                success=True,
                message=result.get("message", "Query executed successfully"),
                data=SQLResultResponse(
                    columns=result.get("columns", []),
                    rows=[],
                    row_count=result.get("row_count", 0),
                    message=result.get("message")
                )
            )
        
        return APIResponse(
            success=True,
            data=SQLResultResponse(
                columns=result.get("columns", []),
                rows=result.get("rows", []),
                row_count=len(result.get("rows", [])),
            )
        )
    except PermissionError as e:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: {str(e)}. Write operations require read_only=False."
        )
    except oracledb.Error as e:
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error executing query: {str(e)}")


@router.post("/{database_name}/write", response_model=APIResponse)
async def execute_write_sql(
    database_name: str,
    request: ExecuteWriteSQLRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Execute a write operation (INSERT, UPDATE, DELETE, DDL).
    
    Changes are automatically committed.
    Requires the database to be configured with read_only=False.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    # Validate it's not a SELECT
    sql_upper = request.sql.strip().upper()
    if sql_upper.startswith('SELECT') or sql_upper.startswith('WITH'):
        raise HTTPException(
            status_code=400,
            detail="Use the /execute endpoint for SELECT statements. This endpoint is for write operations only."
        )
    
    try:
        result = await db_context.run_sql_query(request.sql, max_rows=0)
        
        return APIResponse(
            success=True,
            message=result.get("message", "Statement executed successfully"),
            data={
                "row_count": result.get("row_count", 0),
                "message": result.get("message")
            }
        )
    except PermissionError as e:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied: {str(e)}. Write operations require read_only=False in configuration."
        )
    except oracledb.Error as e:
        error_code = getattr(e.args[0] if e.args else None, 'code', 'Unknown')
        raise HTTPException(
            status_code=400,
            detail=f"Database error (ORA-{error_code}): {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error executing statement: {str(e)}")


@router.post("/{database_name}/explain", response_model=APIResponse)
async def explain_query_plan(
    database_name: str,
    request: ExplainQueryRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Get execution plan for a SQL query.
    
    Returns the Oracle execution plan with optimization suggestions.
    Useful for understanding query performance before execution.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        plan = await db_context.explain_query_plan(request.sql)
        
        if plan.get("error"):
            return APIResponse(
                success=False,
                error=f"Explain plan unavailable: {plan['error']}"
            )
        
        if not plan.get("execution_plan"):
            return APIResponse(
                success=False,
                error="No execution plan rows returned"
            )
        
        return APIResponse(
            success=True,
            data=ExecutionPlanResponse(
                execution_plan=plan.get("execution_plan", []),
                optimization_suggestions=plan.get("optimization_suggestions")
            )
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission error: {str(e)}")
    except oracledb.Error as e:
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error obtaining plan: {str(e)}")


@router.get("/{database_name}/samples", response_model=APIResponse)
async def generate_sample_queries(
    database_name: str,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Generate sample SQL queries based on database schema.
    
    Creates 10 runnable SQL queries from beginner to advanced level
    using actual tables and columns from your database.
    """
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        # Get available tables
        all_tables = await db_context.list_tables()
        
        if not all_tables or len(all_tables) == 0:
            return APIResponse(
                success=False,
                error="No tables found in database"
            )
        
        # Load metadata for first few tables
        tables_to_analyze = all_tables[:min(5, len(all_tables))]
        table_metadata = []
        
        for table_name in tables_to_analyze:
            try:
                table_info = await db_context.get_schema_info(table_name)
                if table_info and table_info.columns:
                    table_metadata.append(table_info)
            except Exception:
                continue
        
        if not table_metadata:
            return APIResponse(
                success=False,
                error="Could not load table metadata for query generation"
            )
        
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
        
        sample_queries = []
        
        # Beginner queries
        sample_queries.append(SampleQueryInfo(
            level="Beginner",
            number=1,
            title="Select All Records",
            description="Retrieve all columns and rows (limited to 10)",
            query=f"SELECT * FROM {table_name} WHERE ROWNUM <= 10"
        ))
        
        cols_to_select = all_columns[:min(3, len(all_columns))]
        sample_queries.append(SampleQueryInfo(
            level="Beginner",
            number=2,
            title="Select Specific Columns",
            description="Retrieve only specific columns",
            query=f"SELECT {', '.join(cols_to_select)} FROM {table_name}"
        ))
        
        if numeric_cols:
            sample_queries.append(SampleQueryInfo(
                level="Beginner",
                number=3,
                title="Filter with WHERE Clause",
                description="Filter records based on a numeric condition",
                query=f"SELECT * FROM {table_name} WHERE {numeric_cols[0]} > 0"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Beginner",
                number=3,
                title="Filter with WHERE Clause",
                description="Filter records with ROWNUM",
                query=f"SELECT * FROM {table_name} WHERE ROWNUM <= 5"
            ))
        
        sample_queries.append(SampleQueryInfo(
            level="Beginner",
            number=4,
            title="Sort Results",
            description="Order results by a specific column",
            query=f"SELECT * FROM {table_name} ORDER BY {all_columns[0]} DESC"
        ))
        
        # Intermediate queries
        sample_queries.append(SampleQueryInfo(
            level="Intermediate",
            number=5,
            title="Count Records",
            description="Count total number of records",
            query=f"SELECT COUNT(*) AS total_records FROM {table_name}"
        ))
        
        if numeric_cols and len(all_columns) > 1:
            group_col = [c for c in all_columns if c not in numeric_cols[:1]]
            group_col = group_col[0] if group_col else all_columns[0]
            sample_queries.append(SampleQueryInfo(
                level="Intermediate",
                number=6,
                title="Group and Aggregate",
                description="Group records and calculate aggregates",
                query=f"SELECT {group_col}, COUNT(*) AS count, AVG({numeric_cols[0]}) AS avg_value FROM {table_name} GROUP BY {group_col} ORDER BY count DESC"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Intermediate",
                number=6,
                title="Group and Count",
                description="Group records and count",
                query=f"SELECT {all_columns[0]}, COUNT(*) AS count FROM {table_name} GROUP BY {all_columns[0]} ORDER BY count DESC"
            ))
        
        sample_queries.append(SampleQueryInfo(
            level="Intermediate",
            number=7,
            title="Find Distinct Values",
            description="Get unique values from a column",
            query=f"SELECT DISTINCT {all_columns[0]} FROM {table_name} ORDER BY {all_columns[0]}"
        ))
        
        # Advanced queries
        if numeric_cols:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=8,
                title="Subquery - Above Average",
                description="Find records with values above average",
                query=f"SELECT * FROM {table_name} WHERE {numeric_cols[0]} > (SELECT AVG({numeric_cols[0]}) FROM {table_name})"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=8,
                title="Subquery - IN clause",
                description="Use subquery in IN clause",
                query=f"SELECT * FROM {table_name} WHERE {all_columns[0]} IN (SELECT {all_columns[0]} FROM {table_name} WHERE ROWNUM <= 5)"
            ))
        
        if numeric_cols and len(all_columns) > 1:
            partition_col = [c for c in all_columns if c != numeric_cols[0]][0]
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=9,
                title="Window Function - Ranking",
                description="Use window functions for ranking",
                query=f"SELECT {', '.join(all_columns[:3])}, ROW_NUMBER() OVER (PARTITION BY {partition_col} ORDER BY {numeric_cols[0]} DESC) AS rank FROM {table_name}"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=9,
                title="Window Function - Row Numbers",
                description="Add row numbers to results",
                query=f"SELECT {', '.join(all_columns[:3])}, ROW_NUMBER() OVER (ORDER BY {all_columns[0]}) AS row_num FROM {table_name}"
            ))
        
        if numeric_cols:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=10,
                title="Statistical Analysis",
                description="Calculate statistical measures",
                query=f"SELECT COUNT(*) AS total_records, MIN({numeric_cols[0]}) AS min_value, MAX({numeric_cols[0]}) AS max_value, AVG({numeric_cols[0]}) AS avg_value, STDDEV({numeric_cols[0]}) AS std_deviation FROM {table_name}"
            ))
        else:
            sample_queries.append(SampleQueryInfo(
                level="Advanced",
                number=10,
                title="Analytical Query",
                description="Use analytical functions",
                query=f"SELECT {all_columns[0]}, COUNT(*) OVER () AS total_count, ROW_NUMBER() OVER (ORDER BY {all_columns[0]}) AS row_num FROM {table_name} WHERE ROWNUM <= 20"
            ))
        
        return APIResponse(
            success=True,
            data=SampleQueriesResponse(
                database_name=database_name,
                queries=sample_queries
            )
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating sample queries: {str(e)}")


@router.post("/{database_name}/dq-rules", response_model=APIResponse)
async def generate_sample_dq_rules(
    database_name: str,
    request: GenerateDQRulesRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Generate sample data quality (DQ) rules based on table schema.
    
    Creates contextual data quality validation rules using actual table columns and their
    data types. Rules span multiple categories including Business Entity Rules, Business
    Attribute Rules, Data Dependency Rules, and Data Validity Rules.
    
    Rule Categories:
    
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
    """
    import random
    
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        num_rules = max(1, min(request.num_rules, 50))
        
        # Get table schema info
        table_info = await db_context.get_schema_info(request.table_name)
        
        if not table_info:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{request.table_name}' not found in database '{database_name}'"
            )
        
        if not table_info.columns:
            raise HTTPException(
                status_code=400,
                detail=f"No columns found for table '{request.table_name}'"
            )
        
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
        
        # Build rule templates
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
                "rule_id": "r_phone_required_if_active",
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
            col_type = col.get('type', 'VARCHAR2(100)')
            max_len = 100
            if '(' in col_type:
                try:
                    max_len = int(col_type.split('(')[1].split(')')[0].split(',')[0])
                except:
                    max_len = col.get('length', 100)
            else:
                max_len = col.get('length', 100)
            
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
                        "expression": f"SELECT {', '.join(dup_check_cols)}, COUNT(*) FROM {request.table_name} GROUP BY {', '.join(dup_check_cols)} HAVING COUNT(*) > 1"
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
        fk_pattern_cols = [col for col in all_cols if any(k in col['name'].upper() for k in ['_ID', '_CODE', '_KEY', '_REF', '_FK'])]
        for col in fk_pattern_cols[:2]:
            col_name = col['name']
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
                    "expression": f"SELECT {col_name}, COUNT(*) FROM {request.table_name} WHERE {col_name} IS NOT NULL GROUP BY {col_name} HAVING COUNT(*) > 1",
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
                    "expression": f"SELECT {cust_col}, {acct_col}, COUNT(*) FROM {request.table_name} GROUP BY {cust_col}, {acct_col} HAVING COUNT(*) > 1",
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
                            "description": f"Many {request.table_name} records can reference one {parent_table} record"
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
                        "expression": f"LENGTH({col_name}) = (SELECT MAX(LENGTH({col_name})) FROM {request.table_name})",
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
        
        # If we have fewer templates than requested, add generic ones
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
        
        # Select requested number of rules
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
        
        # Convert to DQRuleInfo objects
        dq_rules = [
            DQRuleInfo(
                rule_id=rule['rule_id'],
                rule_type=rule['rule_type'],
                category=rule.get('category'),
                target_columns=rule['target_columns'],
                params=rule.get('params'),
                enabled=rule['enabled']
            )
            for rule in rules
        ]
        
        return APIResponse(
            success=True,
            data=DQRulesResponse(
                rule_set_id=f"dq_{request.table_name.lower()}",
                table_name=request.table_name,
                database_name=database_name,
                total_rules=len(dq_rules),
                rule_category_summary=category_summary,
                rules=dq_rules
            )
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating DQ rules: {str(e)}")


@router.post("/{database_name}/dq-rules/validate", response_model=APIResponse)
async def apply_dq_rules(
    database_name: str,
    request: ApplyDQRulesRequest,
    multi_ctx: MultiDBContextDep,
) -> APIResponse:
    """Apply data quality rules to validate table data and optionally store results.
    
    Executes the specified DQ rules against the table, calculates pass/fail statistics,
    and stores validation results in DQ_VALIDATION_RESULTS table.
    
    Args:
        database_name: Name of the database containing the table
        request: Validation request with table_name, rules, store_results, sample_percent
        
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
    """
    import json
    import time
    import uuid
    from datetime import datetime
    
    db_context = get_database_context(database_name, multi_ctx)
    
    try:
        rules = request.rules
        if not rules or len(rules) == 0:
            raise HTTPException(status_code=400, detail="No rules provided for validation")
        validation_run_id = f"vr_{uuid.uuid4().hex[:12]}"
        validation_timestamp = datetime.now().isoformat()
        # --- Write mode override logic ---
        storage_warning = None
        can_store = request.store_results
        original_read_only = True
        connector = None
        if request.store_results:
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
        if request.sample_percent and 1 <= request.sample_percent <= 100:
            sample_clause = f" SAMPLE({request.sample_percent})"
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
            rule_type = rule.rule_type.lower() if rule.rule_type else 'unknown'
            target_cols = rule.target_columns or []
            params = rule.params or {}
            
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
            rule_id = rule.rule_id
            rule_type = rule.rule_type
            category = rule.category
            target_cols = rule.target_columns or []
            params = rule.params or {}
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
                                TABLE_NAME = '{request.table_name}',
                                TARGET_COLUMNS = '{','.join(target_cols)}',
                                PARAMS = {f"'{params_json}'" if params_json else 'NULL'},
                                UPDATED_AT = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN
                            INSERT (RULE_ID, RULE_TYPE, RULE_CATEGORY, TABLE_NAME, TARGET_COLUMNS, PARAMS, ENABLED, CREATED_AT, UPDATED_AT)
                            VALUES ('{rule_id}', '{rule_type}', {f"'{category}'" if category else 'NULL'}, '{request.table_name}', '{','.join(target_cols)}', {f"'{params_json}'" if params_json else 'NULL'}, 1, SYSTIMESTAMP, SYSTIMESTAMP)
                        """
                        await db_context.run_sql_query(merge_rule_sql)
                    except Exception as rule_store_err:
                        if not storage_warning:
                            storage_warning = f"Could not store some rules: {str(rule_store_err)}"
                total_sql, failed_sql, sample_sql = build_validation_sql(rule, request.table_name, sample_clause)
                if total_sql is None:
                    error_msg = failed_sql if failed_sql else sample_sql
                    results.append(DQValidationResultInfo(
                        rule_id=rule_id,
                        rule_type=rule_type,
                        category=category,
                        target_columns=target_cols,
                        status="ERROR",
                        total_rows=0,
                        passed_rows=0,
                        failed_rows=0,
                        pass_rate=0.0,
                        error_message=error_msg,
                        sample_failures=None,
                        execution_time_ms=(time.time() - start_time) * 1000
                    ))
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
                result_entry = DQValidationResultInfo(
                    rule_id=rule_id,
                    rule_type=rule_type,
                    category=category,
                    target_columns=target_cols,
                    status=status,
                    total_rows=total_rows,
                    passed_rows=passed_rows,
                    failed_rows=failed_rows,
                    pass_rate=round(pass_rate, 2),
                    error_message=None,
                    sample_failures=sample_failures,
                    execution_time_ms=round(execution_time, 2)
                )
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
                            {f"'{category}'" if category else 'NULL'}, '{request.table_name}',
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
                results.append(DQValidationResultInfo(
                    rule_id=rule_id,
                    rule_type=rule_type,
                    category=category,
                    target_columns=target_cols,
                    status="ERROR",
                    total_rows=0,
                    passed_rows=0,
                    failed_rows=0,
                    pass_rate=0.0,
                    error_message=str(e),
                    sample_failures=None,
                    execution_time_ms=round(execution_time, 2)
                ))
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
        
        return APIResponse(
            success=True,
            data=DQValidationResponse(
                validation_run_id=validation_run_id,
                table_name=request.table_name,
                database_name=database_name,
                validation_timestamp=validation_timestamp,
                total_rules_applied=total_rules,
                rules_passed=rules_passed,
                rules_failed=rules_failed,
                rules_errored=rules_errored,
                overall_status=overall_status,
                overall_pass_rate=round(overall_pass_rate, 2),
                sample_percent_used=request.sample_percent,
                results_stored=can_store and not storage_warning,
                storage_table=storage_table if can_store else None,
                storage_warning=storage_warning,
                results=results
            )
        )
        
    except HTTPException:
        # Restore read-only mode on HTTP exception
        if connector and original_read_only:
            try:
                connector.read_only = True
            except:
                pass
        raise
    except Exception as e:
        # Restore read-only mode on error
        if connector and original_read_only:
            try:
                connector.read_only = True
            except:
                pass
        raise HTTPException(status_code=400, detail=f"Error applying DQ rules: {str(e)}")


