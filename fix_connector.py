#!/usr/bin/env python3
"""Fix connector attribute access in sql.py"""

import re

# Read the file
with open(r"c:\Users\dhananjayl\OneDrive - Maveric Systems Limited\Desktop\oracle-mcp-server-deer\api\routes\sql.py", 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the incorrect attribute access patterns
# Pattern 1: Replace _connector with db_connector
content = content.replace("hasattr(db_context, '_connector')", "hasattr(db_context, 'db_connector')")
content = content.replace("db_context._connector", "db_context.db_connector")

# Pattern 2: Remove the elif branch for 'connector' attribute (which doesn't exist)
pattern = r"elif hasattr\(db_context, 'connector'\):\s+connector = db_context\.connector\s+original_read_only = getattr\(connector, 'read_only', True\)\s+if original_read_only:\s+connector\.read_only = False"
content = re.sub(pattern, '', content, flags=re.MULTILINE)

# Pattern 3: Add else clause with proper error handling after the db_connector check
old_pattern = r"(if hasattr\(db_context, 'db_connector'\):\s+connector = db_context\.db_connector\s+original_read_only = getattr\(connector, 'read_only', True\)\s+if original_read_only:\s+)connector\.read_only = False"
new_replacement = r'\1print(f"[DQ Storage] Temporarily enabling write mode for storing rules and results", file=sys.stderr)\n                        connector.read_only = False\n                else:\n                    storage_warning = "Could not access database connector to enable write mode"\n                    can_store = False'
content = re.sub(old_pattern, new_replacement, content, flags=re.MULTILINE)

# Write back
with open(r"c:\Users\dhananjayl\OneDrive - Maveric Systems Limited\Desktop\oracle-mcp-server-deer\api\routes\sql.py", 'w', encoding='utf-8') as f:
    f.write(content)

print("✓ Fixed connector attribute access in api/routes/sql.py")
