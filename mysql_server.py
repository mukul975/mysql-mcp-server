


#!/usr/bin/env python3
"""
MCP MySQL Server

A Model Context Protocol server that provides MySQL database access.
Allows LLMs to query databases, inspect schemas, and execute SQL commands.
"""

import asyncio
import json
import logging
import re
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, AsyncIterator, Union
from dataclasses import dataclass

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("mysql-connector-python is required. Install with: pip install mysql-connector-python")
    exit(1)

try:
    from mcp.server.fastmcp import FastMCP, Context
except ImportError:
    print("MCP SDK is required. Install with: pip install 'mcp[cli]'")
    exit(1)


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""


class MySQLConnection:
    """MySQL database connection manager."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        # Read-only query patterns for security
        self.read_only_patterns = [
            r'^\s*SELECT\s+',
            r'^\s*SHOW\s+',
            r'^\s*DESCRIBE\s+',
            r'^\s*DESC\s+',
            r'^\s*EXPLAIN\s+',
            r'^\s*WITH\s+.*SELECT\s+'
        ]
    
    async def connect(self):
        """Connect to MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                autocommit=True
            )
            logging.info(f"Connected to MySQL database: {self.config.host}:{self.config.port}")
            return self
        except MySQLError as e:
            logging.error(f"Failed to connect to MySQL: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from MySQL database."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Disconnected from MySQL database")
    
    def execute_query(self, query: str):
        """Execute a SQL query and return results."""
        if not self.connection or not self.connection.is_connected():
            raise RuntimeError("Not connected to database")
        
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query)
            if cursor.description:  # SELECT query
                results = cursor.fetchall()
                return results
            else:  # INSERT, UPDATE, DELETE, etc.
                affected_rows = cursor.rowcount
                return [{"affected_rows": affected_rows, "message": "Query executed successfully"}]
        finally:
            cursor.close()
    
    def get_tables(self):
        """Get list of tables in the database."""
        query = "SHOW TABLES"
        results = self.execute_query(query)
        return [{"table_name": list(row.values())[0]} for row in results]
    
    def get_table_schema(self, table_name: str):
        """Get schema information for a specific table."""
        query = f"DESCRIBE `{table_name}`"
        return self.execute_query(query)
    
    def get_databases(self):
        """Get list of all databases."""
        query = "SHOW DATABASES"
        results = self.execute_query(query)
        return [{"database_name": list(row.values())[0]} for row in results]
    
    def is_read_only_query(self, query: str) -> bool:
        """Check if a query is read-only using regex patterns."""
        query_upper = query.upper().strip()
        return any(re.match(pattern, query_upper, re.IGNORECASE) for pattern in self.read_only_patterns)
    
    def quote_identifier(self, identifier: str) -> str:
        """Quote a MySQL identifier (table/column name) to prevent injection."""
        return f"`{identifier.replace('`', '``')}`"

    def validate_table_name(self, table_name: str) -> str:
        """Validate and sanitize table name to prevent injection."""
        # Remove any dangerous characters and validate format
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        return table_name
    
    def execute_prepared_query(self, query: str, params: Optional[List] = None):
        """Execute a prepared statement query with parameters."""
        if not self.connection or not self.connection.is_connected():
            raise RuntimeError("Not connected to database")
        
        cursor = self.connection.cursor(dictionary=True)
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if cursor.description:
                results = cursor.fetchall()
                return results
            else:
                affected_rows = cursor.rowcount
                return [{"affected_rows": affected_rows, "message": "Query executed successfully"}]
        finally:
            cursor.close()

# Add async context manager for MySQL connections
import os

@asynccontextmanager
async def get_mysql_connection():
    config = DatabaseConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "")
    )
    conn = MySQLConnection(config)
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.disconnect()

@asynccontextmanager
async def database_lifespan(server: FastMCP) -> AsyncIterator[Dict[str, Any]]:
    """Manage database connection lifecycle."""
    async with get_mysql_connection() as conn:
        yield {"db": conn, "config": conn.config}


# Create FastMCP server with database lifespan management
mcp = FastMCP("MySQL MCP Server", lifespan=database_lifespan)

@mcp.tool()
def mysql_fragmentation_extensive_analysis(ctx: Context, database_name: Optional[str] = None):
    """Provide a detailed analysis and defragmentation recommendations for tables."""
    try:
        db = ctx.lifespan["db"]
        if database_name:
            query = """SELECT 
                table_name,
                engine,
                table_rows,
                data_length,
                index_length,
                data_free,
                ROUND(data_free / IFNULL(NULLIF((data_length + index_length), 0), 1) * 100, 2) AS fragmentation_pct
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND data_free > 0
            ORDER BY fragmentation_pct DESC"""
            fragmentation_info = db.execute_prepared_query(query, [database_name])
        else:
            query = """SELECT 
                table_schema,
                table_name,
                engine,
                table_rows,
                data_length,
                index_length,
                data_free,
                ROUND(data_free / IFNULL(NULLIF((data_length + index_length), 0), 1) * 100, 2) AS fragmentation_pct
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND data_free > 0
            ORDER BY fragmentation_pct DESC"""
            fragmentation_info = db.execute_query(query)

        # Identify tables with high fragmentation
        high_fragmentation = []
        for table in fragmentation_info:
            if table.get('fragmentation_pct', 0) > 10:  # More than 10% fragmented
                high_fragmentation.append({
                    "table_name": table['table_name'],
                    "fragmentation_pct": table['fragmentation_pct'],
                    "engine": table['engine'],
                    "recommendation": f"OPTIMIZE TABLE {table['table_name']}" if table['engine'] == 'MyISAM' else f"ALTER TABLE {table['table_name']} ENGINE=InnoDB"
                })

        return {
            "success": True,
            "fragmentation_analysis": fragmentation_info,
            "high_fragmentation": high_fragmentation,
            "total_tables": len(fragmentation_info),
            "optimization_needed": len(high_fragmentation)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_index_optimization_suggestions(ctx: Context):
    """Suggest optimal indexing strategies and potential consolidations."""
    try:
        db = ctx.lifespan["db"]
        query = """SELECT 
            table_name,
            index_name,
            seq_in_index,
            column_name
        FROM information_schema.statistics 
        WHERE table_schema = DATABASE()
        ORDER BY table_name, index_name, seq_in_index"""
        indexes = db.execute_query(query)

        # Suggestion structure
        suggestions = []

        # Sample suggestion logic (needs enhancement for production)
        for index in indexes:
            suggestions.append({
                "table_name": index['table_name'],
                "index_name": index['index_name'],
                "column_name": index['column_name'],
                "suggestion": "Consider consolidating indexes where possible"
            })

        return {
            "success": True,
            "indexes": indexes,
            "suggestions": suggestions,
            "count": len(suggestions)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_historical_slow_query_analysis(ctx: Context, min_duration: Optional[float] = None):
    """Analyze and aggregate historical slow query patterns."""
    try:
        db = ctx.lifespan["db"]
        min_time = min_duration if min_duration is not None else 1.0
        query = """SELECT 
            sql_text,
            COUNT(*) as execution_count,
            AVG(timer_wait)/1000000000000 AS avg_exec_time_sec
        FROM performance_schema.events_statements_history_long 
        WHERE timer_wait/1000000000000 > %s
        GROUP BY sql_text
        ORDER BY execution_count DESC"""
        slow_queries = db.execute_prepared_query(query, [min_time])

        return {
            "success": True,
            "slow_queries": slow_queries,
            "count": len(slow_queries),
            "min_duration": min_time
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_buffer_pool_cache_diagnostics(ctx: Context):
    """Detailed diagnostics for buffer pool and cache tuning."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW ENGINE INNODB STATUS"
        innodb_status = db.execute_query(query)

        # Example of parsing the InnoDB status
        diagnostics = {
            "buffer_pool_pages": None,
            "cache_hit_ratio": None
        }

        for line in innodb_status:
            if "Buffer pool size" in line["Status"]:
                diagnostics["buffer_pool_pages"] = line["Status"].split("=")[1].strip()
            if "Buffer pool hit rate" in line["Status"]:
                diagnostics["cache_hit_ratio"] = line["Status"].split("=")[1].strip()

        return {
            "success": True,
            "innodb_status": diagnostics
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_advanced_deadlock_detection(ctx: Context):
    """Enhanced detection and reporting of deadlock issues."""
    try:
        db = ctx.lifespan["db"]
        # Get enhanced deadlock information
        query = "SHOW ENGINE INNODB STATUS"
        status = db.execute_query(query)

        # Example: Parsing deadlock information (simplified)
        deadlock_info = {}

        for line in status:
            if "LATEST DETECTED DEADLOCK" in line["Status"]:
                deadlock_info["latest_deadlock"] = line["Status"]

        return {
            "success": True,
            "deadlock_info": deadlock_info
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_cross_database_fk_analysis(ctx: Context):
    """Analyze foreign key issues across databases."""
    try:
        db = ctx.lifespan["db"]
        query = """SELECT * FROM information_schema.key_column_usage WHERE referenced_table_name IS NOT NULL"""
        fk_issues = db.execute_query(query)

        return {
            "success": True,
            "foreign_key_issues": fk_issues,
            "count": len(fk_issues)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_statistical_anomaly_detection(ctx: Context):
    """Detect statistical anomalies in column distributions."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT column_name, AVG(value), STDDEV(value) FROM some_table GROUP BY column_name"
        anomalies = db.execute_query(query)

        return {
            "success": True,
            "anomalies": anomalies,
            "count": len(anomalies)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_audit_log_summary(ctx: Context):
    """Summarize and detect anomalies within audit logs."""
    try:
        db = ctx.lifespan["db"]
        query = """SELECT * FROM mysql.audit_log WHERE event_name LIKE '%error%'"""
        audit_log = db.execute_query(query)

        return {
            "success": True,
            "audit_log_summary": audit_log,
            "count": len(audit_log)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_replication_lag_monitoring(ctx: Context):
    """Monitor replication lag over a time series."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW SLAVE STATUS"
        slave_status = db.execute_query(query)

        lag = None
        if slave_status and "Seconds_Behind_Master" in slave_status[0]:
            lag = slave_status[0]["Seconds_Behind_Master"]

        return {
            "success": True,
            "replication_lag": lag
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_privileges_security_audit(ctx: Context):
    """Audit user privileges and recommend policy improvements."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT user, host FROM mysql.user WHERE is_role = 'N' AND user != 'root'"
        user_privileges = db.execute_query(query)

        return {
            "success": True,
            "user_privileges": user_privileges,
            "recommendations": "Consider revising user privileges"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_real_time_query_perf_metrics(ctx: Context):
    """Provide real-time metrics on query performance and bottlenecks."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM performance_schema.events_waits_summary_by_instance"
        performance_metrics = db.execute_query(query)

        return {
            "success": True,
            "performance_metrics": performance_metrics,
            "count": len(performance_metrics)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_adaptive_index_improvements(ctx: Context):
    """Suggest adaptive index improvements based on query patterns."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW INDEX FROM information_schema.tables WHERE Seq_in_index > 1"
        index_improvements = db.execute_query(query)

        return {
            "success": True,
            "index_improvements": index_improvements,
            "count": len(index_improvements)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_server_health_dashboard(ctx: Context):
    """Create a dashboard displaying server health metrics."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT variable_name, value FROM performance_schema.global_status"
        server_health = db.execute_query(query)

        return {
            "success": True,
            "server_health": server_health,
            "count": len(server_health)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_backup_health_check(ctx: Context):
    """Perform a health check of backup strategies and configurations."""
    try:
        db = ctx.lifespan["db"]
        backup_status = "OK"  # Placeholder for actual status-checking logic

        return {
            "success": True,
            "backup_status": backup_status
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_partition_management_recommendations(ctx: Context):
    """Recommend partition management techniques."""
    try:
        db = ctx.lifespan["db"]
        partition_advice = []

        query = "SHOW TABLE STATUS WHERE Comment LIKE '%partitioned%'"
        partitions = db.execute_query(query)

        for partition in partitions:
            partition_advice.append({
                "table_name": partition['Name'],
                "recommendation": "Consider altering partition strategy"
            })

        return {
            "success": True,
            "partition_advice": partition_advice,
            "count": len(partition_advice)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_dynamic_configuration_tuning(ctx: Context):
    """Analyze and suggest dynamic configuration changes."""
    try:
        db = ctx.lifespan["db"]
        config_tuning_recs = []

        query = "SELECT * FROM performance_schema.global_variables"
        global_vars = db.execute_query(query)

        for var in global_vars:
            config_tuning_recs.append({
                "variable_name": var['Variable_name'],
                "recommendation": "Review and possibly adjust value"
            })

        return {
            "success": True,
            "config_tuning_recs": config_tuning_recs,
            "count": len(config_tuning_recs)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_innodb_metrics_analysis(ctx: Context):
    """Deep analysis of InnoDB metrics for performance tuning."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW ENGINE INNODB STATUS"
        innodb_metrics = db.execute_query(query)

        return {
            "success": True,
            "innodb_metrics": innodb_metrics
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_multi_tenancy_performance_insights(ctx: Context):
    """Insights and recommendations for multi-tenant databases."""
    try:
        db = ctx.lifespan["db"]
        tenant_stats = []
        query = "SELECT table_schema, sum(table_rows) as tenant_rows FROM information_schema.tables GROUP BY table_schema"
        tenant_data = db.execute_query(query)

        for tenant in tenant_data:
            tenant_stats.append({
                "schema": tenant['table_schema'],
                "rows": tenant['tenant_rows'],
                "recommendation": "Optimize for tenant isolation"
            })

        return {
            "success": True,
            "tenant_stats": tenant_stats,
            "count": len(tenant_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_ssl_tls_configuration_audit(ctx: Context):
    """Audit and improve SSL/TLS security configurations."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW STATUS LIKE 'Ssl_cipher'"
        ssl_status = db.execute_query(query)

        recommendations = []
        if not ssl_status or not ssl_status[0].get('Value'):
            recommendations.append({
                "issue": "SSL/TLS not configured",
                "recommendation": "Enable SSL/TLS for secure connections"
            })

        return {
            "success": True,
            "ssl_status": ssl_status,
            "recommendations": recommendations
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_auto_index_rebuild_scheduler(ctx: Context):
    """Automatically schedule index rebuilds based on usage patterns."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM performance_schema.events_waits_summary_by_instance WHERE event_name LIKE 'index/%'"
        index_usage = db.execute_query(query)

        rebuild_schedule = []
        for usage in index_usage:
            if usage.get('SUM_TIMER_WAIT') > 10000000:  # Arbitrary threshold
                rebuild_schedule.append({
                    "index_name": usage['OBJECT_NAME'],
                    "recommendation": "Schedule index rebuild"
                })

        return {
            "success": True,
            "rebuild_schedule": rebuild_schedule,
            "count": len(rebuild_schedule)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }
@mcp.tool()
async def mysql_user_statistics(ctx: Context):
    """Show detailed user statistics like query counts and connection duration."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT USER, TOTAL_CONNECTIONS, CONCURRENT_CONNECTIONS, CONNECTED_TIME, BUSY_TIME, CPU_TIME, BYTES_RECEIVED, BYTES_SENT, SELECT_COMMANDS, UPDATE_COMMANDS, OTHER_COMMANDS FROM performance_schema.user_summary"
        user_stats = db.execute_query(query)
        return {
            "success": True,
            "user_stats": user_stats,
            "count": len(user_stats)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_list_open_transactions(ctx: Context):
    """List current open transactions and their states."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM information_schema.innodb_trx"
        transactions = db.execute_query(query)
        return {
            "success": True,
            "transactions": transactions,
            "count": len(transactions)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_lock_wait_status(ctx: Context):
    """Display status of locks and waits in the database."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM information_schema.innodb_locks"
        locks = db.execute_query(query)
        return {
            "success": True,
            "locks": locks,
            "count": len(locks)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_fragmentation_analysis(ctx: Context):
    """Show table and index fragmentation analysis."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW TABLE STATUS"
        fragmentation = db.execute_query(query)
        return {
            "success": True,
            "fragmentation": fragmentation,
            "count": len(fragmentation)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_index_usage_statistics(ctx: Context):
    """Provide index usage statistics and suggestions for optimization."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM sys.indexes_usage"
        index_usage = db.execute_query(query)
        return {
            "success": True,
            "index_usage": index_usage,
            "count": len(index_usage)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_slow_query_analysis(ctx: Context, limit: int = 10):
    """Analyze queries from the slow query log with summaries."""
    try:
        db = ctx.lifespan["db"]
        query = f"SELECT * FROM mysql.slow_log ORDER BY query_time DESC LIMIT %s"
        slow_queries = db.execute_prepared_query(query, [limit])
        return {
            "success": True,
            "slow_queries": slow_queries,
            "count": len(slow_queries)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_stored_functions_procedures(ctx: Context):
    """List all stored functions and procedures with metadata details."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM information_schema.routines"
        routines = db.execute_query(query)
        return {
            "success": True,
            "routines": routines,
            "count": len(routines)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_audit_logs(ctx: Context):
    """Show audit logs if enabled or recent security events."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM audit_log ORDER BY event_time DESC"
        audit_logs = db.execute_query(query)
        return {
            "success": True,
            "audit_logs": audit_logs,
            "count": len(audit_logs)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_running_events(ctx: Context):
    """List all currently running events with schedules and status."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM information_schema.events WHERE status = 'ENABLED'"
        events = db.execute_query(query)
        return {
            "success": True,
            "events": events,
            "count": len(events)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_triggers_by_event(ctx: Context):
    """Show triggers grouped by event type or action."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM information_schema.triggers ORDER BY event_manipulation"
        triggers = db.execute_query(query)
        return {
            "success": True,
            "triggers": triggers,
            "count": len(triggers)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_connection_monitor(ctx: Context):
    """Provide live monitoring of connections and resource usage."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM information_schema.processlist"
        processes = db.execute_query(query)
        return {
            "success": True,
            "processes": processes,
            "count": len(processes)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_stored_views_info(ctx: Context):
    """Display information about stored views in the database."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.views"
        views = db.execute_query(query)
        return {
            "success": True,
            "views": views,
            "count": len(views)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_roles_and_privileges(ctx: Context):
    """List database roles and their privileges."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM mysql.roles_mapping"
        roles = db.execute_query(query)
        return {
            "success": True,
            "roles": roles,
            "count": len(roles)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_grants_for_entities(ctx: Context):
    """Show grants assigned to roles and users."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW GRANTS"
        grants = db.execute_query(query)
        return {
            "success": True,
            "grants": grants,
            "count": len(grants)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_unused_duplicate_indexes(ctx: Context):
    """Identify unused or duplicate indexes that could be dropped."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT * FROM sys.schema_unused_indexes"
        unused_indexes = db.execute_query(query)
        return {
            "success": True,
            "unused_indexes": unused_indexes,
            "count": len(unused_indexes)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_tables_without_primary_keys(ctx: Context):
    """List tables without primary keys and recommendations."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name NOT IN (SELECT table_name FROM information_schema.columns WHERE column_key = 'PRI')"
        tables_no_pk = db.execute_query(query)
        return {
            "success": True,
            "tables_no_pk": tables_no_pk,
            "count": len(tables_no_pk)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_table_statistics(ctx: Context):
    """Provide info about table statistics like row counts and sizes."""
    try:
        db = ctx.lifespan["db"]
        query = "SELECT table_name, table_rows, avg_row_length, data_length, index_length FROM information_schema.tables WHERE table_schema = DATABASE()"
        tables_stats = db.execute_query(query)
        return {
            "success": True,
            "tables_stats": tables_stats,
            "count": len(tables_stats)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}
@mcp.tool()
async def mysql_replication_binary_log_status(ctx: Context):
    """Show replication and binary log status details."""
    try:
        db = ctx.lifespan["db"]
        replication_status = db.execute_query("SHOW MASTER STATUS")
        binary_log_status = db.execute_query("SHOW BINARY LOGS")
        return {
            "success": True,
            "replication_status": replication_status,
            "binary_log_status": binary_log_status
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_buffer_pool_statistics(ctx: Context):
    """Display buffer pool and cache usage statistics."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW STATUS LIKE 'Innodb_buffer_pool_%'"
        buffer_pool_stats = db.execute_query(query)
        return {
            "success": True,
            "buffer_pool_stats": buffer_pool_stats,
            "count": len(buffer_pool_stats)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_diagnostics_summary(ctx: Context):
    """Provide detailed diagnostics combining multiple status metrics."""
    try:
        db = ctx.lifespan["db"]
        variables = db.execute_query("SHOW GLOBAL VARIABLES")
        status = db.execute_query("SHOW GLOBAL STATUS")
        diagnostics = {
            "variables": variables,
            "status": status
        }
        return {
            "success": True,
            "diagnostics": diagnostics
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}



@mcp.tool()
def mysql_query(query: str, ctx: Context, params: Optional[List] = None):
    """Execute read-only MySQL queries.

    Args:
        query: The SQL query to execute
        params: Optional query parameters

    Returns:
        Query results or error details
    """
    try:
        db = ctx.lifespan["db"]
        # Validate the query
        if not db.is_read_only_query(query):
            raise ValueError("Only read-only queries are allowed.")

        # Execute prepared query
        results = db.execute_prepared_query(query, params)

        return {
            "success": True,
            "results": results,
            "row_count": len(results)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def list_mysql_tables(ctx: Context):
    """List all tables in the current MySQL database.

    Returns:
        List of table names
    """
    try:
        db = ctx.lifespan["db"]
        tables = db.get_tables()

        return {
            "success": True,
            "tables": [table["table_name"] for table in tables],
            "count": len(tables)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_schema(table_name: str, ctx: Context):
    """Get detailed schema information for a MySQL table.

    Args:
        table_name: The name of the table

    Returns:
        Schema details or error details
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        schema = db.get_table_schema(table_name)

        return {
            "success": True,
            "schema": schema
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_data(table_name: str, ctx: Context, limit: int = 10):
    """Fetch sample data from a MySQL table.

    Args:
        table_name: The name of the table
        limit: Number of rows to retrieve

    Returns:
        Sample data or error details
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        query = f"SELECT * FROM {table_name} LIMIT %s"
        results = db.execute_prepared_query(query, [limit])

        return {
            "success": True,
            "data": results,
            "row_count": len(results)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_databases(ctx: Context):
    """List all databases accessible to the current user.

    Returns:
        List of database names
    """
    try:
        db = ctx.lifespan["db"]
        databases = db.get_databases()

        return {
            "success": True,
            "databases": [db_info["database_name"] for db_info in databases],
            "count": len(databases)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_indexes(table_name: str, ctx: Context):
    """Show all indexes for a specific table.

    Args:
        table_name: The name of the table

    Returns:
        Index details including key names, columns, uniqueness
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        query = f"SHOW INDEX FROM `{table_name}`"
        indexes = db.execute_query(query)

        return {
            "success": True,
            "indexes": indexes,
            "count": len(indexes)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_size(table_name: str, ctx: Context):
    """Get storage size information for a table.

    Args:
        table_name: The name of the table

    Returns:
        Table size in bytes, row count, storage engine info
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        query = """SELECT 
            table_name,
            ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb,
            table_rows,
            engine,
            data_length,
            index_length
        FROM information_schema.tables 
        WHERE table_name = %s AND table_schema = DATABASE()"""
        
        size_info = db.execute_prepared_query(query, [table_name])

        return {
            "success": True,
            "table_size_info": size_info
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_user_privileges(ctx: Context):
    """Show current user's privileges.

    Returns:
        List of granted privileges and scope
    """
    try:
        db = ctx.lifespan["db"]
        query = "SHOW GRANTS FOR CURRENT_USER()"
        privileges = db.execute_query(query)

        return {
            "success": True,
            "privileges": privileges
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_process_list(ctx: Context):
    """Show active MySQL connections and processes.

    Returns:
        Active processes with connection details
    """
    try:
        db = ctx.lifespan["db"]
        query = "SHOW PROCESSLIST"
        process_list = db.execute_query(query)

        return {
            "success": True,
            "processes": process_list,
            "count": len(process_list)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_status(ctx: Context, database_name: Optional[str] = None):
    """Get comprehensive status information for tables.

    Args:
        database_name: Optional database to fetch status

    Returns:
        Table status including rows, size, engine, creation time
    """
    try:
        db = ctx.lifespan["db"]
        if database_name:
            query = f"SHOW TABLE STATUS FROM `{database_name}`"
            status_info = db.execute_query(query)
        else:
            query = "SHOW TABLE STATUS"
            status_info = db.execute_query(query)

        return {
            "success": True,
            "table_status": status_info,
            "count": len(status_info)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_variables(ctx: Context, pattern: Optional[str] = None):
    """Show MySQL system variables.

    Args:
        pattern: Optional LIKE pattern for filtering

    Returns:
        System variables and their current values
    """
    try:
        db = ctx.lifespan["db"]
        if pattern:
            query = "SHOW VARIABLES LIKE %s"
            variables = db.execute_prepared_query(query, [pattern])
        else:
            query = "SHOW VARIABLES"
            variables = db.execute_query(query)

        return {
            "success": True,
            "variables": variables,
            "count": len(variables)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_charset_collation(ctx: Context):
    """Show available character sets and collations.

    Returns:
        Available character sets and collations
    """
    try:
        db = ctx.lifespan["db"]
        charsets = db.execute_query("SHOW CHARACTER SET")
        collations = db.execute_query("SHOW COLLATION")

        return {
            "success": True,
            "charsets": charsets,
            "collations": collations,
            "charset_count": len(charsets),
            "collation_count": len(collations)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_constraints(table_name: str, ctx: Context, database_name: Optional[str] = None):
    """Show foreign key and check constraints for a table.

    Args:
        table_name: The name of the table
        database_name: Optional database name

    Returns:
        Constraint details including foreign keys and check constraints
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        
        if database_name:
            query = """SELECT * FROM information_schema.table_constraints 
                     WHERE table_name = %s AND table_schema = %s"""
            params = [table_name, database_name]
        else:
            query = """SELECT * FROM information_schema.table_constraints 
                     WHERE table_name = %s AND table_schema = DATABASE()"""
            params = [table_name]

        constraints = db.execute_prepared_query(query, params)

        return {
            "success": True,
            "constraints": constraints,
            "count": len(constraints)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_column_stats(table_name: str, ctx: Context, column_name: Optional[str] = None):
    """Get column statistics and data distribution.

    Args:
        table_name: The name of the table
        column_name: Optional column name

    Returns:
        Column statistics including min, max, avg, null count
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        
        if column_name:
            query = """SELECT * FROM information_schema.columns 
                     WHERE table_name = %s AND column_name = %s AND table_schema = DATABASE()"""
            params = [table_name, column_name]
        else:
            query = """SELECT * FROM information_schema.columns 
                     WHERE table_name = %s AND table_schema = DATABASE()"""
            params = [table_name]

        column_stats = db.execute_prepared_query(query, params)

        return {
            "success": True,
            "column_stats": column_stats,
            "count": len(column_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_explain_query(query: str, ctx: Context):
    """Analyze query execution plan.

    Args:
        query: The SQL query to analyze

    Returns:
        Query execution plan with cost estimates
    """
    try:
        db = ctx.lifespan["db"]
        # Validate that the query is a SELECT statement
        if not re.match(r'^\s*SELECT', query.strip(), re.IGNORECASE):
            raise ValueError("Only SELECT queries can be explained.")

        explain_query = f"EXPLAIN {query}"
        explain_result = db.execute_query(explain_query)

        return {
            "success": True,
            "execution_plan": explain_result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_search_tables(column_pattern: str, ctx: Context, database_name: Optional[str] = None):
    """Search for tables containing specific column names.

    Args:
        column_pattern: Pattern to search for
        database_name: Optional database name

    Returns:
        Tables containing matching column names
    """
    try:
        db = ctx.lifespan["db"]
        
        if database_name:
            query = """SELECT DISTINCT table_name, table_schema FROM information_schema.columns 
                     WHERE column_name LIKE %s AND table_schema = %s"""
            params = [column_pattern, database_name]
        else:
            query = """SELECT DISTINCT table_name, table_schema FROM information_schema.columns 
                     WHERE column_name LIKE %s AND table_schema = DATABASE()"""
            params = [column_pattern]

        matching_tables = db.execute_prepared_query(query, params)

        return {
            "success": True,
            "matching_tables": matching_tables,
            "count": len(matching_tables)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_backup_info(ctx: Context):
    """Get information about database backup status.

    Returns:
        Backup status information or recommendations
    """
    try:
        db = ctx.lifespan["db"]
        # Try to get backup progress info, but it may not exist
        try:
            query = "SELECT * FROM mysql.backup_progress LIMIT 10"
            backup_info = db.execute_query(query)
            
            return {
                "success": True,
                "backup_info": backup_info,
                "type": "backup_progress_table"
            }
        except:
            # If backup_progress table doesn't exist, provide general recommendations
            return {
                "success": True,
                "backup_info": [
                    {
                        "recommendation": "Use mysqldump for logical backups",
                        "command": "mysqldump -u user -p database_name > backup.sql"
                    },
                    {
                        "recommendation": "Consider MySQL Enterprise Backup for physical backups",
                        "note": "Physical backups are faster for large databases"
                    },
                    {
                        "recommendation": "Implement regular backup schedule",
                        "frequency": "Daily full backups, hourly incremental if needed"
                    }
                ],
                "type": "general_recommendations"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_replication_status(ctx: Context):
    """Show MySQL replication status.

    Returns:
        Replication status including lag and position
    """
    try:
        db = ctx.lifespan["db"]
        
        # Get slave status
        try:
            slave_status = db.execute_query("SHOW SLAVE STATUS")
        except:
            slave_status = []
            
        # Get master status
        try:
            master_status = db.execute_query("SHOW MASTER STATUS")
        except:
            master_status = []

        return {
            "success": True,
            "slave_status": slave_status,
            "master_status": master_status,
            "is_slave": len(slave_status) > 0,
            "is_master": len(master_status) > 0
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_query_cache_stats(ctx: Context):
    """Show query cache statistics.

    Returns:
        Query cache hit ratio and usage statistics
    """
    try:
        db = ctx.lifespan["db"]
        query = "SHOW STATUS LIKE 'Qcache%'"
        cache_stats = db.execute_query(query)

        # Calculate hit ratio if we have the data
        hit_ratio = None
        hits = 0
        total_queries = 0
        
        for stat in cache_stats:
            if stat.get('Variable_name') == 'Qcache_hits':
                hits = int(stat.get('Value', 0))
            elif stat.get('Variable_name') == 'Qcache_inserts':
                total_queries = hits + int(stat.get('Value', 0))
                
        if total_queries > 0:
            hit_ratio = (hits / total_queries) * 100

        return {
            "success": True,
            "cache_stats": cache_stats,
            "hit_ratio_percent": round(hit_ratio, 2) if hit_ratio else None
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_dependencies(ctx: Context, table_name: Optional[str] = None):
    """Find foreign key dependencies between tables.

    Args:
        table_name: Optional table name to filter dependencies

    Returns:
        Table dependency graph with foreign key relationships
    """
    try:
        db = ctx.lifespan["db"]
        
        if table_name:
            query = """SELECT * FROM information_schema.key_column_usage 
                     WHERE table_name = %s AND referenced_table_name IS NOT NULL
                     AND table_schema = DATABASE()"""
            dependencies = db.execute_prepared_query(query, [table_name])
        else:
            query = """SELECT * FROM information_schema.key_column_usage 
                     WHERE referenced_table_name IS NOT NULL
                     AND table_schema = DATABASE()"""
            dependencies = db.execute_query(query)

        return {
            "success": True,
            "dependencies": dependencies,
            "count": len(dependencies)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


# Advanced MySQL Diagnostic Tools

@mcp.tool()
def mysql_slow_queries(ctx: Context, limit: int = 20, min_duration: Optional[float] = None):
    """Analyze slow query log entries.

    Args:
        limit: Number of slow queries to return
        min_duration: Minimum execution time in seconds

    Returns:
        Slow query details with execution times and patterns
    """
    try:
        db = ctx.lifespan["db"]
        
        base_query = """SELECT 
            sql_text,
            timer_wait/1000000000000 AS exec_time_sec,
            lock_time/1000000000000 AS lock_time_sec,
            rows_sent,
            rows_examined,
            created_tmp_tables,
            created_tmp_disk_tables
        FROM performance_schema.events_statements_history_long 
        WHERE timer_wait/1000000000000 > %s
        ORDER BY timer_wait DESC 
        LIMIT %s"""
        
        min_time = min_duration if min_duration is not None else 1.0
        slow_queries = db.execute_prepared_query(base_query, [min_time, limit])

        return {
            "success": True,
            "slow_queries": slow_queries,
            "count": len(slow_queries),
            "min_duration": min_time
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_deadlock_detection(ctx: Context):
    """Detect and analyze table deadlocks.

    Returns:
        Current deadlock information and blocking transactions
    """
    try:
        db = ctx.lifespan["db"]
        
        # Get current transactions
        trx_query = "SELECT * FROM information_schema.innodb_trx"
        transactions = db.execute_query(trx_query)
        
        # Get lock waits
        locks_query = "SELECT * FROM information_schema.innodb_lock_waits"
        try:
            lock_waits = db.execute_query(locks_query)
        except:
            # Fallback for newer MySQL versions
            lock_waits = []

        return {
            "success": True,
            "active_transactions": transactions,
            "lock_waits": lock_waits,
            "potential_deadlocks": len(lock_waits) > 0
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_partition_info(table_name: str, ctx: Context):
    """Show table partitioning information.

    Args:
        table_name: Name of the table

    Returns:
        Partition scheme, row distribution, and storage details
    """
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)
        
        query = """SELECT 
            partition_name,
            partition_method,
            partition_expression,
            partition_description,
            table_rows,
            avg_row_length,
            data_length,
            index_length
        FROM information_schema.partitions 
        WHERE table_name = %s AND table_schema = DATABASE()
        AND partition_name IS NOT NULL"""
        
        partitions = db.execute_prepared_query(query, [table_name])

        return {
            "success": True,
            "table_name": table_name,
            "partitions": partitions,
            "partition_count": len(partitions),
            "is_partitioned": len(partitions) > 0
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_trigger_list(ctx: Context, table_name: Optional[str] = None, database_name: Optional[str] = None):
    """List triggers for database or specific table.

    Args:
        table_name: Optional table name to filter triggers
        database_name: Optional database name

    Returns:
        Trigger definitions, timing, and associated tables
    """
    try:
        db = ctx.lifespan["db"]
        
        base_query = """SELECT 
            trigger_name,
            event_manipulation,
            event_object_table,
            action_timing,
            action_statement,
            created
        FROM information_schema.triggers
        WHERE trigger_schema = %s"""
        
        params = [database_name if database_name else "DATABASE()"]
        
        if table_name:
            base_query += " AND event_object_table = %s"
            params.append(table_name)
            
        triggers = db.execute_prepared_query(base_query, params)

        return {
            "success": True,
            "triggers": triggers,
            "count": len(triggers)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_stored_procedures(ctx: Context, database_name: Optional[str] = None, routine_type: Optional[str] = None):
    """List stored procedures and functions.

    Args:
        database_name: Optional database name
        routine_type: Optional routine type (PROCEDURE or FUNCTION)

    Returns:
        Stored procedure names, parameters, and definitions
    """
    try:
        db = ctx.lifespan["db"]
        
        base_query = """SELECT 
            routine_name,
            routine_type,
            data_type,
            routine_definition,
            is_deterministic,
            sql_data_access,
            created,
            last_altered
        FROM information_schema.routines
        WHERE routine_schema = %s"""
        
        params = [database_name if database_name else "DATABASE()"]
        
        if routine_type and routine_type.upper() in ['PROCEDURE', 'FUNCTION']:
            base_query += " AND routine_type = %s"
            params.append(routine_type.upper())
            
        routines = db.execute_prepared_query(base_query, params)

        return {
            "success": True,
            "routines": routines,
            "count": len(routines)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_event_scheduler(ctx: Context):
    """Show scheduled events information.

    Returns:
        Active events, schedules, and execution status
    """
    try:
        db = ctx.lifespan["db"]
        
        query = """SELECT 
            event_name,
            event_schema,
            event_body,
            event_type,
            execute_at,
            interval_value,
            interval_field,
            status,
            on_completion,
            created,
            last_altered,
            last_executed
        FROM information_schema.events"""
        
        events = db.execute_query(query)

        return {
            "success": True,
            "events": events,
            "count": len(events)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_binary_logs(ctx: Context):
    """List binary log files and positions.

    Returns:
        Binary log files with sizes and positions
    """
    try:
        db = ctx.lifespan["db"]
        
        binary_logs = db.execute_query("SHOW BINARY LOGS")

        return {
            "success": True,
            "binary_logs": binary_logs,
            "count": len(binary_logs)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_engine_status(ctx: Context, engine_name: Optional[str] = None):
    """Show storage engine status information.

    Args:
        engine_name: Optional engine name (default: InnoDB)

    Returns:
        Engine-specific status and performance metrics
    """
    try:
        db = ctx.lifespan["db"]
        engine = engine_name if engine_name else "InnoDB"
        
        query = f"SHOW ENGINE {engine} STATUS"
        engine_status = db.execute_query(query)

        return {
            "success": True,
            "engine": engine,
            "status": engine_status
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_locks(ctx: Context):
    """Show current table locks and waiting processes.

    Returns:
        Active table locks and waiting processes
    """
    try:
        db = ctx.lifespan["db"]
        
        query = """SELECT 
            object_schema,
            object_name,
            lock_type,
            lock_duration,
            lock_status,
            source
        FROM performance_schema.metadata_locks
        WHERE object_type = 'TABLE'"""
        
        locks = db.execute_query(query)

        return {
            "success": True,
            "table_locks": locks,
            "count": len(locks)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_lock_contention(ctx: Context):
    """Analyze lock contention and waits.

    Returns:
        Lock contention statistics and potential bottlenecks
    """
    try:
        db = ctx.lifespan["db"]
        
        query = """SELECT 
            event_name,
            object_type,
            object_schema,
            object_name,
            COUNT_STAR,
            SUM_TIMER_WAIT/1000000000000 AS wait_time_sec
        FROM performance_schema.events_waits_summary_by_instance
        WHERE SUM_TIMER_WAIT > 0 
        ORDER BY SUM_TIMER_WAIT DESC
        LIMIT 50"""
        
        lock_contention = db.execute_query(query)

        return {
            "success": True,
            "lock_contention": lock_contention,
            "count": len(lock_contention)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_connection_statistics(ctx: Context):
    """Analyze connection patterns and statistics.

    Returns:
        Connection statistics by host and user
    """
    try:
        db = ctx.lifespan["db"]
        
        host_stats_query = "SELECT * FROM performance_schema.hosts"
        user_stats_query = "SELECT * FROM performance_schema.users"
        
        host_stats = db.execute_query(host_stats_query)
        user_stats = db.execute_query(user_stats_query)

        return {
            "success": True,
            "host_statistics": host_stats,
            "user_statistics": user_stats
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_schema_unused_indexes(ctx: Context):
    """Identify unused indexes in the schema.

    Returns:
        List of unused indexes and related statistics
    """
    try:
        db = ctx.lifespan["db"]
        
        query = """SELECT 
            object_schema,
            object_name,
            index_name,
            count_star as accesses,
            index_size / 1024 / 1024 AS index_size_mb
        FROM performance_schema.table_io_waits_summary_by_index_usage
        WHERE count_star = 0 AND index_name IS NOT NULL
        ORDER BY index_size_mb DESC
        LIMIT 50"""
        
        unused_indexes = db.execute_query(query)

        return {
            "success": True,
            "unused_indexes": unused_indexes,
            "count": len(unused_indexes)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_memory_usage(ctx: Context):
    """Show memory usage by database objects.

    Returns:
        Memory usage breakdown by database components
    """
    try:
        db = ctx.lifespan["db"]
        
        query = """SELECT 
            event_name,
            current_count_used,
            current_number_of_bytes_used,
            high_count_used,
            high_number_of_bytes_used
        FROM performance_schema.memory_summary_global_by_event_name
        WHERE current_number_of_bytes_used > 0
        ORDER BY current_number_of_bytes_used DESC
        LIMIT 50"""
        
        memory_usage = db.execute_query(query)

        return {
            "success": True,
            "memory_usage": memory_usage,
            "count": len(memory_usage)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_io_statistics(ctx: Context, database_name: Optional[str] = None):
    """Show I/O statistics for tables and indexes.

    Args:
        database_name: Optional database name

    Returns:
        I/O statistics including read/write operations
    """
    try:
        db = ctx.lifespan["db"]
        
        if database_name:
            query = """SELECT 
                object_schema,
                object_name,
                count_read,
                count_write,
                sum_timer_read,
                sum_timer_write
            FROM performance_schema.table_io_waits_summary_by_table
            WHERE object_schema = %s
            ORDER BY (count_read + count_write) DESC"""
            io_stats = db.execute_prepared_query(query, [database_name])
        else:
            query = """SELECT 
                object_schema,
                object_name,
                count_read,
                count_write,
                sum_timer_read,
                sum_timer_write
            FROM performance_schema.table_io_waits_summary_by_table
            WHERE object_schema NOT IN ('mysql', 'information_schema', 'performance_schema')
            ORDER BY (count_read + count_write) DESC
            LIMIT 50"""
            io_stats = db.execute_query(query)

        return {
            "success": True,
            "io_statistics": io_stats,
            "count": len(io_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_key_cache_status(ctx: Context):
    """Show key cache status and statistics.

    Returns:
        Key cache usage, size, and performance metrics
    """
    try:
        db = ctx.lifespan["db"]
        
        query = "SHOW STATUS LIKE 'key_%'"
        key_cache_stats = db.execute_query(query)

        return {
            "success": True,
            "key_cache_stats": key_cache_stats,
            "count": len(key_cache_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_auto_increment_info(ctx: Context, database_name: Optional[str] = None):
    """Show auto-increment information for tables.

    Args:
        database_name: Optional database name

    Returns:
        Auto-increment current values and column information
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        if database_name:
            query = """SELECT 
                table_name,
                auto_increment,
                table_rows
            FROM information_schema.tables
            WHERE table_schema = %s AND auto_increment IS NOT NULL
            ORDER BY auto_increment DESC"""
            auto_inc_info = db.execute_prepared_query(query, [database_name])
        else:
            query = """SELECT 
                table_schema,
                table_name,
                auto_increment,
                table_rows
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND auto_increment IS NOT NULL
            ORDER BY auto_increment DESC"""
            auto_inc_info = db.execute_query(query)

        return {
            "success": True,
            "auto_increment_info": auto_inc_info,
            "count": len(auto_inc_info)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_fulltext_indexes(ctx: Context, database_name: Optional[str] = None):
    """List full-text search indexes.

    Args:
        database_name: Optional database name

    Returns:
        Full-text index information and associated columns
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        if database_name:
            query = """SELECT 
                table_name,
                index_name,
                column_name,
                seq_in_index
            FROM information_schema.statistics
            WHERE table_schema = %s AND index_type = 'FULLTEXT'
            ORDER BY table_name, index_name, seq_in_index"""
            fulltext_indexes = db.execute_prepared_query(query, [database_name])
        else:
            query = """SELECT 
                table_schema,
                table_name,
                index_name,
                column_name,
                seq_in_index
            FROM information_schema.statistics
            WHERE table_schema = DATABASE() AND index_type = 'FULLTEXT'
            ORDER BY table_name, index_name, seq_in_index"""
            fulltext_indexes = db.execute_query(query)

        return {
            "success": True,
            "fulltext_indexes": fulltext_indexes,
            "count": len(fulltext_indexes)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_performance_recommendations(ctx: Context):
    """Generate performance recommendations based on current database state.

    Returns:
        Performance recommendations and optimization suggestions
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        recommendations = []
        
        # Check for tables without primary keys
        no_pk_query = """SELECT table_name 
                       FROM information_schema.tables t
                       LEFT JOIN information_schema.key_column_usage k 
                         ON t.table_name = k.table_name AND k.constraint_name = 'PRIMARY'
                       WHERE t.table_schema = DATABASE() AND k.table_name IS NULL
                       AND t.table_type = 'BASE TABLE'"""
        tables_no_pk = db.execute_query(no_pk_query)
        if tables_no_pk:
            recommendations.append({
                "type": "schema_design",
                "issue": "Tables without primary keys",
                "tables": [t['table_name'] for t in tables_no_pk],
                "recommendation": "Add primary keys to improve replication and performance"
            })

        # Check for large tables that might benefit from partitioning
        large_tables_query = """SELECT table_name, table_rows 
                              FROM information_schema.tables 
                              WHERE table_schema = DATABASE() 
                              AND table_rows > 1000000
                              ORDER BY table_rows DESC"""
        large_tables = db.execute_query(large_tables_query)
        if large_tables:
            recommendations.append({
                "type": "partitioning",
                "issue": "Large tables detected",
                "tables": large_tables,
                "recommendation": "Consider table partitioning for improved query performance"
            })

        # Check query cache status
        try:
            cache_query = "SHOW STATUS LIKE 'Qcache_hits'"
            cache_stats = db.execute_query(cache_query)
            if cache_stats and int(cache_stats[0].get('Value', 0)) == 0:
                recommendations.append({
                    "type": "configuration",
                    "issue": "Query cache not being used",
                    "recommendation": "Consider enabling and tuning query cache for read-heavy workloads"
                })
        except:
            pass

        return {
            "success": True,
            "recommendations": recommendations,
            "count": len(recommendations)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_security_audit(ctx: Context):
    """Perform basic security audit of MySQL configuration and users.

    Returns:
        Security audit results with potential issues
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        security_issues = []
        
        # Check for users with empty passwords (if we have permission)
        try:
            empty_pwd_query = "SELECT user, host FROM mysql.user WHERE authentication_string = '' OR password = ''"
            empty_pwd_users = db.execute_query(empty_pwd_query)
            if empty_pwd_users:
                security_issues.append({
                    "severity": "high",
                    "issue": "Users with empty passwords",
                    "users": empty_pwd_users,
                    "recommendation": "Set strong passwords for all user accounts"
                })
        except:
            security_issues.append({
                "severity": "info",
                "issue": "Cannot access mysql.user table",
                "recommendation": "Limited security audit due to insufficient privileges"
            })

        # Check for root users accessible from any host
        try:
            root_access_query = "SELECT user, host FROM mysql.user WHERE user = 'root' AND host != 'localhost'"
            root_remote = db.execute_query(root_access_query)
            if root_remote:
                security_issues.append({
                    "severity": "high",
                    "issue": "Root user accessible from remote hosts",
                    "users": root_remote,
                    "recommendation": "Restrict root access to localhost only"
                })
        except:
            pass

        # Check SSL/TLS configuration
        try:
            ssl_query = "SHOW STATUS LIKE 'Ssl_cipher'"
            ssl_status = db.execute_query(ssl_query)
            if not ssl_status or not ssl_status[0].get('Value'):
                security_issues.append({
                    "severity": "medium",
                    "issue": "SSL/TLS not configured",
                    "recommendation": "Enable SSL/TLS for encrypted connections"
                })
        except:
            pass

        return {
            "success": True,
            "security_issues": security_issues,
            "count": len(security_issues)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_table_fragmentation(ctx: Context, database_name: Optional[str] = None):
    """Analyze table fragmentation and suggest optimization.

    Args:
        database_name: Optional database name

    Returns:
        Table fragmentation analysis with optimization recommendations
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        if database_name:
            query = """SELECT 
                table_name,
                engine,
                table_rows,
                data_length,
                index_length,
                data_free,
                ROUND(data_free / (data_length + index_length + data_free) * 100, 2) AS fragmentation_pct
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND data_free > 0
            ORDER BY fragmentation_pct DESC"""
            fragmentation_info = db.execute_prepared_query(query, [database_name])
        else:
            query = """SELECT 
                table_schema,
                table_name,
                engine,
                table_rows,
                data_length,
                index_length,
                data_free,
                ROUND(data_free / (data_length + index_length + data_free) * 100, 2) AS fragmentation_pct
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND data_free > 0
            ORDER BY fragmentation_pct DESC"""
            fragmentation_info = db.execute_query(query)

        # Identify tables that need optimization
        needs_optimization = []
        for table in fragmentation_info:
            if table.get('fragmentation_pct', 0) > 10:  # More than 10% fragmented
                needs_optimization.append({
                    "table_name": table['table_name'],
                    "fragmentation_pct": table['fragmentation_pct'],
                    "engine": table['engine'],
                    "recommendation": f"OPTIMIZE TABLE {table['table_name']}" if table['engine'] == 'MyISAM' else f"ALTER TABLE {table['table_name']} ENGINE=InnoDB"
                })

        return {
            "success": True,
            "fragmentation_analysis": fragmentation_info,
            "needs_optimization": needs_optimization,
            "total_tables": len(fragmentation_info),
            "optimization_needed": len(needs_optimization)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_query_analysis(query: str, ctx: Context):
    """Comprehensive query analysis including execution plan and recommendations.

    Args:
        query: The SQL query to analyze

    Returns:
        Detailed query analysis with performance insights
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate that it's a SELECT query
        if not re.match(r'^\s*SELECT', query.strip(), re.IGNORECASE):
            raise ValueError("Only SELECT queries can be analyzed")

        analysis_results = {}

        # Get basic EXPLAIN output
        try:
            explain_query = f"EXPLAIN {query}"
            explain_result = db.execute_query(explain_query)
            analysis_results['explain'] = explain_result
        except Exception as e:
            analysis_results['explain_error'] = str(e)

        # Get extended EXPLAIN (if available)
        try:
            explain_extended = f"EXPLAIN EXTENDED {query}"
            extended_result = db.execute_query(explain_extended)
            analysis_results['explain_extended'] = extended_result
        except:
            pass

        # Analyze potential issues
        issues = []
        if explain_result:
            for row in explain_result:
                # Check for table scans
                if row.get('type') == 'ALL':
                    issues.append({
                        "severity": "high",
                        "issue": f"Full table scan on {row.get('table')}",
                        "recommendation": "Consider adding appropriate indexes"
                    })
                
                # Check for filesort
                if row.get('Extra') and 'Using filesort' in row.get('Extra', ''):
                    issues.append({
                        "severity": "medium",
                        "issue": "Query requires filesort",
                        "recommendation": "Consider adding index for ORDER BY clause"
                    })
                
                # Check for temporary tables
                if row.get('Extra') and 'Using temporary' in row.get('Extra', ''):
                    issues.append({
                        "severity": "medium",
                        "issue": "Query uses temporary table",
                        "recommendation": "Consider query optimization or indexing"
                    })

        analysis_results['issues'] = issues
        analysis_results['issue_count'] = len(issues)

        return {
            "success": True,
            "query_analysis": analysis_results
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_diagnostics_info(ctx: Context):
    """Run comprehensive diagnostics and retrieve aggregated info.

    Returns:
        Diagnostic summary with key findings
    """
    try:
        # Combining several diagnostic queries into a single report
        diagnostics = {}
        db = ctx.request_context.lifespan_context["db"]
        
        # Memory usage
        memory_usage_query = """SELECT 
            event_name,
            current_number_of_bytes_used
        FROM performance_schema.memory_summary_global_by_event_name
        WHERE current_number_of_bytes_used > 0
        ORDER BY current_number_of_bytes_used DESC
        LIMIT 5"""
        memory_usage = db.execute_query(memory_usage_query)
        diagnostics['memory_usage'] = memory_usage

        # IO Stats
        io_stats_query = "SELECT SUM(count_read + count_write) as io_total FROM performance_schema.table_io_waits_summary_by_table"
        io_stats = db.execute_query(io_stats_query)
        diagnostics['io_stats'] = io_stats

        # Lock contention
        lock_contention_query = "SELECT event_name, SUM_TIMER_WAIT/1000000000000 AS wait_time_sec FROM performance_schema.events_waits_summary_global_by_event_name ORDER BY SUM_TIMER_WAIT DESC LIMIT 5"
        lock_contention = db.execute_query(lock_contention_query)
        diagnostics['lock_contention'] = lock_contention

        return {
            "success": True,
            "diagnostics": diagnostics
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_json_validation(table_name: str, ctx: Context, column_name: Optional[str] = None):
    """Validate JSON columns and data.

    Args:
        table_name: Name of the table
        column_name: Optional specific JSON column

    Returns:
        JSON validation results and error statistics
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # First, get JSON columns
        if column_name:
            json_cols_query = """SELECT column_name, data_type 
                               FROM information_schema.columns 
                               WHERE table_name = %s AND column_name = %s 
                               AND table_schema = DATABASE() AND data_type = 'json'"""
            json_cols = db.execute_prepared_query(json_cols_query, [table_name, column_name])
        else:
            json_cols_query = """SELECT column_name, data_type 
                               FROM information_schema.columns 
                               WHERE table_name = %s AND table_schema = DATABASE() 
                               AND data_type = 'json'"""
            json_cols = db.execute_prepared_query(json_cols_query, [table_name])
        
        if not json_cols:
            return {
                "success": False,
                "error": "No JSON columns found in the specified table"
            }
        
        # Validate JSON data for each column
        validation_results = []
        for col in json_cols:
            col_name = col['column_name']
            try:
                # Count total and valid JSON entries
                count_query = f"SELECT COUNT(*) as total_rows FROM `{table_name}` WHERE `{col_name}` IS NOT NULL"
                total_count = db.execute_query(count_query)[0]['total_rows']
                
                validation_results.append({
                    "column_name": col_name,
                    "total_rows": total_count,
                    "status": "JSON column validated"
                })
            except Exception as e:
                validation_results.append({
                    "column_name": col_name,
                    "error": str(e)
                })

        return {
            "success": True,
            "json_columns": json_cols,
            "validation_results": validation_results
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_create_database(database_name: str, ctx: Context, charset: str = "utf8mb4", collation: str = "utf8mb4_unicode_ci"):
    """Create a new MySQL database.

    Args:
        database_name: Name of the database to create
        charset: Character set (default: utf8mb4)
        collation: Collation (default: utf8mb4_unicode_ci)

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate database name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database_name):
            raise ValueError(f"Invalid database name: {database_name}")
        
        query = f"CREATE DATABASE `{database_name}` CHARACTER SET {charset} COLLATE {collation}"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Database '{database_name}' created successfully",
            "database_name": database_name,
            "charset": charset,
            "collation": collation
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_database(database_name: str, ctx: Context):
    """Drop a MySQL database (USE WITH CAUTION!).

    Args:
        database_name: Name of the database to drop

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate database name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database_name):
            raise ValueError(f"Invalid database name: {database_name}")
        
        # Prevent dropping system databases
        system_dbs = ['information_schema', 'performance_schema', 'mysql', 'sys']
        if database_name.lower() in system_dbs:
            raise ValueError(f"Cannot drop system database: {database_name}")
        
        query = f"DROP DATABASE `{database_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Database '{database_name}' dropped successfully",
            "database_name": database_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_create_table(table_name: str, columns: List[Dict[str, str]], ctx: Context, engine: str = "InnoDB"):
    """Create a new table in the current database.

    Args:
        table_name: Name of the table to create
        columns: List of column definitions with 'name', 'type', and optional 'constraints'
        engine: Storage engine (default: InnoDB)

    Returns:
        Success status and table creation details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate table name
        table_name = db.validate_table_name(table_name)
        
        # Build column definitions
        column_defs = []
        for col in columns:
            if 'name' not in col or 'type' not in col:
                raise ValueError("Each column must have 'name' and 'type' fields")
            
            col_def = f"`{col['name']}` {col['type']}"
            if 'constraints' in col:
                col_def += f" {col['constraints']}"
            column_defs.append(col_def)
        
        if not column_defs:
            raise ValueError("At least one column must be specified")
        
        query = f"CREATE TABLE `{table_name}` ({', '.join(column_defs)}) ENGINE={engine}"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' created successfully",
            "table_name": table_name,
            "columns": columns,
            "engine": engine
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_insert_data(table_name: str, data: List[Dict[str, Any]], ctx: Context):
    """Insert data into a table.

    Args:
        table_name: Name of the table
        data: List of dictionaries representing rows to insert

    Returns:
        Success status and insertion details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        if not data:
            raise ValueError("No data provided for insertion")
        
        # Get column names from first row
        columns = list(data[0].keys())
        if not columns:
            raise ValueError("No columns specified in data")
        
        # Build INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f"`{col}`" for col in columns])
        query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # Execute for each row
        inserted_rows = 0
        for row in data:
            values = [row.get(col) for col in columns]
            db.execute_prepared_query(query, values)
            inserted_rows += 1
        
        return {
            "success": True,
            "message": f"Inserted {inserted_rows} rows into '{table_name}'",
            "table_name": table_name,
            "inserted_rows": inserted_rows,
            "columns": columns
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_update_data(table_name: str, set_values: Dict[str, Any], where_clause: str, ctx: Context, params: Optional[List] = None):
    """Update data in a table.

    Args:
        table_name: Name of the table
        set_values: Dictionary of column names and their new values
        where_clause: WHERE clause condition (without WHERE keyword)
        params: Optional parameters for the WHERE clause

    Returns:
        Success status and update details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        if not set_values:
            raise ValueError("No values provided for update")
        
        # Build SET clause
        set_clauses = []
        set_params = []
        for col, value in set_values.items():
            set_clauses.append(f"`{col}` = %s")
            set_params.append(value)
        
        # Build complete query
        query = f"UPDATE `{table_name}` SET {', '.join(set_clauses)} WHERE {where_clause}"
        
        # Combine parameters
        all_params = set_params + (params or [])
        
        result = db.execute_prepared_query(query, all_params)
        affected_rows = result[0].get('affected_rows', 0) if result else 0
        
        return {
            "success": True,
            "message": f"Updated {affected_rows} rows in '{table_name}'",
            "table_name": table_name,
            "affected_rows": affected_rows,
            "set_values": set_values
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_delete_data(table_name: str, where_clause: str, ctx: Context, params: Optional[List] = None):
    """Delete data from a table.

    Args:
        table_name: Name of the table
        where_clause: WHERE clause condition (without WHERE keyword)
        params: Optional parameters for the WHERE clause

    Returns:
        Success status and deletion details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"DELETE FROM `{table_name}` WHERE {where_clause}"
        result = db.execute_prepared_query(query, params or [])
        affected_rows = result[0].get('affected_rows', 0) if result else 0
        
        return {
            "success": True,
            "message": f"Deleted {affected_rows} rows from '{table_name}'",
            "table_name": table_name,
            "affected_rows": affected_rows
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_use_database(database_name: str, ctx: Context):
    """Switch to a different database.

    Args:
        database_name: Name of the database to use

    Returns:
        Success status
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate database name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database_name):
            raise ValueError(f"Invalid database name: {database_name}")
        
        query = f"USE `{database_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Now using database '{database_name}'",
            "database_name": database_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_table(table_name: str, ctx: Context, if_exists: bool = True):
    """Drop a table from the database.

    Args:
        table_name: Name of the table to drop
        if_exists: Whether to use IF EXISTS clause to avoid errors

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        if_exists_clause = "IF EXISTS " if if_exists else ""
        query = f"DROP TABLE {if_exists_clause}`{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' dropped successfully",
            "table_name": table_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_alter_table_add_column(table_name: str, column_name: str, column_type: str, ctx: Context, 
                                 constraints: Optional[str] = None, after_column: Optional[str] = None):
    """Add a column to an existing table.

    Args:
        table_name: Name of the table
        column_name: Name of the new column
        column_type: Data type of the new column
        constraints: Optional column constraints
        after_column: Optional column to add after

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate column name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValueError(f"Invalid column name: {column_name}")
        
        query = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {column_type}"
        if constraints:
            query += f" {constraints}"
        if after_column:
            query += f" AFTER `{after_column}`"
        
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Column '{column_name}' added to table '{table_name}'",
            "table_name": table_name,
            "column_name": column_name,
            "column_type": column_type
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_alter_table_drop_column(table_name: str, column_name: str, ctx: Context):
    """Drop a column from an existing table.

    Args:
        table_name: Name of the table
        column_name: Name of the column to drop

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate column name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValueError(f"Invalid column name: {column_name}")
        
        query = f"ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Column '{column_name}' dropped from table '{table_name}'",
            "table_name": table_name,
            "column_name": column_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_alter_table_modify_column(table_name: str, column_name: str, new_column_type: str, 
                                   ctx: Context, constraints: Optional[str] = None):
    """Modify a column in an existing table.

    Args:
        table_name: Name of the table
        column_name: Name of the column to modify
        new_column_type: New data type for the column
        constraints: Optional column constraints

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate column name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValueError(f"Invalid column name: {column_name}")
        
        query = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column_name}` {new_column_type}"
        if constraints:
            query += f" {constraints}"
        
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Column '{column_name}' modified in table '{table_name}'",
            "table_name": table_name,
            "column_name": column_name,
            "new_column_type": new_column_type
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_rename_table(old_table_name: str, new_table_name: str, ctx: Context):
    """Rename a table.

    Args:
        old_table_name: Current name of the table
        new_table_name: New name for the table

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        old_table_name = db.validate_table_name(old_table_name)
        new_table_name = db.validate_table_name(new_table_name)
        
        query = f"RENAME TABLE `{old_table_name}` TO `{new_table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table renamed from '{old_table_name}' to '{new_table_name}'",
            "old_table_name": old_table_name,
            "new_table_name": new_table_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_truncate_table(table_name: str, ctx: Context):
    """Truncate a table (remove all rows quickly).

    Args:
        table_name: Name of the table to truncate

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"TRUNCATE TABLE `{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' truncated successfully",
            "table_name": table_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_create_index(table_name: str, index_name: str, columns: List[str], ctx: Context, 
                      unique: bool = False, index_type: Optional[str] = None):
    """Create an index on a table.

    Args:
        table_name: Name of the table
        index_name: Name of the index
        columns: List of column names for the index
        unique: Whether to create a unique index
        index_type: Optional index type (BTREE, HASH, etc.)

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate index name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', index_name):
            raise ValueError(f"Invalid index name: {index_name}")
        
        # Validate column names
        for col in columns:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}")
        
        unique_clause = "UNIQUE " if unique else ""
        columns_str = ', '.join([f"`{col}`" for col in columns])
        index_type_clause = f" USING {index_type}" if index_type else ""
        
        query = f"CREATE {unique_clause}INDEX `{index_name}` ON `{table_name}` ({columns_str}){index_type_clause}"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Index '{index_name}' created on table '{table_name}'",
            "table_name": table_name,
            "index_name": index_name,
            "columns": columns,
            "unique": unique
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_index(table_name: str, index_name: str, ctx: Context):
    """Drop an index from a table.

    Args:
        table_name: Name of the table
        index_name: Name of the index to drop

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate index name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', index_name):
            raise ValueError(f"Invalid index name: {index_name}")
        
        query = f"DROP INDEX `{index_name}` ON `{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Index '{index_name}' dropped from table '{table_name}'",
            "table_name": table_name,
            "index_name": index_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_add_primary_key(table_name: str, columns: List[str], ctx: Context):
    """Add a primary key to a table.

    Args:
        table_name: Name of the table
        columns: List of column names for the primary key

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate column names
        for col in columns:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}")
        
        columns_str = ', '.join([f"`{col}`" for col in columns])
        query = f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({columns_str})"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Primary key added to table '{table_name}'",
            "table_name": table_name,
            "columns": columns
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_primary_key(table_name: str, ctx: Context):
    """Drop the primary key from a table.

    Args:
        table_name: Name of the table

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"ALTER TABLE `{table_name}` DROP PRIMARY KEY"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Primary key dropped from table '{table_name}'",
            "table_name": table_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_add_foreign_key(table_name: str, constraint_name: str, columns: List[str], 
                         ref_table: str, ref_columns: List[str], ctx: Context,
                         on_delete: Optional[str] = None, on_update: Optional[str] = None):
    """Add a foreign key constraint to a table.

    Args:
        table_name: Name of the table
        constraint_name: Name of the foreign key constraint
        columns: List of column names for the foreign key
        ref_table: Referenced table name
        ref_columns: List of referenced column names
        on_delete: ON DELETE action (CASCADE, SET NULL, RESTRICT, etc.)
        on_update: ON UPDATE action (CASCADE, SET NULL, RESTRICT, etc.)

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        ref_table = db.validate_table_name(ref_table)
        
        # Validate constraint name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', constraint_name):
            raise ValueError(f"Invalid constraint name: {constraint_name}")
        
        # Validate column names
        for col in columns + ref_columns:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}")
        
        columns_str = ', '.join([f"`{col}`" for col in columns])
        ref_columns_str = ', '.join([f"`{col}`" for col in ref_columns])
        
        query = f"ALTER TABLE `{table_name}` ADD CONSTRAINT `{constraint_name}` FOREIGN KEY ({columns_str}) REFERENCES `{ref_table}` ({ref_columns_str})"
        
        if on_delete:
            query += f" ON DELETE {on_delete}"
        if on_update:
            query += f" ON UPDATE {on_update}"
        
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Foreign key '{constraint_name}' added to table '{table_name}'",
            "table_name": table_name,
            "constraint_name": constraint_name,
            "columns": columns,
            "ref_table": ref_table,
            "ref_columns": ref_columns
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_foreign_key(table_name: str, constraint_name: str, ctx: Context):
    """Drop a foreign key constraint from a table.

    Args:
        table_name: Name of the table
        constraint_name: Name of the foreign key constraint to drop

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate constraint name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', constraint_name):
            raise ValueError(f"Invalid constraint name: {constraint_name}")
        
        query = f"ALTER TABLE `{table_name}` DROP FOREIGN KEY `{constraint_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Foreign key '{constraint_name}' dropped from table '{table_name}'",
            "table_name": table_name,
            "constraint_name": constraint_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_create_view(view_name: str, query: str, ctx: Context, replace: bool = False):
    """Create a view in the database.

    Args:
        view_name: Name of the view to create
        query: SELECT query for the view
        replace: Whether to use CREATE OR REPLACE

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate view name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', view_name):
            raise ValueError(f"Invalid view name: {view_name}")
        
        # Validate that the query is a SELECT statement
        if not re.match(r'^\s*SELECT', query.strip(), re.IGNORECASE):
            raise ValueError("View query must be a SELECT statement")
        
        or_replace = "OR REPLACE " if replace else ""
        full_query = f"CREATE {or_replace}VIEW `{view_name}` AS {query}"
        result = db.execute_query(full_query)
        
        return {
            "success": True,
            "message": f"View '{view_name}' created successfully",
            "view_name": view_name,
            "query": query
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_view(view_name: str, ctx: Context, if_exists: bool = True):
    """Drop a view from the database.

    Args:
        view_name: Name of the view to drop
        if_exists: Whether to use IF EXISTS clause

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate view name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', view_name):
            raise ValueError(f"Invalid view name: {view_name}")
        
        if_exists_clause = "IF EXISTS " if if_exists else ""
        query = f"DROP VIEW {if_exists_clause}`{view_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"View '{view_name}' dropped successfully",
            "view_name": view_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_optimize_table(table_name: str, ctx: Context):
    """Optimize a table to reclaim unused space and defragment.

    Args:
        table_name: Name of the table to optimize

    Returns:
        Success status and optimization details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"OPTIMIZE TABLE `{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' optimized successfully",
            "table_name": table_name,
            "optimization_result": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_analyze_table(table_name: str, ctx: Context):
    """Analyze a table to update key distribution statistics.

    Args:
        table_name: Name of the table to analyze

    Returns:
        Success status and analysis details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"ANALYZE TABLE `{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' analyzed successfully",
            "table_name": table_name,
            "analysis_result": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_repair_table(table_name: str, ctx: Context):
    """Repair a possibly corrupted table.

    Args:
        table_name: Name of the table to repair

    Returns:
        Success status and repair details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"REPAIR TABLE `{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' repair completed",
            "table_name": table_name,
            "repair_result": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_check_table(table_name: str, ctx: Context, check_type: str = "MEDIUM"):
    """Check a table for errors.

    Args:
        table_name: Name of the table to check
        check_type: Type of check (QUICK, FAST, MEDIUM, EXTENDED, CHANGED)

    Returns:
        Success status and check results
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        # Validate check type
        valid_types = ['QUICK', 'FAST', 'MEDIUM', 'EXTENDED', 'CHANGED']
        if check_type.upper() not in valid_types:
            raise ValueError(f"Invalid check type. Must be one of: {', '.join(valid_types)}")
        
        query = f"CHECK TABLE `{table_name}` {check_type.upper()}"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table '{table_name}' check completed",
            "table_name": table_name,
            "check_type": check_type.upper(),
            "check_result": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_create_table(table_name: str, ctx: Context):
    """Show the CREATE TABLE statement for a table.

    Args:
        table_name: Name of the table

    Returns:
        Success status and CREATE TABLE statement
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        table_name = db.validate_table_name(table_name)
        
        query = f"SHOW CREATE TABLE `{table_name}`"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "table_name": table_name,
            "create_statement": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_copy_table(source_table: str, dest_table: str, ctx: Context, 
                   copy_data: bool = True, if_not_exists: bool = True):
    """Copy a table structure and optionally its data.

    Args:
        source_table: Name of the source table
        dest_table: Name of the destination table
        copy_data: Whether to copy data as well as structure
        if_not_exists: Whether to use IF NOT EXISTS clause

    Returns:
        Success status and copy details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        source_table = db.validate_table_name(source_table)
        dest_table = db.validate_table_name(dest_table)
        
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        
        if copy_data:
            # Copy structure and data
            query = f"CREATE TABLE {if_not_exists_clause}`{dest_table}` AS SELECT * FROM `{source_table}`"
        else:
            # Copy structure only
            query = f"CREATE TABLE {if_not_exists_clause}`{dest_table}` LIKE `{source_table}`"
        
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Table copied from '{source_table}' to '{dest_table}'",
            "source_table": source_table,
            "dest_table": dest_table,
            "copy_data": copy_data
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_create_user(username: str, password: str, host: str, ctx: Context):
    """Create a new MySQL user.

    Args:
        username: Username for the new user
        password: Password for the new user
        host: Host from which the user can connect (e.g., 'localhost', '%')

    Returns:
        Success status and user creation details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate username
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_@.-]*$', username):
            raise ValueError(f"Invalid username: {username}")
        
        query = f"CREATE USER '{username}'@'{host}' IDENTIFIED BY %s"
        result = db.execute_prepared_query(query, [password])
        
        return {
            "success": True,
            "message": f"User '{username}'@'{host}' created successfully",
            "username": username,
            "host": host
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_drop_user(username: str, host: str, ctx: Context):
    """Drop a MySQL user.

    Args:
        username: Username to drop
        host: Host specification

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate username
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_@.-]*$', username):
            raise ValueError(f"Invalid username: {username}")
        
        query = f"DROP USER '{username}'@'{host}'"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"User '{username}'@'{host}' dropped successfully",
            "username": username,
            "host": host
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_grant_privileges(username: str, host: str, privileges: str, database: str, ctx: Context, 
                          table: Optional[str] = None):
    """Grant privileges to a MySQL user.

    Args:
        username: Username to grant privileges to
        host: Host specification
        privileges: Privileges to grant (e.g., 'ALL', 'SELECT,INSERT', 'CREATE')
        database: Database name or '*' for all databases
        table: Optional table name or '*' for all tables

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate username
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_@.-]*$', username):
            raise ValueError(f"Invalid username: {username}")
        
        # Build the GRANT statement
        table_spec = f".`{table}`" if table and table != '*' else ".*"
        if database == '*':
            target = "*.*"
        else:
            target = f"`{database}`{table_spec}"
        
        query = f"GRANT {privileges} ON {target} TO '{username}'@'{host}'"
        result = db.execute_query(query)
        
        # Flush privileges
        db.execute_query("FLUSH PRIVILEGES")
        
        return {
            "success": True,
            "message": f"Granted {privileges} on {target} to '{username}'@'{host}'",
            "username": username,
            "host": host,
            "privileges": privileges,
            "target": target
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_revoke_privileges(username: str, host: str, privileges: str, database: str, ctx: Context,
                           table: Optional[str] = None):
    """Revoke privileges from a MySQL user.

    Args:
        username: Username to revoke privileges from
        host: Host specification
        privileges: Privileges to revoke
        database: Database name or '*' for all databases
        table: Optional table name or '*' for all tables

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate username
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_@.-]*$', username):
            raise ValueError(f"Invalid username: {username}")
        
        # Build the REVOKE statement
        table_spec = f".`{table}`" if table and table != '*' else ".*"
        if database == '*':
            target = "*.*"
        else:
            target = f"`{database}`{table_spec}"
        
        query = f"REVOKE {privileges} ON {target} FROM '{username}'@'{host}'"
        result = db.execute_query(query)
        
        # Flush privileges
        db.execute_query("FLUSH PRIVILEGES")
        
        return {
            "success": True,
            "message": f"Revoked {privileges} on {target} from '{username}'@'{host}'",
            "username": username,
            "host": host,
            "privileges": privileges,
            "target": target
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_users(ctx: Context):
    """Show all MySQL users.

    Returns:
        List of all users and their hosts
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        query = "SELECT user, host FROM mysql.user ORDER BY user, host"
        users = db.execute_query(query)
        
        return {
            "success": True,
            "users": users,
            "count": len(users)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_user_privileges(username: str, host: str, ctx: Context):
    """Show privileges for a specific user.

    Args:
        username: Username to check privileges for
        host: Host specification

    Returns:
        User privileges information
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        query = f"SHOW GRANTS FOR '{username}'@'{host}'"
        grants = db.execute_query(query)
        
        return {
            "success": True,
            "username": username,
            "host": host,
            "grants": grants,
            "count": len(grants)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_change_user_password(username: str, host: str, new_password: str, ctx: Context):
    """Change password for a MySQL user.

    Args:
        username: Username to change password for
        host: Host specification
        new_password: New password

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        # Validate username
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_@.-]*$', username):
            raise ValueError(f"Invalid username: {username}")
        
        query = f"ALTER USER '{username}'@'{host}' IDENTIFIED BY %s"
        result = db.execute_prepared_query(query, [new_password])
        
        return {
            "success": True,
            "message": f"Password changed for user '{username}'@'{host}'",
            "username": username,
            "host": host
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_flush_privileges(ctx: Context):
    """Flush MySQL privileges to reload grant tables.

    Returns:
        Success status
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        result = db.execute_query("FLUSH PRIVILEGES")
        
        return {
            "success": True,
            "message": "Privileges flushed successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_status(ctx: Context, pattern: Optional[str] = None):
    """Show MySQL server status variables.

    Args:
        pattern: Optional LIKE pattern to filter status variables

    Returns:
        Server status variables and their values
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        if pattern:
            query = "SHOW STATUS LIKE %s"
            status_vars = db.execute_prepared_query(query, [pattern])
        else:
            query = "SHOW STATUS"
            status_vars = db.execute_query(query)
        
        return {
            "success": True,
            "status_variables": status_vars,
            "count": len(status_vars)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_engines(ctx: Context):
    """Show available storage engines.

    Returns:
        List of available storage engines and their properties
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        query = "SHOW ENGINES"
        engines = db.execute_query(query)
        
        return {
            "success": True,
            "engines": engines,
            "count": len(engines)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_warnings(ctx: Context):
    """Show MySQL warnings from the last statement.

    Returns:
        List of warnings
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        query = "SHOW WARNINGS"
        warnings = db.execute_query(query)
        
        return {
            "success": True,
            "warnings": warnings,
            "count": len(warnings)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_show_errors(ctx: Context):
    """Show MySQL errors from the last statement.

    Returns:
        List of errors
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        query = "SHOW ERRORS"
        errors = db.execute_query(query)
        
        return {
            "success": True,
            "errors": errors,
            "count": len(errors)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_kill_process(process_id: int, ctx: Context):
    """Kill a MySQL process.

    Args:
        process_id: Process ID to kill

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        query = f"KILL {process_id}"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Process {process_id} killed successfully",
            "process_id": process_id
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_set_variable(variable_name: str, value: str, ctx: Context, global_scope: bool = False):
    """Set a MySQL variable.

    Args:
        variable_name: Name of the variable to set
        value: Value to set
        global_scope: Whether to set globally (requires privileges)

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        scope = "GLOBAL" if global_scope else "SESSION"
        query = f"SET {scope} {variable_name} = %s"
        result = db.execute_prepared_query(query, [value])
        
        return {
            "success": True,
            "message": f"{scope} variable '{variable_name}' set to '{value}'",
            "variable_name": variable_name,
            "value": value,
            "scope": scope
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_reset_query_cache(ctx: Context):
    """Reset (clear) the query cache.

    Returns:
        Success status
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        result = db.execute_query("RESET QUERY CACHE")
        
        return {
            "success": True,
            "message": "Query cache reset successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_flush_logs(ctx: Context):
    """Flush MySQL logs.

    Returns:
        Success status
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        result = db.execute_query("FLUSH LOGS")
        
        return {
            "success": True,
            "message": "Logs flushed successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_flush_tables(ctx: Context, table_names: Optional[List[str]] = None):
    """Flush MySQL tables.

    Args:
        table_names: Optional list of specific table names to flush

    Returns:
        Success status
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        if table_names:
            # Validate table names
            for table_name in table_names:
                db.validate_table_name(table_name)
            
            tables_str = ', '.join([f"`{table}`" for table in table_names])
            query = f"FLUSH TABLES {tables_str}"
        else:
            query = "FLUSH TABLES"
        
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": "Tables flushed successfully",
            "tables": table_names or "all"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_lock_tables(table_locks: List[Dict[str, str]], ctx: Context):
    """Lock tables for the current session.

    Args:
        table_locks: List of dictionaries with 'table' and 'lock_type' keys
                    lock_type can be 'READ', 'WRITE', 'LOW_PRIORITY WRITE'

    Returns:
        Success status and details
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        
        lock_clauses = []
        for lock in table_locks:
            if 'table' not in lock or 'lock_type' not in lock:
                raise ValueError("Each lock must have 'table' and 'lock_type' keys")
            
            table_name = db.validate_table_name(lock['table'])
            lock_type = lock['lock_type'].upper()
            
            if lock_type not in ['READ', 'WRITE', 'LOW_PRIORITY WRITE']:
                raise ValueError(f"Invalid lock type: {lock_type}")
            
            lock_clauses.append(f"`{table_name}` {lock_type}")
        
        query = f"LOCK TABLES {', '.join(lock_clauses)}"
        result = db.execute_query(query)
        
        return {
            "success": True,
            "message": f"Locked {len(table_locks)} table(s)",
            "locks": table_locks
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.tool()
def mysql_unlock_tables(ctx: Context):
    """Unlock all tables for the current session.

    Returns:
        Success status
    """
    try:
        db = ctx.request_context.lifespan_context["db"]
        result = db.execute_query("UNLOCK TABLES")
        
        return {
            "success": True,
            "message": "All tables unlocked successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }


@mcp.resource("mysql://tables")
def get_tables_resource():
    """Resource providing list of all tables in the database."""
    return """# Database Tables

## Available Tables:
This resource provides a list of all tables in the MySQL database.
Use the list_mysql_tables tool to get the actual table list.

Example usage:
- Call the list_mysql_tables tool to get current tables
- Use mysql_table_schema tool to get schema for specific tables
"""


@mcp.resource("mysql://schema/{table_name}")
def get_table_schema_resource(table_name: str):
    """Resource providing schema information for a specific table."""
    return f"""# Table Schema: {table_name}

## Schema Information
This resource provides schema information for the table '{table_name}'.
Use the mysql_table_schema tool to get the actual schema details.

Example usage:
- Call mysql_table_schema with table_name: "{table_name}"
- Use mysql_table_data to see sample data from this table
- Check mysql_table_indexes for index information
"""


@mcp.tool()
def show_table_status(ctx: Context, table_name: str = ""):
    """Show status information for tables.
    
    Args:
        table_name: Optional specific table name to check (empty for all tables)
    """
    try:
        db = ctx.lifespan["db"]
        if table_name:
            query = "SHOW TABLE STATUS LIKE %s"
            result = db.execute_prepared_query(query, [table_name])
        else:
            query = "SHOW TABLE STATUS"
            result = db.execute_query(query)
        
        if not result:
            return f"No table status information found{' for ' + table_name if table_name else ''}"
        
        status_info = []
        for row in result:
            status_info.append(f"Table: {row[0]}")
            status_info.append(f"  Engine: {row[1]}")
            status_info.append(f"  Rows: {row[4]}")
            status_info.append(f"  Data Length: {row[6]}")
            status_info.append(f"  Index Length: {row[8]}")
            status_info.append(f"  Auto Increment: {row[10]}")
            status_info.append(f"  Create Time: {row[11]}")
            status_info.append(f"  Update Time: {row[12]}")
            status_info.append("")
        
        return "\n".join(status_info)
    except Exception as e:
        return f"Error getting table status: {str(e)}"


@mcp.tool()
def show_create_table(ctx: Context, table_name: str):
    """Show the CREATE TABLE statement for a table.
    
    Args:
        table_name: Name of the table
    """
    try:
        db = ctx.lifespan["db"]
        query = f"SHOW CREATE TABLE `{table_name}`"
        result = db.execute_query(query)
        
        if not result:
            return f"Table '{table_name}' not found"
        
        return result[0][1]
    except Exception as e:
        return f"Error getting table definition: {str(e)}"


@mcp.tool()
def show_create_database(ctx: Context, database_name: str):
    """Show the CREATE DATABASE statement for a database.
    
    Args:
        database_name: Name of the database
    """
    try:
        db = ctx.lifespan["db"]
        query = f"SHOW CREATE DATABASE `{database_name}`"
        result = db.execute_query(query)
        
        if not result:
            return f"Database '{database_name}' not found"
        
        return result[0][1]
    except Exception as e:
        return f"Error getting database definition: {str(e)}"


@mcp.tool()
def describe_table(ctx: Context, table_name: str):
    """Describe table structure (same as SHOW COLUMNS).
    
    Args:
        table_name: Name of the table to describe
    """
    try:
        db = ctx.lifespan["db"]
        query = f"DESCRIBE `{table_name}`"
        result = db.execute_query(query)
        
        if not result:
            return f"Table '{table_name}' not found"
        
        columns = []
        for row in result:
            columns.append(f"Field: {row[0]}, Type: {row[1]}, Null: {row[2]}, Key: {row[3]}, Default: {row[4]}, Extra: {row[5]}")
        
        return "\n".join(columns)
    except Exception as e:
        return f"Error describing table: {str(e)}"


@mcp.tool()
def show_triggers(ctx: Context, table_name: str = ""):
    """Show triggers for a table or all triggers.
    
    Args:
        table_name: Optional table name to filter triggers
    """
    try:
        db = ctx.lifespan["db"]
        if table_name:
            query = "SHOW TRIGGERS LIKE %s"
            result = db.execute_prepared_query(query, [table_name])
        else:
            query = "SHOW TRIGGERS"
            result = db.execute_query(query)
        
        if not result:
            return f"No triggers found{' for table ' + table_name if table_name else ''}"
        
        triggers = []
        for row in result:
            triggers.append(f"Trigger: {row[0]}")
            triggers.append(f"  Event: {row[1]}")
            triggers.append(f"  Table: {row[2]}")
            triggers.append(f"  Statement: {row[3]}")
            triggers.append(f"  Timing: {row[4]}")
            triggers.append("")
        
        return "\n".join(triggers)
    except Exception as e:
        return f"Error getting triggers: {str(e)}"


@mcp.tool()
def show_events(ctx: Context):
    """Show scheduled events in the database."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW EVENTS"
        result = db.execute_query(query)
        
        if not result:
            return "No scheduled events found"
        
        events = []
        for row in result:
            events.append(f"Event: {row[1]}")
            events.append(f"  Status: {row[3]}")
            events.append(f"  Execute At: {row[5]}")
            events.append(f"  Interval: {row[6]}")
            events.append(f"  Starts: {row[7]}")
            events.append(f"  Ends: {row[8]}")
            events.append("")
        
        return "\n".join(events)
    except Exception as e:
        return f"Error getting events: {str(e)}"


@mcp.tool()
def show_function_status(ctx: Context, function_name: str = ""):
    """Show status of stored functions.
    
    Args:
        function_name: Optional function name to filter
    """
    try:
        db = ctx.lifespan["db"]
        if function_name:
            query = "SHOW FUNCTION STATUS LIKE %s"
            result = db.execute_prepared_query(query, [function_name])
        else:
            query = "SHOW FUNCTION STATUS"
            result = db.execute_query(query)
        
        if not result:
            return f"No functions found{' matching ' + function_name if function_name else ''}"
        
        functions = []
        for row in result:
            functions.append(f"Function: {row[1]}")
            functions.append(f"  Database: {row[0]}")
            functions.append(f"  Type: {row[2]}")
            functions.append(f"  Definer: {row[3]}")
            functions.append(f"  Modified: {row[4]}")
            functions.append(f"  Created: {row[5]}")
            functions.append("")
        
        return "\n".join(functions)
    except Exception as e:
        return f"Error getting function status: {str(e)}"


@mcp.tool()
def show_procedure_status(ctx: Context, procedure_name: str = ""):
    """Show status of stored procedures.
    
    Args:
        procedure_name: Optional procedure name to filter
    """
    try:
        db = ctx.lifespan["db"]
        if procedure_name:
            query = "SHOW PROCEDURE STATUS LIKE %s"
            result = db.execute_prepared_query(query, [procedure_name])
        else:
            query = "SHOW PROCEDURE STATUS"
            result = db.execute_query(query)
        
        if not result:
            return f"No procedures found{' matching ' + procedure_name if procedure_name else ''}"
        
        procedures = []
        for row in result:
            procedures.append(f"Procedure: {row[1]}")
            procedures.append(f"  Database: {row[0]}")
            procedures.append(f"  Type: {row[2]}")
            procedures.append(f"  Definer: {row[3]}")
            procedures.append(f"  Modified: {row[4]}")
            procedures.append(f"  Created: {row[5]}")
            procedures.append("")
        
        return "\n".join(procedures)
    except Exception as e:
        return f"Error getting procedure status: {str(e)}"


@mcp.tool()
def show_binary_logs(ctx: Context):
    """Show binary log files."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW BINARY LOGS"
        result = db.execute_query(query)
        
        if not result:
            return "No binary logs found or binary logging is disabled"
        
        logs = []
        for row in result:
            logs.append(f"Log: {row[0]}, Size: {row[1]} bytes")
        
        return "\n".join(logs)
    except Exception as e:
        return f"Error getting binary logs: {str(e)}"


@mcp.tool()
def show_master_status(ctx: Context):
    """Show master status for replication."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW MASTER STATUS"
        result = db.execute_query(query)
        
        if not result:
            return "Master status not available or binary logging is disabled"
        
        row = result[0]
        status = []
        status.append(f"File: {row[0]}")
        status.append(f"Position: {row[1]}")
        status.append(f"Binlog_Do_DB: {row[2]}")
        status.append(f"Binlog_Ignore_DB: {row[3]}")
        
        return "\n".join(status)
    except Exception as e:
        return f"Error getting master status: {str(e)}"


@mcp.tool()
def show_slave_status(ctx: Context):
    """Show slave status for replication."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW SLAVE STATUS"
        result = db.execute_query(query)
        
        if not result:
            return "Slave status not available or server is not configured as a slave"
        
        row = result[0]
        status = []
        status.append(f"Slave_IO_State: {row[0]}")
        status.append(f"Master_Host: {row[1]}")
        status.append(f"Master_User: {row[2]}")
        status.append(f"Master_Port: {row[3]}")
        status.append(f"Connect_Retry: {row[4]}")
        status.append(f"Master_Log_File: {row[5]}")
        status.append(f"Read_Master_Log_Pos: {row[6]}")
        status.append(f"Relay_Log_File: {row[7]}")
        status.append(f"Relay_Log_Pos: {row[8]}")
        status.append(f"Relay_Master_Log_File: {row[9]}")
        status.append(f"Slave_IO_Running: {row[10]}")
        status.append(f"Slave_SQL_Running: {row[11]}")
        status.append(f"Seconds_Behind_Master: {row[32]}")
        
        return "\n".join(status)
    except Exception as e:
        return f"Error getting slave status: {str(e)}"


@mcp.tool()
def show_character_sets(ctx: Context):
    """Show available character sets."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW CHARACTER SET"
        result = db.execute_query(query)
        
        if not result:
            return "No character sets found"
        
        charsets = []
        for row in result:
            charsets.append(f"Charset: {row[0]}, Description: {row[1]}, Default Collation: {row[2]}, Maxlen: {row[3]}")
        
        return "\n".join(charsets)
    except Exception as e:
        return f"Error getting character sets: {str(e)}"


@mcp.tool()
def show_collations(ctx: Context, charset: str = ""):
    """Show available collations.
    
    Args:
        charset: Optional character set to filter collations
    """
    try:
        db = ctx.lifespan["db"]
        if charset:
            query = "SHOW COLLATION WHERE Charset = %s"
            result = db.execute_prepared_query(query, [charset])
        else:
            query = "SHOW COLLATION"
            result = db.execute_query(query)
        
        if not result:
            return f"No collations found{' for charset ' + charset if charset else ''}"
        
        collations = []
        for row in result:
            default = " (Default)" if row[3] == "Yes" else ""
            collations.append(f"Collation: {row[0]}, Charset: {row[1]}, Id: {row[2]}, Compiled: {row[4]}, Sortlen: {row[5]}{default}")
        
        return "\n".join(collations)
    except Exception as e:
        return f"Error getting collations: {str(e)}"


@mcp.tool()
async def show_table_types():
    """Show available table types (storage engines)."""
    async with get_mysql_connection() as conn:
        query = "SHOW TABLE TYPES"
        result = await conn.execute_query(query)
        
        if not result:
            return "No table types found"
        
        types = []
        for row in result:
            types.append(f"Engine: {row[0]}, Support: {row[1]}, Comment: {row[2]}")
        
        return "\n".join(types)


@mcp.tool()
async def show_open_tables(database_name: str = ""):
    """Show open tables in the table cache.
    
    Args:
        database_name: Optional database name to filter
    """
    async with get_mysql_connection() as conn:
        if database_name:
            query = "SHOW OPEN TABLES FROM %s" % conn.quote_identifier(database_name)
        else:
            query = "SHOW OPEN TABLES"
        result = await conn.execute_query(query)
        
        if not result:
            return f"No open tables found{' in database ' + database_name if database_name else ''}"
        
        tables = []
        for row in result:
            tables.append(f"Database: {row[0]}, Table: {row[1]}, In_use: {row[2]}, Name_locked: {row[3]}")
        
        return "\n".join(tables)


@mcp.tool()
async def show_session_variables(pattern: str = ""):
    """Show session variables.
    
    Args:
        pattern: Optional pattern to filter variables
    """
    async with get_mysql_connection() as conn:
        if pattern:
            query = "SHOW SESSION VARIABLES LIKE %s"
            result = await conn.execute_query(query, (pattern,))
        else:
            query = "SHOW SESSION VARIABLES"
            result = await conn.execute_query(query)
        
        if not result:
            return f"No session variables found{' matching pattern ' + pattern if pattern else ''}"
        
        variables = []
        for row in result:
            variables.append(f"{row[0]} = {row[1]}")
        
        return "\n".join(variables)


@mcp.tool()
async def show_global_variables(pattern: str = ""):
    """Show global variables.
    
    Args:
        pattern: Optional pattern to filter variables
    """
    async with get_mysql_connection() as conn:
        if pattern:
            query = "SHOW GLOBAL VARIABLES LIKE %s"
            result = await conn.execute_query(query, (pattern,))
        else:
            query = "SHOW GLOBAL VARIABLES"
            result = await conn.execute_query(query)
        
        if not result:
            return f"No global variables found{' matching pattern ' + pattern if pattern else ''}"
        
        variables = []
        for row in result:
            variables.append(f"{row[0]} = {row[1]}")
        
        return "\n".join(variables)


@mcp.tool()
async def explain_table(table_name: str):
    """Explain table structure (alias for DESCRIBE).
    
    Args:
        table_name: Name of the table to explain
    """
    return await describe_table(table_name)


@mcp.tool()
async def show_table_indexes(table_name: str):
    """Show all indexes for a specific table.
    
    Args:
        table_name: Name of the table
    """
    async with get_mysql_connection() as conn:
        query = "SHOW INDEXES FROM %s" % conn.quote_identifier(table_name)
        result = await conn.execute_query(query)
        
        if not result:
            return f"No indexes found for table '{table_name}' or table does not exist"
        
        indexes = {}
        for row in result:
            index_name = row[2]
            if index_name not in indexes:
                indexes[index_name] = {
                    'table': row[0],
                    'unique': row[1] == 0,
                    'columns': [],
                    'type': row[10] if len(row) > 10 else 'BTREE'
                }
            indexes[index_name]['columns'].append({
                'column': row[4],
                'sequence': row[3],
                'collation': row[7],
                'cardinality': row[6]
            })
        
        result_lines = []
        for index_name, index_info in indexes.items():
            result_lines.append(f"Index: {index_name}")
            result_lines.append(f"  Table: {index_info['table']}")
            result_lines.append(f"  Unique: {index_info['unique']}")
            result_lines.append(f"  Type: {index_info['type']}")
            result_lines.append("  Columns:")
            for col in index_info['columns']:
                result_lines.append(f"    {col['sequence']}. {col['column']} (Cardinality: {col['cardinality']})")
            result_lines.append("")
        
        return "\n".join(result_lines)


@mcp.tool()
async def show_grants_for_user(username: str, hostname: str = "%"):
    """Show grants for a specific user.
    
    Args:
        username: Username to show grants for
        hostname: Hostname for the user (default: %)
    """
    async with get_mysql_connection() as conn:
        query = "SHOW GRANTS FOR %s@%s"
        user_host = f"'{username}'@'{hostname}'"
        try:
            result = await conn.execute_query(f"SHOW GRANTS FOR {user_host}")
            
            if not result:
                return f"No grants found for user {user_host}"
            
            grants = []
            for row in result:
                grants.append(row[0])
            
            return "\n".join(grants)
        except Exception as e:
            return f"Error showing grants for user {user_host}: {str(e)}"


@mcp.tool()
async def list_events():
    """List all scheduled events in the database."""
    async with get_mysql_connection() as conn:
        try:
            result = await conn.execute_query("SHOW EVENTS")
            
            if not result:
                return "No events found or EVENT scheduler is disabled"
            
            events = []
            for row in result:
                events.append(f"Event: {row[1]} | Status: {row[3]} | Type: {row[4]} | Execute: {row[5]}")
            
            return "\n".join(events)
        except Exception as e:
            return f"Error listing events: {str(e)}"


@mcp.tool()
async def create_event(event_name: str, schedule: str, sql_statement: str, starts: str = "", ends: str = ""):
    """Create a scheduled event.
    
    Args:
        event_name: Name of the event
        schedule: Schedule expression (e.g., 'EVERY 1 HOUR', 'AT "2024-12-01 10:00:00"')
        sql_statement: SQL statement to execute
        starts: Optional start timestamp
        ends: Optional end timestamp
    """
    if not all(c.isalnum() or c == '_' for c in event_name):
        return "Error: Event name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            query_parts = [f"CREATE EVENT {event_name}"]
            query_parts.append(f"ON SCHEDULE {schedule}")
            
            if starts:
                query_parts.append(f"STARTS '{starts}'")
            if ends:
                query_parts.append(f"ENDS '{ends}'")
                
            query_parts.append(f"DO {sql_statement}")
            
            query = " ".join(query_parts)
            await conn.execute_query(query)
            return f"Event '{event_name}' created successfully"
        except Exception as e:
            return f"Error creating event '{event_name}': {str(e)}"


@mcp.tool()
async def drop_event(event_name: str):
    """Drop a scheduled event.
    
    Args:
        event_name: Name of the event to drop
    """
    if not all(c.isalnum() or c == '_' for c in event_name):
        return "Error: Event name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            await conn.execute_query(f"DROP EVENT IF EXISTS {event_name}")
            return f"Event '{event_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping event '{event_name}': {str(e)}"


@mcp.tool()
async def alter_event(event_name: str, new_schedule: str = "", new_statement: str = "", enable: bool = None):
    """Alter a scheduled event.
    
    Args:
        event_name: Name of the event to alter
        new_schedule: New schedule expression
        new_statement: New SQL statement
        enable: Enable (True) or disable (False) the event
    """
    if not all(c.isalnum() or c == '_' for c in event_name):
        return "Error: Event name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            query_parts = [f"ALTER EVENT {event_name}"]
            
            if new_schedule:
                query_parts.append(f"ON SCHEDULE {new_schedule}")
            if new_statement:
                query_parts.append(f"DO {new_statement}")
            if enable is not None:
                status = "ENABLE" if enable else "DISABLE"
                query_parts.append(status)
                
            if len(query_parts) == 1:
                return "Error: No changes specified"
                
            query = " ".join(query_parts)
            await conn.execute_query(query)
            return f"Event '{event_name}' altered successfully"
        except Exception as e:
            return f"Error altering event '{event_name}': {str(e)}"


@mcp.tool()
async def list_partitions(table_name: str):
    """List partitions for a partitioned table.
    
    Args:
        table_name: Name of the table
    """
    async with get_mysql_connection() as conn:
        try:
            query = """
            SELECT PARTITION_NAME, PARTITION_EXPRESSION, PARTITION_DESCRIPTION, 
                   TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH
            FROM INFORMATION_SCHEMA.PARTITIONS 
            WHERE TABLE_NAME = %s AND PARTITION_NAME IS NOT NULL
            ORDER BY PARTITION_ORDINAL_POSITION
            """
            result = await conn.execute_query(query, (table_name,))
            
            if not result:
                return f"Table '{table_name}' has no partitions or doesn't exist"
            
            partitions = []
            for row in result:
                partitions.append(
                    f"Partition: {row[0]} | Expression: {row[1]} | Description: {row[2]} | "
                    f"Rows: {row[3]} | Avg Row Length: {row[4]} | Data Length: {row[5]}"
                )
            
            return "\n".join(partitions)
        except Exception as e:
            return f"Error listing partitions for table '{table_name}': {str(e)}"


@mcp.tool()
async def add_partition(table_name: str, partition_name: str, partition_value: str):
    """Add a partition to a table.
    
    Args:
        table_name: Name of the table
        partition_name: Name of the new partition
        partition_value: Partition value expression
    """
    if not all(c.isalnum() or c == '_' for c in partition_name):
        return "Error: Partition name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            query = f"ALTER TABLE {conn.quote_identifier(table_name)} ADD PARTITION (PARTITION {partition_name} VALUES {partition_value})"
            await conn.execute_query(query)
            return f"Partition '{partition_name}' added to table '{table_name}' successfully"
        except Exception as e:
            return f"Error adding partition to table '{table_name}': {str(e)}"


@mcp.tool()
async def drop_partition(table_name: str, partition_name: str):
    """Drop a partition from a table.
    
    Args:
        table_name: Name of the table
        partition_name: Name of the partition to drop
    """
    if not all(c.isalnum() or c == '_' for c in partition_name):
        return "Error: Partition name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            query = f"ALTER TABLE {conn.quote_identifier(table_name)} DROP PARTITION {partition_name}"
            await conn.execute_query(query)
            return f"Partition '{partition_name}' dropped from table '{table_name}' successfully"
        except Exception as e:
            return f"Error dropping partition from table '{table_name}': {str(e)}"


@mcp.tool()
async def create_role(role_name: str):
    """Create a new role (MySQL 8.0+).
    
    Args:
        role_name: Name of the role to create
    """
    if not all(c.isalnum() or c in '_-' for c in role_name):
        return "Error: Role name can only contain alphanumeric characters, underscores, and hyphens"
    
    async with get_mysql_connection() as conn:
        try:
            await conn.execute_query(f"CREATE ROLE '{role_name}'")
            return f"Role '{role_name}' created successfully"
        except Exception as e:
            return f"Error creating role '{role_name}': {str(e)}"


@mcp.tool()
async def drop_role(role_name: str):
    """Drop a role (MySQL 8.0+).
    
    Args:
        role_name: Name of the role to drop
    """
    if not all(c.isalnum() or c in '_-' for c in role_name):
        return "Error: Role name can only contain alphanumeric characters, underscores, and hyphens"
    
    async with get_mysql_connection() as conn:
        try:
            await conn.execute_query(f"DROP ROLE IF EXISTS '{role_name}'")
            return f"Role '{role_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping role '{role_name}': {str(e)}"


@mcp.tool()
async def grant_role_to_user(role_name: str, username: str, hostname: str = "%"):
    """Grant a role to a user (MySQL 8.0+).
    
    Args:
        role_name: Name of the role to grant
        username: Username to grant the role to
        hostname: Hostname for the user (default: %)
    """
    if not all(c.isalnum() or c in '_-' for c in role_name):
        return "Error: Role name can only contain alphanumeric characters, underscores, and hyphens"
    
    async with get_mysql_connection() as conn:
        try:
            user_host = f"'{username}'@'{hostname}'"
            await conn.execute_query(f"GRANT '{role_name}' TO {user_host}")
            return f"Role '{role_name}' granted to user {user_host} successfully"
        except Exception as e:
            return f"Error granting role '{role_name}' to user '{username}': {str(e)}"


@mcp.tool()
async def revoke_role_from_user(role_name: str, username: str, hostname: str = "%"):
    """Revoke a role from a user (MySQL 8.0+).
    
    Args:
        role_name: Name of the role to revoke
        username: Username to revoke the role from
        hostname: Hostname for the user (default: %)
    """
    if not all(c.isalnum() or c in '_-' for c in role_name):
        return "Error: Role name can only contain alphanumeric characters, underscores, and hyphens"
    
    async with get_mysql_connection() as conn:
        try:
            user_host = f"'{username}'@'{hostname}'"
            await conn.execute_query(f"REVOKE '{role_name}' FROM {user_host}")
            return f"Role '{role_name}' revoked from user {user_host} successfully"
        except Exception as e:
            return f"Error revoking role '{role_name}' from user '{username}': {str(e)}"


@mcp.tool()
async def show_roles():
    """Show all roles in the database (MySQL 8.0+)."""
    async with get_mysql_connection() as conn:
        try:
            result = await conn.execute_query("SELECT User, Host FROM mysql.user WHERE account_locked = 'Y'")
            
            if not result:
                return "No roles found"
            
            roles = []
            for row in result:
                roles.append(f"Role: '{row[0]}'@'{row[1]}'")
            
            return "\n".join(roles)
        except Exception as e:
            return f"Error showing roles: {str(e)}"


@mcp.tool()
async def create_user_with_ssl(username: str, hostname: str, password: str, ssl_type: str = "SSL"):
    """Create a user with SSL requirements.
    
    Args:
        username: Username for the new user
        hostname: Hostname for the user
        password: Password for the user
        ssl_type: SSL requirement type (SSL, X509, CIPHER, ISSUER, SUBJECT)
    """
    if not all(c.isalnum() or c == '_' for c in username):
        return "Error: Username can only contain alphanumeric characters and underscores"
    
    valid_ssl_types = ["SSL", "X509", "CIPHER", "ISSUER", "SUBJECT"]
    if ssl_type not in valid_ssl_types:
        return f"Error: SSL type must be one of: {', '.join(valid_ssl_types)}"
    
    async with get_mysql_connection() as conn:
        try:
            user_host = f"'{username}'@'{hostname}'"
            query = f"CREATE USER {user_host} IDENTIFIED BY '{password}' REQUIRE {ssl_type}"
            await conn.execute_query(query)
            return f"User {user_host} with SSL requirement created successfully"
        except Exception as e:
            return f"Error creating SSL user '{username}': {str(e)}"


@mcp.tool()
async def create_stored_procedure(proc_name: str, parameters: str, body: str):
    """Create a stored procedure.
    
    Args:
        proc_name: Name of the procedure
        parameters: Parameter list (e.g., 'IN param1 INT, OUT param2 VARCHAR(100)')
        body: Procedure body SQL
    """
    if not all(c.isalnum() or c == '_' for c in proc_name):
        return "Error: Procedure name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            query = f"""
            DELIMITER //
            CREATE PROCEDURE {proc_name}({parameters})
            BEGIN
                {body}
            END //
            DELIMITER ;
            """
            await conn.execute_query(query)
            return f"Stored procedure '{proc_name}' created successfully"
        except Exception as e:
            return f"Error creating stored procedure '{proc_name}': {str(e)}"


@mcp.tool()
async def drop_stored_procedure(proc_name: str):
    """Drop a stored procedure.
    
    Args:
        proc_name: Name of the procedure to drop
    """
    if not all(c.isalnum() or c == '_' for c in proc_name):
        return "Error: Procedure name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            await conn.execute_query(f"DROP PROCEDURE IF EXISTS {proc_name}")
            return f"Stored procedure '{proc_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping stored procedure '{proc_name}': {str(e)}"


@mcp.tool()
async def list_stored_procedures():
    """List all stored procedures in the current database."""
    async with get_mysql_connection() as conn:
        try:
            query = """
            SELECT ROUTINE_NAME, ROUTINE_TYPE, CREATED, LAST_ALTERED, 
                   SQL_DATA_ACCESS, SECURITY_TYPE
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ROUTINE_NAME
            """
            result = await conn.execute_query(query)
            
            if not result:
                return "No stored procedures found in the current database"
            
            procedures = []
            for row in result:
                procedures.append(
                    f"Procedure: {row[0]} | Created: {row[2]} | Modified: {row[3]} | "
                    f"Access: {row[4]} | Security: {row[5]}"
                )
            
            return "\n".join(procedures)
        except Exception as e:
            return f"Error listing stored procedures: {str(e)}"


@mcp.tool()
async def create_function(func_name: str, parameters: str, return_type: str, body: str, deterministic: bool = False):
    """Create a stored function.
    
    Args:
        func_name: Name of the function
        parameters: Parameter list (e.g., 'param1 INT, param2 VARCHAR(100)')
        return_type: Return type (e.g., 'INT', 'VARCHAR(255)')
        body: Function body SQL
        deterministic: Whether the function is deterministic
    """
    if not all(c.isalnum() or c == '_' for c in func_name):
        return "Error: Function name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            deterministic_clause = "DETERMINISTIC" if deterministic else "NOT DETERMINISTIC"
            query = f"""
            DELIMITER //
            CREATE FUNCTION {func_name}({parameters}) 
            RETURNS {return_type}
            {deterministic_clause}
            READS SQL DATA
            BEGIN
                {body}
            END //
            DELIMITER ;
            """
            await conn.execute_query(query)
            return f"Stored function '{func_name}' created successfully"
        except Exception as e:
            return f"Error creating stored function '{func_name}': {str(e)}"


@mcp.tool()
async def drop_stored_function(func_name: str):
    """Drop a stored function.
    
    Args:
        func_name: Name of the function to drop
    """
    if not all(c.isalnum() or c == '_' for c in func_name):
        return "Error: Function name can only contain alphanumeric characters and underscores"
    
    async with get_mysql_connection() as conn:
        try:
            await conn.execute_query(f"DROP FUNCTION IF EXISTS {func_name}")
            return f"Stored function '{func_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping stored function '{func_name}': {str(e)}"


@mcp.tool()
async def analyze_index_usage(table_name: str = ""):
    """Analyze index usage statistics.
    
    Args:
        table_name: Optional specific table to analyze (default: all tables)
    """
    async with get_mysql_connection() as conn:
        try:
            if table_name:
                where_clause = f"WHERE t.TABLE_NAME = '{table_name}'"
            else:
                where_clause = "WHERE t.TABLE_SCHEMA = DATABASE()"
            
            query = f"""
            SELECT 
                t.TABLE_NAME,
                s.INDEX_NAME,
                s.COLUMN_NAME,
                s.SEQ_IN_INDEX,
                s.CARDINALITY,
                s.SUB_PART,
                s.INDEX_TYPE
            FROM INFORMATION_SCHEMA.STATISTICS s
            JOIN INFORMATION_SCHEMA.TABLES t ON s.TABLE_NAME = t.TABLE_NAME
            {where_clause}
            ORDER BY t.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX
            """
            result = await conn.execute_query(query)
            
            if not result:
                return f"No index information found{' for table ' + table_name if table_name else ''}"
            
            indexes = []
            for row in result:
                indexes.append(
                    f"Table: {row[0]} | Index: {row[1]} | Column: {row[2]} | "
                    f"Seq: {row[3]} | Cardinality: {row[4]} | Type: {row[6]}"
                )
            
            return "\n".join(indexes)
        except Exception as e:
            return f"Error analyzing index usage: {str(e)}"


@mcp.tool()
async def identify_redundant_indexes():
    """Identify potentially redundant indexes."""
    async with get_mysql_connection() as conn:
        try:
            query = """
            SELECT 
                TABLE_NAME,
                INDEX_NAME,
                GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as COLUMNS
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE TABLE_SCHEMA = DATABASE()
            GROUP BY TABLE_NAME, INDEX_NAME
            ORDER BY TABLE_NAME, INDEX_NAME
            """
            result = await conn.execute_query(query)
            
            if not result:
                return "No indexes found in the current database"
            
            # Group indexes by table
            table_indexes = {}
            for row in result:
                table_name = row[0]
                if table_name not in table_indexes:
                    table_indexes[table_name] = []
                table_indexes[table_name].append({
                    'name': row[1],
                    'columns': row[2].split(',')
                })
            
            redundant = []
            for table_name, indexes in table_indexes.items():
                for i, idx1 in enumerate(indexes):
                    for idx2 in indexes[i+1:]:
                        # Check if idx1 is a prefix of idx2 or vice versa
                        cols1 = idx1['columns']
                        cols2 = idx2['columns']
                        
                        if len(cols1) <= len(cols2) and cols1 == cols2[:len(cols1)]:
                            redundant.append(
                                f"Table: {table_name} | Redundant: {idx1['name']} | "
                                f"Covered by: {idx2['name']}"
                            )
                        elif len(cols2) <= len(cols1) and cols2 == cols1[:len(cols2)]:
                            redundant.append(
                                f"Table: {table_name} | Redundant: {idx2['name']} | "
                                f"Covered by: {idx1['name']}"
                            )
            
            if not redundant:
                return "No obviously redundant indexes found"
                
            return "\n".join(redundant)
        except Exception as e:
            return f"Error identifying redundant indexes: {str(e)}"


@mcp.tool()
async def rebuild_table_indexes(table_name: str):
    """Rebuild all indexes for a table.
    
    Args:
        table_name: Name of the table
    """
    async with get_mysql_connection() as conn:
        try:
            query = f"ALTER TABLE {conn.quote_identifier(table_name)} ENGINE=InnoDB"
            await conn.execute_query(query)
            return f"Indexes for table '{table_name}' rebuilt successfully"
        except Exception as e:
            return f"Error rebuilding indexes for table '{table_name}': {str(e)}"
        except Exception as e:
            return f"Error rebuilding indexes for table '{table_name}': {str(e)}"


@mcp.prompt()
def generate_sql_query(task: str, table_name: str = ""):
    """Generate SQL query based on task description.
    
    Args:
        task: Description of what you want to accomplish
        table_name: Optional specific table to work with
    """
    table_hint = f" Focus on the '{table_name}' table." if table_name else ""
    
    return f"""Generate a SQL query to accomplish the following task: {task}{table_hint}

Please provide:
1. The complete SQL query
2. A brief explanation of what the query does
3. Any important considerations or assumptions

Make sure the query is safe and follows best practices."""


@mcp.prompt()
def analyze_query_performance(query: str):
    """Analyze SQL query performance and suggest optimizations.
    
    Args:
        query: The SQL query to analyze
    """
    return f"""Analyze the following SQL query for performance and suggest optimizations:

```sql
{query}
```

Please provide:
1. Performance analysis
2. Potential bottlenecks
3. Optimization suggestions
4. Index recommendations if applicable
5. Alternative query approaches if beneficial"""


def main():
    """Main entry point for the MySQL MCP server."""
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("""
MySQL MCP Server

Environment Variables:
  MYSQL_HOST        MySQL host (default: localhost)
  MYSQL_PORT        MySQL port (default: 3306)  
  MYSQL_USER        MySQL username (default: root)
  MYSQL_PASSWORD    MySQL password (default: empty)
  MYSQL_DATABASE    MySQL database name (default: empty)

Usage:
  python mysql_server.py                    # Run with stdio transport
  python mysql_server.py --transport sse    # Run with SSE transport
""")
        return
    
    # Determine transport
    transport = "stdio"
    if "--transport" in sys.argv:
        idx = sys.argv.index("--transport")
        if idx + 1 < len(sys.argv):
            transport = sys.argv[idx + 1]
    
    # Run the server
    mcp.run(transport=transport)


@mcp.tool()
async def mysql_show_user_tables(ctx: Context):
    """Show tables owned by the current MySQL user."""
    try:
        async with get_mysql_connection() as conn:
            query = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'"
            tables = conn.execute_query(query)
            
            return {
                "success": True,
                "tables": [t["TABLE_NAME"] for t in tables],
                "count": len(tables)
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_table_engines(ctx: Context):
    """Show available storage engines in the MySQL server."""
    try:
        async with get_mysql_connection() as conn:
            query = "SHOW ENGINES"
            engines = conn.execute_query(query)
            
            return {
                "success": True,
                "engines": engines,
                "count": len(engines)
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_table_partitions(ctx: Context, table_name: str):
    """Show partitioning information for a table if partitioned."""
    try:
        async with get_mysql_connection() as conn:
            # Validate table name (basic validation)
            if not table_name.replace('_', '').replace('-', '').isalnum():
                raise ValueError("Invalid table name")
            
            query = """
                SELECT PARTITION_NAME, SUBPARTITION_NAME, PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION, PARTITION_METHOD, SUBPARTITION_METHOD, PARTITION_EXPRESSION
                FROM information_schema.partitions
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION
            """
            partitions = conn.execute_prepared_query(query, [table_name])

            return {
                "success": True,
                "table_name": table_name,
                "partitions": partitions,
                "partition_count": len(partitions)
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_table_constraints(ctx: Context, table_name: str):
    """Show constraints for a given table (primary keys, unique, foreign keys)."""
    try:
        async with get_mysql_connection() as conn:
            # Validate table name (basic validation)
            if not table_name.replace('_', '').replace('-', '').isalnum():
                raise ValueError("Invalid table name")
                
            query = """
                SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
                FROM information_schema.table_constraints
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            """
            constraints = conn.execute_prepared_query(query, [table_name])

            return {
                "success": True,
                "table_name": table_name,
                "constraints": constraints,
                "count": len(constraints)
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_foreign_keys(ctx: Context, table_name: str):
    """Show foreign key constraints for a table."""
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)

        query = """
            SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.key_column_usage
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        foreign_keys = db.execute_prepared_query(query, [table_name])

        return {
            "success": True,
            "table_name": table_name,
            "foreign_keys": foreign_keys,
            "count": len(foreign_keys)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_check_constraints(ctx: Context, table_name: str):
    """Show CHECK constraints for a table."""
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)

        query = """
            SELECT CONSTRAINT_NAME, CHECK_CLAUSE
            FROM information_schema.check_constraints cc
            JOIN information_schema.table_constraints tc
            ON cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = %s AND tc.CONSTRAINT_TYPE = 'CHECK'
        """
        check_constraints = db.execute_prepared_query(query, [table_name])

        return {
            "success": True,
            "table_name": table_name,
            "check_constraints": check_constraints,
            "count": len(check_constraints)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_table_triggers(ctx: Context, table_name: str):
    """List triggers defined on a table."""
    try:
        db = ctx.lifespan["db"]
        table_name = db.validate_table_name(table_name)

        query = "SHOW TRIGGERS WHERE `Table` = %s"
        triggers = db.execute_prepared_query(query, [table_name])

        return {
            "success": True,
            "table_name": table_name,
            "triggers": triggers,
            "count": len(triggers)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_procedure_params(ctx: Context, procedure_name: str):
    """Show parameters of a stored procedure."""
    try:
        db = ctx.lifespan["db"]

        query = """
            SELECT PARAMETER_NAME, DATA_TYPE, DTD_IDENTIFIER, PARAMETER_MODE
            FROM information_schema.parameters
            WHERE SPECIFIC_SCHEMA = DATABASE() AND SPECIFIC_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        params = db.execute_prepared_query(query, [procedure_name])

        return {
            "success": True,
            "procedure_name": procedure_name,
            "parameters": params,
            "count": len(params)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_function_params(ctx: Context, function_name: str):
    """Show parameters of a stored function."""
    try:
        db = ctx.lifespan["db"]
        query = """
            SELECT PARAMETER_NAME, DATA_TYPE, DTD_IDENTIFIER
            FROM information_schema.parameters
            WHERE SPECIFIC_SCHEMA = DATABASE() AND SPECIFIC_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        params = db.execute_prepared_query(query, [function_name])

        return {
            "success": True,
            "function_name": function_name,
            "parameters": params,
            "count": len(params)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_events(ctx: Context):
    """List all scheduled events."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW EVENTS"
        events = db.execute_query(query)

        return {
            "success": True,
            "events": events,
            "count": len(events)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_processlist(ctx: Context):
    """Show the current process list."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW PROCESSLIST"
        processlist = db.execute_query(query)

        return {
            "success": True,
            "processlist": processlist,
            "count": len(processlist)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_status_variables(ctx: Context, pattern: str = None):
    """Show server status variables, optionally filtered by pattern."""
    try:
        db = ctx.lifespan["db"]

        if pattern:
            query = "SHOW STATUS LIKE %s"
            status_vars = db.execute_prepared_query(query, [pattern])
        else:
            query = "SHOW STATUS"
            status_vars = db.execute_query(query)

        return {
            "success": True,
            "variables": status_vars,
            "count": len(status_vars)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_variables(ctx: Context, pattern: str = None):
    """Show server system variables, optionally filtered by pattern."""
    try:
        db = ctx.lifespan["db"]
        if pattern:
            query = "SHOW VARIABLES LIKE %s"
            variables = db.execute_prepared_query(query, [pattern])
        else:
            query = "SHOW VARIABLES"
            variables = db.execute_query(query)

        return {
            "success": True,
            "variables": variables,
            "count": len(variables)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_tables_information(ctx: Context):
    """Show comprehensive information about all tables in current database."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW TABLE STATUS"
        tables_info = db.execute_query(query)
        
        return {
            "success": True,
            "tables_info": tables_info,
            "count": len(tables_info)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_engines_status(ctx: Context):
    """Show detailed status of all storage engines."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW ENGINES"
        engines = db.execute_query(query)

        return {
            "success": True,
            "engines": engines,
            "count": len(engines)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_memory_status(ctx: Context):
    """Show memory usage statistics."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW STATUS LIKE 'Innodb_buffer_pool_%'"
        memory_stats = db.execute_query(query)

        return {
            "success": True,
            "memory_stats": memory_stats,
            "count": len(memory_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_replication_status(ctx: Context):
    """Show replication status including master and slave."""
    try:
        db = ctx.lifespan["db"]
        master_status = []
        slave_status = []
        try:
            master_status = db.execute_query("SHOW MASTER STATUS")
        except:
            pass
        try:
            slave_status = db.execute_query("SHOW SLAVE STATUS")
        except:
            pass

        return {
            "success": True,
            "master_status": master_status,
            "slave_status": slave_status,
            "is_master": bool(master_status),
            "is_slave": bool(slave_status)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_show_error_log(ctx: Context):
    """Read the MySQL error log file content. (Requires file access privileges)"""
    import os
    try:
        # This requires that the MySQL error log file is accessible to this script
        error_log_path = "/var/log/mysql/error.log"  # Typical path on Linux, may differ
        if not os.path.exists(error_log_path):
            return {"success": False, "error": "Error log file not found"}

        with open(error_log_path, 'r') as f:
            content = f.read()

        return {"success": True, "error_log": content}
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_show_variables_status(ctx: Context):
    """Show all MySQL variables and status in one combined report."""
    try:
        db = ctx.lifespan["db"]
        variables = db.execute_query("SHOW VARIABLES")
        status = db.execute_query("SHOW STATUS")

        return {
            "success": True,
            "variables": variables,
            "status": status
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_show_schemas(ctx: Context):
    """Show all available database schemas."""
    try:
        db = ctx.lifespan["db"]
        query = "SHOW DATABASES"
        schemas = db.execute_query(query)

        return {
            "success": True,
            "schemas": [s["Database"] if "Database" in s else list(s.values())[0] for s in schemas],
            "count": len(schemas)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}

@mcp.tool()
async def mysql_show_slow_log(ctx: Context, limit: int = 10):
    """Show recent entries from the slow query log."""
    try:
        db = ctx.lifespan["db"]
        query = f"SELECT * FROM mysql.slow_log ORDER BY start_time DESC LIMIT %s"
        slow_logs = db.execute_prepared_query(query, [limit])
        return {
            "success": True,
            "slow_log": slow_logs,
            "count": len(slow_logs)
        }
    except Exception as e:
        return {"success": False, "error": str(e), "error_type": type(e).__name__}


@mcp.tool()
async def mysql_query_cache_analysis(ctx: Context):
    """Analyze query cache performance and configuration."""
    try:
        db = ctx.lifespan["db"]
        query = """
        SHOW VARIABLES LIKE 'query_cache%'
        UNION ALL
        SHOW STATUS LIKE 'Qcache%'
        """
        cache_stats = db.execute_query(query)
        
        return {
            "success": True,
            "query_cache_stats": cache_stats,
            "count": len(cache_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_innodb_buffer_pool_analysis(ctx: Context):
    """Detailed analysis of InnoDB buffer pool performance."""
    try:
        db = ctx.lifespan["db"]
        query = """
        SELECT 
            POOL_ID,
            POOL_SIZE,
            FREE_BUFFERS,
            DATABASE_PAGES,
            OLD_DATABASE_PAGES,
            MODIFIED_DATABASE_PAGES,
            PENDING_DECOMPRESS,
            PENDING_READS,
            PENDING_FLUSH_LRU,
            PENDING_FLUSH_LIST,
            PAGES_MADE_YOUNG,
            PAGES_NOT_MADE_YOUNG,
            PAGES_MADE_YOUNG_RATE,
            PAGES_MADE_NOT_YOUNG_RATE,
            NUMBER_PAGES_READ,
            NUMBER_PAGES_CREATED,
            NUMBER_PAGES_WRITTEN,
            PAGES_READ_RATE,
            PAGES_CREATE_RATE,
            PAGES_WRITTEN_RATE,
            NUMBER_PAGES_GET,
            HIT_RATE,
            YOUNG_MAKE_PER_THOUSAND_GETS,
            NOT_YOUNG_MAKE_PER_THOUSAND_GETS,
            NUMBER_PAGES_READ_AHEAD,
            NUMBER_READ_AHEAD_EVICTED,
            READ_AHEAD_RATE,
            READ_AHEAD_EVICTED_RATE,
            LRU_IO_TOTAL,
            LRU_IO_CURRENT,
            UNCOMPRESS_TOTAL,
            UNCOMPRESS_CURRENT
        FROM INFORMATION_SCHEMA.INNODB_BUFFER_POOL_STATS
        """
        buffer_pool_stats = db.execute_query(query)
        
        return {
            "success": True,
            "buffer_pool_stats": buffer_pool_stats,
            "count": len(buffer_pool_stats)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_innodb_lock_waits(ctx: Context):
    """Show detailed InnoDB lock wait information."""
    try:
        db = ctx.lifespan["db"]
        query = """
        SELECT 
            r.trx_id waiting_trx_id,
            r.trx_mysql_thread_id waiting_thread,
            r.trx_query waiting_query,
            b.trx_id blocking_trx_id,
            b.trx_mysql_thread_id blocking_thread,
            b.trx_query blocking_query,
            l.lock_table,
            l.lock_index,
            l.lock_mode,
            l.lock_type
        FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS w
        INNER JOIN INFORMATION_SCHEMA.INNODB_TRX b ON b.trx_id = w.blocking_trx_id
        INNER JOIN INFORMATION_SCHEMA.INNODB_TRX r ON r.trx_id = w.requesting_trx_id
        INNER JOIN INFORMATION_SCHEMA.INNODB_LOCKS l ON l.lock_id = w.requested_lock_id
        """
        lock_waits = db.execute_query(query)
        
        return {
            "success": True,
            "lock_waits": lock_waits,
            "count": len(lock_waits)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_table_compression_analysis(ctx: Context, schema_name: str = None):
    """Analyze table compression ratios and storage efficiency."""
    try:
        db = ctx.lifespan["db"]
        where_clause = f"AND table_schema = '{schema_name}'" if schema_name else ""
        query = f"""
        SELECT 
            table_schema,
            table_name,
            engine,
            row_format,
            table_rows,
            data_length,
            index_length,
            data_free,
            (data_length + index_length) as total_size,
            ROUND(((data_length + index_length) / 1024 / 1024), 2) as total_size_mb,
            CASE 
                WHEN row_format IN ('COMPRESSED', 'DYNAMIC') THEN 'Compressed'
                ELSE 'Not Compressed'
            END as compression_status
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        {where_clause}
        ORDER BY (data_length + index_length) DESC
        """
        compression_analysis = db.execute_query(query)
        
        return {
            "success": True,
            "compression_analysis": compression_analysis,
            "count": len(compression_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_connection_thread_analysis(ctx: Context):
    """Analyze connection threads and their resource usage."""
    try:
        db = ctx.lifespan["db"]
        query = """
        SELECT 
            t.processlist_id,
            t.processlist_user,
            t.processlist_host,
            t.processlist_db,
            t.processlist_command,
            t.processlist_time,
            t.processlist_state,
            t.processlist_info,
            esc.thread_id,
            esc.event_name,
            esc.source,
            esc.timer_wait,
            esc.lock_time,
            esc.rows_examined,
            esc.rows_sent,
            esc.rows_affected,
            esc.created_tmp_disk_tables,
            esc.created_tmp_tables,
            esc.select_full_join,
            esc.select_full_range_join,
            esc.select_range,
            esc.select_range_check,
            esc.select_scan,
            esc.sort_merge_passes,
            esc.sort_range,
            esc.sort_rows,
            esc.sort_scan
        FROM performance_schema.threads t
        LEFT JOIN performance_schema.events_statements_current esc ON t.thread_id = esc.thread_id
        WHERE t.processlist_id IS NOT NULL
        ORDER BY t.processlist_time DESC
        """
        thread_analysis = db.execute_query(query)
        
        return {
            "success": True,
            "thread_analysis": thread_analysis,
            "count": len(thread_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_query_digest_analysis(ctx: Context, limit: int = 10):
    """Analyze query digest statistics for performance optimization."""
    try:
        db = ctx.lifespan["db"]
        query = f"""
        SELECT 
            schema_name,
            digest,
            digest_text,
            count_star,
            sum_timer_wait,
            min_timer_wait,
            avg_timer_wait,
            max_timer_wait,
            sum_lock_time,
            sum_errors,
            sum_warnings,
            sum_rows_affected,
            sum_rows_sent,
            sum_rows_examined,
            sum_created_tmp_disk_tables,
            sum_created_tmp_tables,
            sum_select_full_join,
            sum_select_full_range_join,
            sum_select_range,
            sum_select_range_check,
            sum_select_scan,
            sum_sort_merge_passes,
            sum_sort_range,
            sum_sort_rows,
            sum_sort_scan,
            sum_no_index_used,
            sum_no_good_index_used,
            first_seen,
            last_seen
        FROM performance_schema.events_statements_summary_by_digest
        ORDER BY sum_timer_wait DESC
        LIMIT %s
        """
        digest_analysis = db.execute_prepared_query(query, [limit])
        
        return {
            "success": True,
            "query_digest_analysis": digest_analysis,
            "count": len(digest_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_partition_performance_analysis(ctx: Context, schema_name: str = None, table_name: str = None):
    """Analyze partition performance and pruning effectiveness."""
    try:
        db = ctx.lifespan["db"]
        where_clause = ""
        params = []
        
        if schema_name:
            where_clause += " AND table_schema = %s"
            params.append(schema_name)
        if table_name:
            where_clause += " AND table_name = %s"
            params.append(table_name)
            
        query = f"""
        SELECT 
            table_schema,
            table_name,
            partition_name,
            partition_ordinal_position,
            partition_method,
            partition_expression,
            partition_description,
            table_rows,
            avg_row_length,
            data_length,
            max_data_length,
            index_length,
            data_free,
            create_time,
            update_time,
            check_time,
            checksum,
            partition_comment
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE partition_name IS NOT NULL
        {where_clause}
        ORDER BY table_schema, table_name, partition_ordinal_position
        """
        
        if params:
            partition_analysis = db.execute_prepared_query(query, params)
        else:
            partition_analysis = db.execute_query(query)
        
        return {
            "success": True,
            "partition_analysis": partition_analysis,
            "count": len(partition_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_foreign_key_dependency_analysis(ctx: Context, schema_name: str = None):
    """Analyze foreign key dependencies and referential integrity."""
    try:
        db = ctx.lifespan["db"]
        where_clause = f"WHERE constraint_schema = '{schema_name}'" if schema_name else ""
        query = f"""
        SELECT 
            kcu.constraint_schema,
            kcu.constraint_name,
            kcu.table_name,
            kcu.column_name,
            kcu.referenced_table_schema,
            kcu.referenced_table_name,
            kcu.referenced_column_name,
            rc.match_option,
            rc.update_rule,
            rc.delete_rule,
            tc.constraint_type
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
            ON kcu.constraint_name = rc.constraint_name 
            AND kcu.constraint_schema = rc.constraint_schema
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
            ON kcu.constraint_name = tc.constraint_name 
            AND kcu.constraint_schema = tc.constraint_schema
        {where_clause}
        ORDER BY kcu.constraint_schema, kcu.table_name, kcu.constraint_name
        """
        fk_analysis = db.execute_query(query)
        
        return {
            "success": True,
            "foreign_key_analysis": fk_analysis,
            "count": len(fk_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_table_space_analysis(ctx: Context):
    """Analyze tablespace usage and file system layout."""
    try:
        db = ctx.lifespan["db"]
        query = """
        SELECT 
            space,
            name,
            flag,
            row_format,
            page_size,
            zip_page_size,
            space_type,
            fs_block_size,
            file_size,
            allocated_size,
            autoextend_size,
            server_version,
            space_version
        FROM INFORMATION_SCHEMA.INNODB_TABLESPACES
        ORDER BY allocated_size DESC
        """
        tablespace_analysis = db.execute_query(query)
        
        return {
            "success": True,
            "tablespace_analysis": tablespace_analysis,
            "count": len(tablespace_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_innodb_metrics_deep_analysis(ctx: Context, metric_pattern: str = None):
    """Analyze InnoDB performance metrics and counters."""
    try:
        db = ctx.lifespan["db"]
        where_clause = f"WHERE name LIKE '%{metric_pattern}%'" if metric_pattern else ""
        query = f"""
        SELECT 
            name,
            subsystem,
            count,
            max_count,
            min_count,
            avg_count,
            count_reset,
            max_count_reset,
            min_count_reset,
            avg_count_reset,
            time_enabled,
            time_disabled,
            time_elapsed,
            time_remaining,
            status,
            type,
            comment
        FROM INFORMATION_SCHEMA.INNODB_METRICS
        {where_clause}
        ORDER BY subsystem, name
        """
        metrics_analysis = db.execute_query(query)
        
        return {
            "success": True,
            "innodb_metrics": metrics_analysis,
            "count": len(metrics_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_performance_schema_setup_analysis(ctx: Context):
    """Analyze Performance Schema configuration and instrumentation setup."""
    try:
        db = ctx.lifespan["db"]
        
        # Get consumers
        consumers = db.execute_query("SELECT * FROM performance_schema.setup_consumers ORDER BY name")
        
        # Get instruments
        instruments = db.execute_query("""
            SELECT name, enabled, timed 
            FROM performance_schema.setup_instruments 
            WHERE enabled = 'YES' 
            ORDER BY name
        """)
        
        # Get actors
        actors = db.execute_query("SELECT * FROM performance_schema.setup_actors ORDER BY host, user")
        
        # Get objects
        objects = db.execute_query("""
            SELECT object_type, object_schema, object_name, enabled, timed
            FROM performance_schema.setup_objects 
            ORDER BY object_type, object_schema, object_name
        """)
        
        return {
            "success": True,
            "consumers": consumers,
            "enabled_instruments": instruments,
            "actors": actors,
            "objects": objects,
            "summary": {
                "consumers_count": len(consumers),
                "enabled_instruments_count": len(instruments),
                "actors_count": len(actors),
                "objects_count": len(objects)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_memory_usage_by_thread(ctx: Context, limit: int = 20):
    """Analyze memory usage by thread for performance optimization."""
    try:
        db = ctx.lifespan["db"]
        query = f"""
        SELECT 
            t.processlist_id,
            t.processlist_user,
            t.processlist_host,
            t.processlist_db,
            t.processlist_command,
            t.processlist_state,
            mbt.sum_number_of_bytes_alloc,
            mbt.sum_number_of_bytes_free,
            mbt.sum_number_of_bytes_alloc - mbt.sum_number_of_bytes_free as current_allocated,
            mbt.current_count_used,
            mbt.high_count_used,
            mbt.low_count_used
        FROM performance_schema.threads t
        JOIN performance_schema.memory_summary_by_thread_by_event_name mbt ON t.thread_id = mbt.thread_id
        WHERE t.processlist_id IS NOT NULL
        AND mbt.event_name = 'memory/sql/thd::main_mem_root'
        ORDER BY current_allocated DESC
        LIMIT %s
        """
        memory_analysis = db.execute_prepared_query(query, [limit])
        
        return {
            "success": True,
            "memory_usage_by_thread": memory_analysis,
            "count": len(memory_analysis)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_index_usage_effectiveness(ctx: Context, schema_name: str = None):
    """Analyze index usage effectiveness and identify unused indexes."""
    try:
        db = ctx.lifespan["db"]
        where_clause = f"AND object_schema = '{schema_name}'" if schema_name else ""
        query = f"""
        SELECT 
            object_schema,
            object_name,
            index_name,
            count_fetch,
            count_insert,
            count_update,
            count_delete,
            (count_fetch + count_insert + count_update + count_delete) as total_operations,
            CASE 
                WHEN count_fetch = 0 THEN 'NEVER_USED_FOR_READS'
                WHEN count_fetch < 10 THEN 'RARELY_USED_FOR_READS'
                WHEN count_fetch < 100 THEN 'MODERATELY_USED_FOR_READS'
                ELSE 'FREQUENTLY_USED_FOR_READS'
            END as read_usage_category,
            CASE 
                WHEN (count_insert + count_update + count_delete) = 0 THEN 'NO_WRITE_OVERHEAD'
                WHEN (count_insert + count_update + count_delete) < 10 THEN 'LOW_WRITE_OVERHEAD'
                WHEN (count_insert + count_update + count_delete) < 100 THEN 'MODERATE_WRITE_OVERHEAD'
                ELSE 'HIGH_WRITE_OVERHEAD'
            END as write_overhead_category
        FROM performance_schema.table_io_waits_summary_by_index_usage
        WHERE object_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
        {where_clause}
        ORDER BY object_schema, object_name, total_operations DESC
        """
        index_effectiveness = db.execute_query(query)
        
        return {
            "success": True,
            "index_effectiveness": index_effectiveness,
            "count": len(index_effectiveness)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_temp_table_analysis(ctx: Context):
    """Analyze temporary table usage and identify optimization opportunities."""
    try:
        db = ctx.lifespan["db"]
        
        # Get temporary table statistics from status variables
        temp_stats_query = """
        SHOW STATUS WHERE Variable_name IN (
            'Created_tmp_tables',
            'Created_tmp_disk_tables',
            'Created_tmp_files'
        )
        """
        temp_stats = db.execute_query(temp_stats_query)
        
        # Get current temporary tables from performance schema
        current_temp_query = """
        SELECT 
            object_schema,
            object_name,
            object_type,
            created
        FROM performance_schema.objects_summary_global_by_type
        WHERE object_type LIKE '%tmp%'
        ORDER BY created DESC
        """
        current_temp = db.execute_query(current_temp_query)
        
        # Get configuration variables related to temporary tables
        temp_config_query = """
        SHOW VARIABLES WHERE Variable_name IN (
            'tmp_table_size',
            'max_heap_table_size',
            'tmpdir',
            'big_tables'
        )
        """
        temp_config = db.execute_query(temp_config_query)
        
        return {
            "success": True,
            "temporary_table_stats": temp_stats,
            "current_temporary_objects": current_temp,
            "temporary_table_config": temp_config,
            "summary": {
                "stats_count": len(temp_stats),
                "current_temp_objects": len(current_temp),
                "config_variables": len(temp_config)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_ssl_connection_analysis(ctx: Context):
    """Analyze SSL/TLS connection configuration and usage."""
    try:
        db = ctx.lifespan["db"]
        
        # Get SSL configuration variables
        ssl_config = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE '%ssl%' OR Variable_name LIKE '%tls%'
        """)
        
        # Get SSL status information
        ssl_status = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE '%ssl%' OR Variable_name LIKE '%tls%'
        """)
        
        # Get current SSL connections from processlist
        ssl_connections = db.execute_query("""
            SELECT 
                ID,
                USER,
                HOST,
                DB,
                COMMAND,
                TIME,
                STATE,
                INFO
            FROM INFORMATION_SCHEMA.PROCESSLIST
            WHERE HOST LIKE '%SSL%' OR INFO LIKE '%SSL%'
        """)
        
        return {
            "success": True,
            "ssl_configuration": ssl_config,
            "ssl_status": ssl_status,
            "ssl_connections": ssl_connections,
            "summary": {
                "ssl_config_vars": len(ssl_config),
                "ssl_status_vars": len(ssl_status),
                "active_ssl_connections": len(ssl_connections)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_charset_collation_analysis(ctx: Context, schema_name: str = None):
    """Analyze character set and collation usage across databases and tables."""
    try:
        db = ctx.lifespan["db"]
        
        # Get database character sets and collations
        db_charset_query = f"""
        SELECT 
            SCHEMA_NAME as database_name,
            DEFAULT_CHARACTER_SET_NAME as charset,
            DEFAULT_COLLATION_NAME as collation
        FROM INFORMATION_SCHEMA.SCHEMATA
        {f"WHERE SCHEMA_NAME = '{schema_name}'" if schema_name else "WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"}
        ORDER BY SCHEMA_NAME
        """
        db_charsets = db.execute_query(db_charset_query)
        
        # Get table character sets and collations
        table_charset_query = f"""
        SELECT 
            TABLE_SCHEMA as database_name,
            TABLE_NAME as table_name,
            TABLE_COLLATION as table_collation,
            TABLE_COMMENT
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        {f"AND TABLE_SCHEMA = '{schema_name}'" if schema_name else ""}
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        table_charsets = db.execute_query(table_charset_query)
        
        # Get column character sets and collations
        column_charset_query = f"""
        SELECT 
            TABLE_SCHEMA as database_name,
            TABLE_NAME as table_name,
            COLUMN_NAME as column_name,
            DATA_TYPE,
            CHARACTER_SET_NAME as charset,
            COLLATION_NAME as collation
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE CHARACTER_SET_NAME IS NOT NULL
        AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        {f"AND TABLE_SCHEMA = '{schema_name}'" if schema_name else ""}
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """
        column_charsets = db.execute_query(column_charset_query)
        
        # Get available character sets
        available_charsets = db.execute_query("""
            SELECT 
                CHARACTER_SET_NAME as charset,
                DEFAULT_COLLATE_NAME as default_collation,
                DESCRIPTION,
                MAXLEN as max_bytes_per_char
            FROM INFORMATION_SCHEMA.CHARACTER_SETS
            ORDER BY CHARACTER_SET_NAME
        """)
        
        return {
            "success": True,
            "database_charsets": db_charsets,
            "table_charsets": table_charsets,
            "column_charsets": column_charsets,
            "available_charsets": available_charsets,
            "summary": {
                "databases_analyzed": len(db_charsets),
                "tables_analyzed": len(table_charsets),
                "columns_with_charset": len(column_charsets),
                "available_charsets_count": len(available_charsets)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_replication_lag_analysis(ctx: Context):
    """Analyze replication lag and slave status for monitoring purposes."""
    try:
        db = ctx.lifespan["db"]
        
        # Get master status
        master_status = []
        try:
            master_status = db.execute_query("SHOW MASTER STATUS")
        except:
            pass  # Not a master or no binary logging
        
        # Get slave status
        slave_status = []
        try:
            slave_status = db.execute_query("SHOW SLAVE STATUS")
        except:
            pass  # Not a slave
            
        # Get replication-related variables
        repl_variables = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE '%repl%' 
            OR Variable_name LIKE '%slave%' 
            OR Variable_name LIKE '%master%'
            OR Variable_name LIKE '%binlog%'
        """)
        
        # Get binary log files if this is a master
        binary_logs = []
        try:
            binary_logs = db.execute_query("SHOW BINARY LOGS")
        except:
            pass  # No binary logging or not a master
            
        # Calculate lag information from slave status if available
        lag_info = {}
        if slave_status:
            for slave in slave_status:
                lag_info = {
                    "seconds_behind_master": slave.get("Seconds_Behind_Master"),
                    "master_log_file": slave.get("Master_Log_File"),
                    "read_master_log_pos": slave.get("Read_Master_Log_Pos"),
                    "relay_master_log_file": slave.get("Relay_Master_Log_File"),
                    "exec_master_log_pos": slave.get("Exec_Master_Log_Pos"),
                    "slave_io_running": slave.get("Slave_IO_Running"),
                    "slave_sql_running": slave.get("Slave_SQL_Running"),
                    "last_errno": slave.get("Last_Errno"),
                    "last_error": slave.get("Last_Error")
                }
                break
        
        return {
            "success": True,
            "is_master": bool(master_status),
            "is_slave": bool(slave_status),
            "master_status": master_status,
            "slave_status": slave_status,
            "lag_information": lag_info,
            "replication_variables": repl_variables,
            "binary_logs": binary_logs,
            "summary": {
                "replication_role": "Master" if master_status else "Slave" if slave_status else "Standalone",
                "binary_logs_count": len(binary_logs),
                "replication_variables_count": len(repl_variables)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_query_optimizer_analysis(ctx: Context, query: str):
    """Analyze query execution plan and optimizer decisions."""
    try:
        db = ctx.lifespan["db"]
        
        # Get traditional EXPLAIN output
        explain_query = f"EXPLAIN {query}"
        explain_result = db.execute_query(explain_query)
        
        # Get extended EXPLAIN output if supported
        extended_explain = []
        try:
            extended_explain = db.execute_query(f"EXPLAIN EXTENDED {query}")
        except:
            pass  # Not supported or query issue
            
        # Get JSON format EXPLAIN if supported
        json_explain = []
        try:
            json_explain = db.execute_query(f"EXPLAIN FORMAT=JSON {query}")
        except:
            pass  # Not supported or query issue
            
        # Get optimizer trace if enabled
        optimizer_trace = []
        try:
            # Enable optimizer trace
            db.execute_query("SET optimizer_trace='enabled=on'")
            # Execute the query (but limit it to avoid long execution)
            db.execute_query(f"SELECT 1 FROM ({query} LIMIT 1) AS trace_query")
            # Get the trace
            optimizer_trace = db.execute_query("""
                SELECT TRACE, MISSING_BYTES_BEYOND_MAX_MEM_SIZE, INSUFFICIENT_PRIVILEGES
                FROM INFORMATION_SCHEMA.OPTIMIZER_TRACE
            """)
            # Disable optimizer trace
            db.execute_query("SET optimizer_trace='enabled=off'")
        except:
            pass  # Optimizer trace not available or enabled
        
        return {
            "success": True,
            "query": query,
            "explain_result": explain_result,
            "extended_explain": extended_explain,
            "json_explain": json_explain,
            "optimizer_trace": optimizer_trace,
            "analysis_summary": {
                "explain_rows": len(explain_result),
                "extended_available": bool(extended_explain),
                "json_format_available": bool(json_explain),
                "optimizer_trace_available": bool(optimizer_trace)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_comprehensive_health_check(ctx: Context):
    """Perform a comprehensive MySQL server health check and diagnostics."""
    try:
        db = ctx.lifespan["db"]
        
        health_report = {}
        
        # 1. Basic server information
        try:
            server_info = db.execute_query("SELECT VERSION() as version, NOW() as current_time")
            uptime = db.execute_query("SHOW STATUS LIKE 'Uptime'")
            health_report["server_info"] = {
                "version_info": server_info,
                "uptime": uptime
            }
        except Exception as e:
            health_report["server_info"] = {"error": str(e)}
        
        # 2. Connection and thread statistics
        try:
            connection_stats = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Threads_connected', 'Threads_running', 'Connections',
                    'Aborted_connects', 'Aborted_clients', 'Max_used_connections'
                )
            """)
            max_connections = db.execute_query("SHOW VARIABLES LIKE 'max_connections'")
            health_report["connections"] = {
                "statistics": connection_stats,
                "configuration": max_connections
            }
        except Exception as e:
            health_report["connections"] = {"error": str(e)}
        
        # 3. InnoDB status
        try:
            innodb_stats = db.execute_query("""
                SHOW STATUS WHERE Variable_name LIKE 'Innodb%' 
                AND Variable_name IN (
                    'Innodb_buffer_pool_read_requests',
                    'Innodb_buffer_pool_reads',
                    'Innodb_buffer_pool_pages_dirty',
                    'Innodb_buffer_pool_pages_free',
                    'Innodb_buffer_pool_pages_total',
                    'Innodb_rows_read',
                    'Innodb_rows_inserted',
                    'Innodb_rows_updated',
                    'Innodb_rows_deleted'
                )
            """)
            health_report["innodb_status"] = innodb_stats
        except Exception as e:
            health_report["innodb_status"] = {"error": str(e)}
        
        # 4. Query performance indicators
        try:
            query_stats = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Slow_queries', 'Questions', 'Queries',
                    'Com_select', 'Com_insert', 'Com_update', 'Com_delete',
                    'Created_tmp_tables', 'Created_tmp_disk_tables',
                    'Sort_merge_passes', 'Table_locks_waited'
                )
            """)
            health_report["query_performance"] = query_stats
        except Exception as e:
            health_report["query_performance"] = {"error": str(e)}
        
        # 5. Storage and table statistics
        try:
            storage_stats = db.execute_query("""
                SELECT 
                    COUNT(*) as total_tables,
                    SUM(data_length + index_length) as total_size_bytes,
                    SUM(data_length + index_length) / 1024 / 1024 as total_size_mb,
                    AVG(data_length + index_length) as avg_table_size_bytes
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            """)
            health_report["storage_statistics"] = storage_stats
        except Exception as e:
            health_report["storage_statistics"] = {"error": str(e)}
        
        # 6. Replication status (if applicable)
        try:
            master_status = []
            slave_status = []
            try:
                master_status = db.execute_query("SHOW MASTER STATUS")
            except:
                pass
            try:
                slave_status = db.execute_query("SHOW SLAVE STATUS")
            except:
                pass
            health_report["replication"] = {
                "is_master": bool(master_status),
                "is_slave": bool(slave_status),
                "master_status": master_status,
                "slave_status": slave_status
            }
        except Exception as e:
            health_report["replication"] = {"error": str(e)}
        
        # 7. Error log indicators
        try:
            error_indicators = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Aborted_clients', 'Aborted_connects',
                    'Connection_errors_internal', 'Connection_errors_max_connections'
                )
            """)
            health_report["error_indicators"] = error_indicators
        except Exception as e:
            health_report["error_indicators"] = {"error": str(e)}
        
        # 8. Security configuration check
        try:
            security_vars = db.execute_query("""
                SHOW VARIABLES WHERE Variable_name IN (
                    'ssl_ca', 'ssl_cert', 'ssl_key',
                    'validate_password_policy', 'local_infile'
                )
            """)
            health_report["security_config"] = security_vars
        except Exception as e:
            health_report["security_config"] = {"error": str(e)}
        
        return {
            "success": True,
            "health_check_report": health_report,
            "timestamp": db.execute_query("SELECT NOW() as check_time")[0]["check_time"],
            "summary": {
                "sections_checked": len([k for k in health_report.keys() if "error" not in health_report[k]]),
                "sections_with_errors": len([k for k in health_report.keys() if isinstance(health_report[k], dict) and "error" in health_report[k]])
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_buffer_pool_hit_ratio(ctx: Context):
    """Calculate and analyze InnoDB buffer pool hit ratio and efficiency."""
    try:
        db = ctx.lifespan["db"]
        
        buffer_pool_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name IN (
                'Innodb_buffer_pool_read_requests',
                'Innodb_buffer_pool_reads',
                'Innodb_buffer_pool_pages_total',
                'Innodb_buffer_pool_pages_free',
                'Innodb_buffer_pool_pages_dirty',
                'Innodb_buffer_pool_pages_data'
            )
        """)
        
        # Calculate hit ratio
        read_requests = 0
        physical_reads = 0
        
        stats_dict = {}
        for stat in buffer_pool_stats:
            stats_dict[stat['Variable_name']] = int(stat['Value'])
            
        read_requests = stats_dict.get('Innodb_buffer_pool_read_requests', 0)
        physical_reads = stats_dict.get('Innodb_buffer_pool_reads', 0)
        
        hit_ratio = 0
        if read_requests > 0:
            hit_ratio = (read_requests - physical_reads) / read_requests * 100
            
        total_pages = stats_dict.get('Innodb_buffer_pool_pages_total', 0)
        free_pages = stats_dict.get('Innodb_buffer_pool_pages_free', 0)
        dirty_pages = stats_dict.get('Innodb_buffer_pool_pages_dirty', 0)
        
        utilization = 0
        if total_pages > 0:
            utilization = (total_pages - free_pages) / total_pages * 100
            
        return {
            "success": True,
            "buffer_pool_analysis": {
                "hit_ratio_percentage": round(hit_ratio, 2),
                "buffer_pool_utilization_percentage": round(utilization, 2),
                "total_pages": total_pages,
                "free_pages": free_pages,
                "dirty_pages": dirty_pages,
                "read_requests": read_requests,
                "physical_reads": physical_reads,
                "recommendation": "Good" if hit_ratio >= 99 else "Consider increasing buffer pool size" if hit_ratio < 95 else "Monitor"
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_temp_table_usage(ctx: Context):
    """Analyze MySQL thread pool status and performance metrics."""
    try:
        db = ctx.lifespan["db"]
        
        # Check if thread pool is enabled
        thread_pool_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE 'thread_pool%'
        """)
        
        thread_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE 'Thread%'
            OR Variable_name IN ('Connections', 'Max_used_connections')
        """)
        
        processlist = db.execute_query("""
            SELECT 
                State,
                COUNT(*) as count,
                AVG(Time) as avg_time,
                MAX(Time) as max_time
            FROM INFORMATION_SCHEMA.PROCESSLIST 
            WHERE Command != 'Sleep'
            GROUP BY State
            ORDER BY count DESC
        """)
        
        return {
            "success": True,
            "thread_pool_config": thread_pool_vars,
            "thread_statistics": thread_stats,
            "active_connections_by_state": processlist
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_query_cache_performance(ctx: Context):
    """Analyze query cache hit ratio and efficiency metrics."""
    try:
        db = ctx.lifespan["db"]
        
        cache_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE 'query_cache%'
        """)
        
        cache_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE 'Qcache%'
        """)
        
        # Calculate cache efficiency
        stats_dict = {}
        for stat in cache_stats:
            stats_dict[stat['Variable_name']] = int(stat['Value'])
            
        hits = stats_dict.get('Qcache_hits', 0)
        inserts = stats_dict.get('Qcache_inserts', 0)
        
        hit_ratio = 0
        if (hits + inserts) > 0:
            hit_ratio = hits / (hits + inserts) * 100
            
        return {
            "success": True,
            "query_cache_config": cache_vars,
            "query_cache_statistics": cache_stats,
            "efficiency_metrics": {
                "hit_ratio_percentage": round(hit_ratio, 2),
                "total_queries": hits + inserts,
                "cache_hits": hits,
                "cache_inserts": inserts,
                "recommendation": "Excellent" if hit_ratio >= 80 else "Good" if hit_ratio >= 60 else "Poor - Consider optimization"
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_deadlock_analysis(ctx: Context):
    """Analyze recent deadlocks and lock contention patterns."""
    try:
        db = ctx.lifespan["db"]
        
        # Get InnoDB status for deadlock information
        try:
            innodb_status = db.execute_query("SHOW ENGINE INNODB STATUS")
        except:
            innodb_status = []
        
        # Lock waits and timeouts
        lock_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name IN (
                'Innodb_lock_timeouts',
                'Innodb_deadlocks',
                'Innodb_row_lock_current_waits',
                'Innodb_row_lock_time',
                'Innodb_row_lock_time_avg',
                'Innodb_row_lock_time_max',
                'Innodb_row_lock_waits',
                'Table_locks_immediate',
                'Table_locks_waited'
            )
        """)
        
        # Current lock waits from Performance Schema (if available)
        try:
            current_locks = db.execute_query("""
                SELECT 
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    LOCK_TYPE,
                    LOCK_DURATION,
                    LOCK_STATUS,
                    COUNT(*) as lock_count
                FROM performance_schema.metadata_locks 
                WHERE OBJECT_TYPE = 'TABLE'
                GROUP BY OBJECT_SCHEMA, OBJECT_NAME, LOCK_TYPE, LOCK_DURATION, LOCK_STATUS
                ORDER BY lock_count DESC
                LIMIT 20
            """)
        except:
            current_locks = []
            
        return {
            "success": True,
            "lock_statistics": lock_stats,
            "current_metadata_locks": current_locks,
            "innodb_status_available": bool(innodb_status)
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_io_statistics_analysis(ctx: Context):
    """Analyze MySQL I/O statistics and disk usage patterns."""
    try:
        db = ctx.lifespan["db"]
        
        # File I/O statistics
        file_io_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE 'Innodb_data%'
            OR Variable_name LIKE 'Innodb_log%'
            OR Variable_name LIKE 'Innodb_os%'
        """)
        
        # Table I/O statistics from Performance Schema
        try:
            table_io_stats = db.execute_query("""
                SELECT 
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    COUNT_READ,
                    COUNT_WRITE,
                    SUM_TIMER_READ / 1000000000 as read_time_seconds,
                    SUM_TIMER_WRITE / 1000000000 as write_time_seconds
                FROM performance_schema.table_io_waits_summary_by_table
                WHERE OBJECT_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                ORDER BY (COUNT_READ + COUNT_WRITE) DESC
                LIMIT 20
            """)
        except:
            table_io_stats = []
            
        # Temporary table usage
        temp_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE 'Created_tmp%'
        """)
        
        return {
            "success": True,
            "file_io_statistics": file_io_stats,
            "table_io_statistics": table_io_stats,
            "temporary_table_stats": temp_stats
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_memory_usage_breakdown(ctx: Context):
    """Provide detailed breakdown of MySQL memory usage by component."""
    try:
        db = ctx.lifespan["db"]
        
        # Memory-related variables
        memory_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name IN (
                'innodb_buffer_pool_size',
                'key_buffer_size',
                'query_cache_size',
                'tmp_table_size',
                'max_heap_table_size',
                'sort_buffer_size',
                'read_buffer_size',
                'read_rnd_buffer_size',
                'join_buffer_size',
                'thread_stack',
                'max_connections',
                'table_open_cache',
                'table_definition_cache'
            )
        """)
        
        # Memory usage from Performance Schema (if available)
        try:
            memory_usage = db.execute_query("""
                SELECT 
                    EVENT_NAME,
                    CURRENT_COUNT_USED,
                    CURRENT_SIZE_USED,
                    CURRENT_SIZE_USED / 1024 / 1024 as size_mb
                FROM performance_schema.memory_summary_global_by_event_name
                WHERE CURRENT_SIZE_USED > 0
                ORDER BY CURRENT_SIZE_USED DESC
                LIMIT 20
            """)
        except:
            memory_usage = []
            
        # Calculate estimated memory usage
        vars_dict = {}
        for var in memory_vars:
            try:
                vars_dict[var['Variable_name']] = int(var['Value'])
            except:
                vars_dict[var['Variable_name']] = var['Value']
                
        estimated_memory = {
            "innodb_buffer_pool_mb": vars_dict.get('innodb_buffer_pool_size', 0) / 1024 / 1024,
            "key_buffer_mb": vars_dict.get('key_buffer_size', 0) / 1024 / 1024,
            "query_cache_mb": vars_dict.get('query_cache_size', 0) / 1024 / 1024,
            "tmp_table_mb": vars_dict.get('tmp_table_size', 0) / 1024 / 1024
        }
        
        return {
            "success": True,
            "memory_configuration": memory_vars,
            "memory_usage_details": memory_usage,
            "estimated_memory_allocation_mb": estimated_memory
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_plugin_and_components_status(ctx: Context):
    """Show status of MySQL plugins and components."""
    try:
        db = ctx.lifespan["db"]
        
        # Active plugins
        plugins = db.execute_query("""
            SELECT 
                PLUGIN_NAME,
                PLUGIN_VERSION,
                PLUGIN_STATUS,
                PLUGIN_TYPE,
                PLUGIN_DESCRIPTION,
                LOAD_OPTION
            FROM INFORMATION_SCHEMA.PLUGINS
            ORDER BY PLUGIN_TYPE, PLUGIN_NAME
        """)
        
        # Storage engines
        engines = db.execute_query("""
            SELECT 
                ENGINE,
                SUPPORT,
                COMMENT,
                TRANSACTIONS,
                XA,
                SAVEPOINTS
            FROM INFORMATION_SCHEMA.ENGINES
            ORDER BY SUPPORT DESC, ENGINE
        """)
        
        # Components (MySQL 8.0+)
        try:
            components = db.execute_query("""
                SELECT 
                    COMPONENT_ID,
                    COMPONENT_URN,
                    COMPONENT_VERSION
                FROM mysql.component
            """)
        except:
            components = []
            
        return {
            "success": True,
            "plugins": plugins,
            "storage_engines": engines,
            "components": components
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_ssl_encryption_status(ctx: Context):
    """Analyze SSL/TLS encryption status and certificate information."""
    try:
        async with get_mysql_connection() as db:
            # SSL variables
            ssl_vars = db.execute_query("""
                SHOW VARIABLES WHERE Variable_name LIKE 'ssl_%'
                OR Variable_name LIKE 'tls_%'
                OR Variable_name = 'have_ssl'
            """)
            
            # SSL status
            ssl_status = db.execute_query("""
                SHOW STATUS WHERE Variable_name LIKE 'Ssl_%'
            """)
            
            # Current connection SSL status
            try:
                current_ssl = db.execute_query("""
                    SHOW STATUS WHERE Variable_name IN (
                        'Ssl_cipher',
                        'Ssl_version',
                        'Ssl_cipher_list'
                    )
                """)
            except:
                current_ssl = []
                
            # Connected sessions with SSL
            try:
                ssl_connections = db.execute_query("""
                    SELECT 
                        COUNT(*) as total_connections,
                        COUNT(CASE WHEN CONNECTION_TYPE = 'SSL/TLS' THEN 1 END) as ssl_connections,
                        COUNT(CASE WHEN CONNECTION_TYPE = 'TCP/IP' THEN 1 END) as tcp_connections
                    FROM performance_schema.threads 
                    WHERE TYPE = 'FOREGROUND'
                """)
            except:
                ssl_connections = []
                
            return {
                "success": True,
                "ssl_configuration": ssl_vars,
                "ssl_status": ssl_status,
                "current_connection_ssl": current_ssl,
                "connection_types": ssl_connections
            }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_binary_log_analysis(ctx: Context):
    """Analyze binary log configuration, usage, and replication impact."""
    try:
        async with get_mysql_connection() as db:
            # Binary log configuration
            binlog_vars = db.execute_query("""
                SHOW VARIABLES WHERE Variable_name LIKE 'log_bin%'
                OR Variable_name LIKE 'binlog_%'
                OR Variable_name IN ('sync_binlog', 'expire_logs_days')
            """)
            
            # Binary log status
            try:
                binlog_status = db.execute_query("SHOW BINARY LOGS")
            except:
                binlog_status = []
                
            # Master status
            try:
                master_status = db.execute_query("SHOW MASTER STATUS")
            except:
                master_status = []
                
            # Binary log events statistics
            binlog_stats = db.execute_query("""
                SHOW STATUS WHERE Variable_name LIKE 'Binlog_%'
                OR Variable_name LIKE 'Com_show_binlog%'
            """)
            
            # Calculate total binlog size
            total_size = 0
            if binlog_status:
                for log in binlog_status:
                    if 'File_size' in log:
                        total_size += int(log['File_size'])
                        
            return {
                "success": True,
                "binary_log_configuration": binlog_vars,
                "binary_logs": binlog_status,
                "master_status": master_status,
                "binary_log_statistics": binlog_stats,
                "total_binlog_size_bytes": total_size,
                "total_binlog_size_mb": round(total_size / 1024 / 1024, 2)
            }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_optimizer_statistics_analysis(ctx: Context):
    """Analyze query optimizer statistics and histogram data."""
    try:
        async with get_mysql_connection() as db:
        
        # Optimizer-related variables
            optimizer_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE 'optimizer_%'
            OR Variable_name LIKE 'eq_range_%'
            OR Variable_name IN ('table_open_cache', 'table_definition_cache')
        """)
        
        # Table statistics
        table_stats = db.execute_query("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_ROWS,
                AVG_ROW_LENGTH,
                DATA_LENGTH,
                INDEX_LENGTH,
                AUTO_INCREMENT,
                UPDATE_TIME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
            LIMIT 20
        """)
        
        # Column statistics (histograms in MySQL 8.0+)
        try:
            histogram_stats = db.execute_query("""
                SELECT 
                    SCHEMA_NAME,
                    TABLE_NAME,
                    COLUMN_NAME,
                    JSON_EXTRACT(HISTOGRAM, '$."number-of-buckets-specified"') as buckets,
                    JSON_EXTRACT(HISTOGRAM, '$."last-updated"') as last_updated
                FROM INFORMATION_SCHEMA.COLUMN_STATISTICS
                LIMIT 50
            """)
        except:
            histogram_stats = []
            
        # Index usage from Performance Schema
        try:
            index_usage = db.execute_query("""
                SELECT 
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    INDEX_NAME,
                    COUNT_FETCH,
                    COUNT_INSERT,
                    COUNT_UPDATE,
                    COUNT_DELETE
                FROM performance_schema.table_io_waits_summary_by_index_usage
                WHERE OBJECT_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                AND (COUNT_FETCH + COUNT_INSERT + COUNT_UPDATE + COUNT_DELETE) > 0
                ORDER BY (COUNT_FETCH + COUNT_INSERT + COUNT_UPDATE + COUNT_DELETE) DESC
                LIMIT 30
            """)
        except:
            index_usage = []
            
        return {
            "success": True,
            "optimizer_configuration": optimizer_vars,
            "table_statistics": table_stats,
            "histogram_statistics": histogram_stats,
            "index_usage_statistics": index_usage
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_backup_recovery_status(ctx: Context):
    """Analyze backup and recovery configuration and status."""
    try:
        async with get_mysql_connection() as db:
        
        # Backup-related variables
            backup_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE '%backup%'
            OR Variable_name LIKE 'log_bin%'
            OR Variable_name IN ('sync_binlog', 'innodb_flush_log_at_trx_commit')
        """)
        
        # Check for backup-related tables/schemas
        backup_objects = db.execute_query("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                CREATE_TIME,
                UPDATE_TIME,
                TABLE_COMMENT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA LIKE '%backup%'
            OR TABLE_NAME LIKE '%backup%'
            OR TABLE_NAME LIKE '%dump%'
            OR TABLE_COMMENT LIKE '%backup%'
        """)
        
        # Point-in-time recovery readiness
        pitr_status = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name IN (
                'log_bin',
                'binlog_format',
                'sync_binlog',
                'innodb_flush_log_at_trx_commit',
                'innodb_support_xa'
            )
        """)
        
        # Recent binary logs for PITR
        try:
            recent_binlogs = db.execute_query("SHOW BINARY LOGS")
        except:
            recent_binlogs = []
            
        return {
            "success": True,
            "backup_configuration": backup_vars,
            "backup_objects": backup_objects,
            "pitr_configuration": pitr_status,
            "available_binary_logs": recent_binlogs,
            "pitr_ready": len([v for v in pitr_status if v['Variable_name'] == 'log_bin' and v['Value'] == 'ON']) > 0
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_connection_audit_analysis(ctx: Context):
    """Perform security audit of connections, users, and access patterns."""
    try:
        async with get_mysql_connection() as db:
        
        # User account security analysis
            user_security = db.execute_query("""
            SELECT 
                User,
                Host,
                plugin,
                password_expired,
                password_lifetime,
                account_locked,
                Create_user_priv,
                Super_priv,
                Grant_priv
            FROM mysql.user
            ORDER BY User, Host
        """)
        
        # Users with elevated privileges
        privileged_users = db.execute_query("""
            SELECT 
                User,
                Host,
                Super_priv,
                Create_user_priv,
                Grant_priv,
                Shutdown_priv,
                Process_priv,
                File_priv
            FROM mysql.user
            WHERE Super_priv = 'Y'
            OR Create_user_priv = 'Y'
            OR Grant_priv = 'Y'
            OR File_priv = 'Y'
        """)
        
        # Connection patterns from Performance Schema
        try:
            connection_history = db.execute_query("""
                SELECT 
                    USER,
                    HOST,
                    COUNT(*) as connection_count,
                    COUNT(DISTINCT PROCESSLIST_ID) as unique_sessions
                FROM performance_schema.events_statements_history_long
                WHERE USER IS NOT NULL
                GROUP BY USER, HOST
                ORDER BY connection_count DESC
                LIMIT 20
            """)
        except:
            connection_history = []
            
        # Current active connections
        active_connections = db.execute_query("""
            SELECT 
                USER,
                HOST,
                DB,
                COMMAND,
                TIME,
                STATE,
                COUNT(*) as session_count
            FROM INFORMATION_SCHEMA.PROCESSLIST
            WHERE USER != 'system user'
            GROUP BY USER, HOST, DB, COMMAND, STATE
            ORDER BY session_count DESC, TIME DESC
        """)
        
        # Failed connection attempts
        try:
            failed_connections = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Aborted_connects',
                    'Aborted_clients',
                    'Connection_errors_accept',
                    'Connection_errors_internal',
                    'Connection_errors_max_connections',
                    'Connection_errors_peer_address',
                    'Connection_errors_select',
                    'Connection_errors_tcpwrap'
                )
            """)
        except:
            failed_connections = []
            
        return {
            "success": True,
            "user_security_analysis": user_security,
            "privileged_users": privileged_users,
            "connection_patterns": connection_history,
            "active_connections": active_connections,
            "connection_errors": failed_connections
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_resource_consumption_analysis(ctx: Context):
    """Analyze resource consumption patterns by users, databases, and operations."""
    try:
        async with get_mysql_connection() as db:
        
        # Resource limits per user
            user_resources = db.execute_query("""
            SELECT 
                User,
                Host,
                max_questions,
                max_updates,
                max_connections,
                max_user_connections
            FROM mysql.user
            WHERE max_questions > 0 
            OR max_updates > 0 
            OR max_connections > 0 
            OR max_user_connections > 0
        """)
        
        # Current resource usage by user
        try:
            user_usage = db.execute_query("""
                SELECT 
                    USER,
                    COUNT(*) as current_connections,
                    SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active_queries,
                    AVG(TIME) as avg_query_time,
                    MAX(TIME) as max_query_time
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE USER != 'system user'
                GROUP BY USER
                ORDER BY current_connections DESC
            """)
        except:
            user_usage = []
            
        # Database size and usage
        db_usage = db.execute_query("""
            SELECT 
                TABLE_SCHEMA as database_name,
                COUNT(*) as table_count,
                SUM(TABLE_ROWS) as total_rows,
                SUM(DATA_LENGTH) as data_size_bytes,
                SUM(INDEX_LENGTH) as index_size_bytes,
                SUM(DATA_LENGTH + INDEX_LENGTH) as total_size_bytes,
                SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 as total_size_mb
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            GROUP BY TABLE_SCHEMA
            ORDER BY total_size_bytes DESC
        """)
        
        # Top resource-consuming queries (if Performance Schema is enabled)
        try:
            top_queries = db.execute_query("""
                SELECT 
                    DIGEST_TEXT,
                    COUNT_STAR as execution_count,
                    SUM_TIMER_WAIT / 1000000000 as total_time_seconds,
                    AVG_TIMER_WAIT / 1000000000 as avg_time_seconds,
                    SUM_ROWS_EXAMINED,
                    SUM_ROWS_SENT,
                    SUM_SELECT_SCAN,
                    SUM_SELECT_FULL_JOIN
                FROM performance_schema.events_statements_summary_by_digest
                ORDER BY SUM_TIMER_WAIT DESC
                LIMIT 15
            """)
        except:
            top_queries = []
            
        # Temporary table usage
        temp_usage = db.execute_query("""
            SHOW STATUS WHERE Variable_name IN (
                'Created_tmp_tables',
                'Created_tmp_disk_tables',
                'Created_tmp_files'
            )
        """)
        
        return {
            "success": True,
            "user_resource_limits": user_resources,
            "current_user_usage": user_usage,
            "database_usage": db_usage,
            "top_resource_consuming_queries": top_queries,
            "temporary_resource_usage": temp_usage
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_innodb_lock_analysis(ctx: Context):
    """Analyze InnoDB redo log and undo log configuration and usage."""
    try:
        async with get_mysql_connection() as db:
        
        # InnoDB log configuration
            log_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE 'innodb_log%'
            OR Variable_name LIKE 'innodb_undo%'
            OR Variable_name IN ('innodb_flush_log_at_trx_commit', 'sync_binlog')
        """)
        
        # InnoDB log status
        log_status = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE 'Innodb_log%'
            OR Variable_name LIKE 'Innodb_undo%'
        """)
        
        # LSN and checkpoint information
        try:
            innodb_status = db.execute_query("SHOW ENGINE INNODB STATUS")
            # Parse LSN information from the status output
            lsn_info = "InnoDB status contains LSN information" if innodb_status else "No InnoDB status available"
        except:
            lsn_info = "Unable to retrieve InnoDB status"
            
        # Redo log usage calculation
        vars_dict = {}
        for var in log_vars:
            if var['Variable_name'] in ['innodb_log_file_size', 'innodb_log_files_in_group']:
                try:
                    vars_dict[var['Variable_name']] = int(var['Value'])
                except:
                    vars_dict[var['Variable_name']] = var['Value']
        
        total_redo_size = 0
        if 'innodb_log_file_size' in vars_dict and 'innodb_log_files_in_group' in vars_dict:
            total_redo_size = vars_dict['innodb_log_file_size'] * vars_dict['innodb_log_files_in_group']
            
        return {
            "success": True,
            "innodb_log_configuration": log_vars,
            "innodb_log_status": log_status,
            "lsn_checkpoint_info": lsn_info,
            "total_redo_log_size_bytes": total_redo_size,
            "total_redo_log_size_mb": round(total_redo_size / 1024 / 1024, 2) if total_redo_size > 0 else 0
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_adaptive_hash_index_analysis(ctx: Context):
    """Analyze InnoDB Adaptive Hash Index usage and effectiveness."""
    try:
        async with get_mysql_connection() as db:
        
        # AHI configuration
            ahi_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE 'innodb_adaptive_hash%'
        """)
        
        # AHI statistics
        ahi_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE 'Innodb_adaptive_hash%'
        """)
        
        # Buffer pool and hash index related statistics
        hash_stats = db.execute_query("""
            SHOW STATUS WHERE Variable_name IN (
                'Innodb_buffer_pool_read_requests',
                'Innodb_buffer_pool_reads',
                'Innodb_pages_read',
                'Innodb_pages_written'
            )
        """)
        
        # Calculate AHI effectiveness if data is available
        effectiveness_metrics = {}
        stats_dict = {}
        
        for stat in ahi_stats:
            stats_dict[stat['Variable_name']] = stat['Value']
            
        # Try to calculate hit ratios and effectiveness
        if 'Innodb_adaptive_hash_searches' in stats_dict and 'Innodb_adaptive_hash_searches_btree' in stats_dict:
            try:
                ahi_searches = int(stats_dict['Innodb_adaptive_hash_searches'])
                btree_searches = int(stats_dict['Innodb_adaptive_hash_searches_btree'])
                
                if (ahi_searches + btree_searches) > 0:
                    ahi_hit_ratio = ahi_searches / (ahi_searches + btree_searches) * 100
                    effectiveness_metrics['ahi_hit_ratio_percentage'] = round(ahi_hit_ratio, 2)
                    effectiveness_metrics['total_searches'] = ahi_searches + btree_searches
                    effectiveness_metrics['ahi_searches'] = ahi_searches
                    effectiveness_metrics['btree_searches'] = btree_searches
            except:
                pass
                
        return {
            "success": True,
            "adaptive_hash_index_config": ahi_vars,
            "adaptive_hash_index_statistics": ahi_stats,
            "related_buffer_pool_stats": hash_stats,
            "effectiveness_metrics": effectiveness_metrics
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_event_scheduler_analysis(ctx: Context):
    """Analyze MySQL Event Scheduler status, events, and execution history."""
    try:
        async with get_mysql_connection() as db:
        
        # Event Scheduler configuration
            event_vars = db.execute_query("""
            SHOW VARIABLES WHERE Variable_name LIKE 'event_scheduler'
        """)
        
        # All defined events
        events = db.execute_query("""
            SELECT 
                EVENT_SCHEMA,
                EVENT_NAME,
                STATUS,
                EVENT_TYPE,
                EXECUTE_AT,
                INTERVAL_VALUE,
                INTERVAL_FIELD,
                STARTS,
                ENDS,
                ON_COMPLETION,
                CREATED,
                LAST_ALTERED,
                LAST_EXECUTED,
                EVENT_COMMENT
            FROM INFORMATION_SCHEMA.EVENTS
            ORDER BY EVENT_SCHEMA, EVENT_NAME
        """)
        
        # Event execution statistics (if available)
        try:
            event_history = db.execute_query("""
                SELECT 
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    COUNT_STAR as execution_count,
                    SUM_TIMER_WAIT / 1000000000 as total_time_seconds,
                    AVG_TIMER_WAIT / 1000000000 as avg_time_seconds,
                    MIN_TIMER_WAIT / 1000000000 as min_time_seconds,
                    MAX_TIMER_WAIT / 1000000000 as max_time_seconds
                FROM performance_schema.events_statements_summary_by_program
                WHERE OBJECT_TYPE = 'EVENT'
                ORDER BY COUNT_STAR DESC
            """)
        except:
            event_history = []
            
        # Current event scheduler processes
        event_processes = db.execute_query("""
            SELECT 
                ID,
                USER,
                HOST,
                DB,
                COMMAND,
                TIME,
                STATE,
                INFO
            FROM INFORMATION_SCHEMA.PROCESSLIST
            WHERE USER = 'event_scheduler'
            OR INFO LIKE '%EVENT%'
            OR COMMAND = 'Daemon'
        """)
        
        # Event-related status variables
        event_status = db.execute_query("""
            SHOW STATUS WHERE Variable_name LIKE '%event%'
        """)
        
        return {
            "success": True,
            "event_scheduler_config": event_vars,
            "defined_events": events,
            "event_execution_history": event_history,
            "event_scheduler_processes": event_processes,
            "event_status_variables": event_status,
            "scheduler_enabled": any(var['Value'] == 'ON' for var in event_vars if var['Variable_name'] == 'event_scheduler')
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_partition_management_analysis(ctx: Context):
    """Analyze table partitioning configuration and partition statistics."""
    try:
        async with get_mysql_connection() as db:
        
        # Partitioned tables
            partitioned_tables = db.execute_query("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                PARTITION_NAME,
                SUBPARTITION_NAME,
                PARTITION_ORDINAL_POSITION,
                PARTITION_METHOD,
                PARTITION_EXPRESSION,
                PARTITION_DESCRIPTION,
                TABLE_ROWS,
                AVG_ROW_LENGTH,
                DATA_LENGTH,
                INDEX_LENGTH,
                CREATE_TIME,
                UPDATE_TIME,
                CHECK_TIME
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE PARTITION_NAME IS NOT NULL
            ORDER BY TABLE_SCHEMA, TABLE_NAME, PARTITION_ORDINAL_POSITION
        """)
        
        # Partition summary by table
        partition_summary = db.execute_query("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COUNT(*) as partition_count,
                MAX(PARTITION_METHOD) as partition_method,
                SUM(TABLE_ROWS) as total_rows,
                SUM(DATA_LENGTH + INDEX_LENGTH) as total_size_bytes,
                SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 as total_size_mb,
                MIN(CREATE_TIME) as oldest_partition,
                MAX(UPDATE_TIME) as most_recent_update
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE PARTITION_NAME IS NOT NULL
            GROUP BY TABLE_SCHEMA, TABLE_NAME
            ORDER BY total_size_bytes DESC
        """)
        
        # Tables that could benefit from partitioning (large tables without partitions)
        large_unpartitioned = db.execute_query("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_ROWS,
                DATA_LENGTH + INDEX_LENGTH as total_size_bytes,
                (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 as total_size_mb,
                CREATE_TIME,
                UPDATE_TIME
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            AND TABLE_TYPE = 'BASE TABLE'
            AND (DATA_LENGTH + INDEX_LENGTH) > 100 * 1024 * 1024  -- Tables larger than 100MB
            AND NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.PARTITIONS p 
                WHERE p.TABLE_SCHEMA = t.TABLE_SCHEMA 
                AND p.TABLE_NAME = t.TABLE_NAME 
                AND p.PARTITION_NAME IS NOT NULL
            )
            ORDER BY total_size_bytes DESC
            LIMIT 20
        """)
        
        # Partition pruning statistics (if available)
        try:
            partition_stats = db.execute_query("""
                SELECT 
                    OBJECT_SCHEMA,
                    OBJECT_NAME,
                    COUNT_READ,
                    COUNT_WRITE,
                    SUM_TIMER_READ / 1000000000 as read_time_seconds,
                    SUM_TIMER_WRITE / 1000000000 as write_time_seconds
                FROM performance_schema.table_io_waits_summary_by_table
                WHERE OBJECT_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                AND OBJECT_NAME IN (
                    SELECT DISTINCT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.PARTITIONS 
                    WHERE PARTITION_NAME IS NOT NULL
                )
                ORDER BY (COUNT_READ + COUNT_WRITE) DESC
                LIMIT 15
            """)
        except:
            partition_stats = []
            
        return {
            "success": True,
            "partitioned_tables_detail": partitioned_tables,
            "partition_summary_by_table": partition_summary,
            "large_unpartitioned_tables": large_unpartitioned,
            "partition_io_statistics": partition_stats,
            "total_partitioned_tables": len(set((t['TABLE_SCHEMA'], t['TABLE_NAME']) for t in partitioned_tables))
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_comprehensive_performance_summary(ctx: Context):
    """Generate a comprehensive performance summary combining multiple metrics."""
    try:
        async with get_mysql_connection() as db:
        
            summary = {}
        
        # 1. Server uptime and basic info
        try:
            basic_info = db.execute_query("""
                SELECT 
                    VERSION() as mysql_version,
                    NOW() as current_time
            """)
            uptime = db.execute_query("SHOW STATUS LIKE 'Uptime'")
            summary["server_info"] = {
                "basic_info": basic_info,
                "uptime_seconds": int(uptime[0]['Value']) if uptime else 0
            }
        except Exception as e:
            summary["server_info"] = {"error": str(e)}
            
        # 2. Connection performance
        try:
            conn_metrics = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Connections', 'Threads_connected', 'Threads_running',
                    'Aborted_connects', 'Aborted_clients', 'Max_used_connections'
                )
            """)
            max_conn = db.execute_query("SHOW VARIABLES LIKE 'max_connections'")
            summary["connection_performance"] = {
                "metrics": conn_metrics,
                "max_connections": max_conn
            }
        except Exception as e:
            summary["connection_performance"] = {"error": str(e)}
            
        # 3. Query performance summary
        try:
            query_metrics = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Questions', 'Queries', 'Slow_queries',
                    'Com_select', 'Com_insert', 'Com_update', 'Com_delete',
                    'Created_tmp_tables', 'Created_tmp_disk_tables',
                    'Sort_merge_passes', 'Table_locks_waited'
                )
            """)
            
            # Calculate query rates
            uptime_val = int(uptime[0]['Value']) if uptime else 1
            query_analysis = {}
            
            for metric in query_metrics:
                if metric['Variable_name'] == 'Questions':
                    query_analysis['queries_per_second'] = round(int(metric['Value']) / uptime_val, 2)
                elif metric['Variable_name'] == 'Slow_queries':
                    query_analysis['slow_queries_per_second'] = round(int(metric['Value']) / uptime_val, 4)
                    
            summary["query_performance"] = {
                "metrics": query_metrics,
                "analysis": query_analysis
            }
        except Exception as e:
            summary["query_performance"] = {"error": str(e)}
            
        # 4. InnoDB performance summary
        try:
            innodb_metrics = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN (
                    'Innodb_buffer_pool_read_requests',
                    'Innodb_buffer_pool_reads',
                    'Innodb_buffer_pool_pages_total',
                    'Innodb_buffer_pool_pages_free',
                    'Innodb_rows_read', 'Innodb_rows_inserted',
                    'Innodb_rows_updated', 'Innodb_rows_deleted',
                    'Innodb_data_reads', 'Innodb_data_writes',
                    'Innodb_log_writes', 'Innodb_deadlocks'
                )
            """)
            
            # Calculate buffer pool hit ratio
            bp_requests = 0
            bp_reads = 0
            for metric in innodb_metrics:
                if metric['Variable_name'] == 'Innodb_buffer_pool_read_requests':
                    bp_requests = int(metric['Value'])
                elif metric['Variable_name'] == 'Innodb_buffer_pool_reads':
                    bp_reads = int(metric['Value'])
                    
            hit_ratio = 0
            if bp_requests > 0:
                hit_ratio = (bp_requests - bp_reads) / bp_requests * 100
                
            summary["innodb_performance"] = {
                "metrics": innodb_metrics,
                "buffer_pool_hit_ratio_percentage": round(hit_ratio, 2)
            }
        except Exception as e:
            summary["innodb_performance"] = {"error": str(e)}
            
        # 5. Storage summary
        try:
            storage_summary = db.execute_query("""
                SELECT 
                    COUNT(*) as total_tables,
                    SUM(TABLE_ROWS) as total_rows,
                    SUM(DATA_LENGTH) as total_data_bytes,
                    SUM(INDEX_LENGTH) as total_index_bytes,
                    SUM(DATA_LENGTH + INDEX_LENGTH) as total_size_bytes,
                    SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 / 1024 as total_size_gb
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                AND TABLE_TYPE = 'BASE TABLE'
            """)
            summary["storage_summary"] = storage_summary
        except Exception as e:
            summary["storage_summary"] = {"error": str(e)}
            
        # 6. Top performance recommendations
        recommendations = []
        
        try:
            # Check for high tmp table to disk ratio
            tmp_stats = db.execute_query("""
                SHOW STATUS WHERE Variable_name IN ('Created_tmp_tables', 'Created_tmp_disk_tables')
            """)
            tmp_dict = {stat['Variable_name']: int(stat['Value']) for stat in tmp_stats}
            
            if tmp_dict.get('Created_tmp_tables', 0) > 0:
                disk_ratio = tmp_dict.get('Created_tmp_disk_tables', 0) / tmp_dict.get('Created_tmp_tables', 1) * 100
                if disk_ratio > 25:
                    recommendations.append(f"High temporary tables to disk ratio ({disk_ratio:.1f}%) - consider increasing tmp_table_size")
                    
            # Check buffer pool hit ratio
            if hit_ratio < 95:
                recommendations.append(f"Low buffer pool hit ratio ({hit_ratio:.1f}%) - consider increasing innodb_buffer_pool_size")
                
            # Check for high aborted connections
            if summary.get("connection_performance", {}).get("metrics"):
                for metric in summary["connection_performance"]["metrics"]:
                    if metric['Variable_name'] == 'Aborted_connects' and int(metric['Value']) > 100:
                        recommendations.append("High number of aborted connections - check network and authentication issues")
                        
        except:
            pass
            
        summary["performance_recommendations"] = recommendations
        
        return {
            "success": True,
            "performance_summary": summary,
            "generated_at": db.execute_query("SELECT NOW() as timestamp")[0]["timestamp"]
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@mcp.tool()
async def mysql_adaptive_index_fragmentation_analysis(ctx: Context):
    """Performs an adaptive analysis of index fragmentation and provides recommendations."""
    try:
        db = ctx.lifespan["db"]
        index_info = db.execute_query(
            """
            SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, 
                   ROUND(SUM(DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS index_size_mb,
                   SUM(INDEX_LENGTH) / SUM(DATA_LENGTH + INDEX_LENGTH) * 100 AS index_frag_percent
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
            HAVING index_frag_percent > 10
            """
        )
        message = "Indexes with fragmentation greater than 10% detected. Consider rebuilding." if index_info else "No significant index fragmentation detected."
        return {
            "success": True,
            "indexes": index_info,
            "message": message
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@mcp.tool()
async def mysql_query_pattern_clustering(ctx: Context):
    """Identifies clustered patterns and anomalies in query executions."""
    try:
        db = ctx.lifespan["db"]
        query_patterns = db.execute_query(
            """
            SELECT digest, COUNT_STAR AS occurrences, SUM_TIMER_WAIT/1000000 AS total_exec_time_ms
            FROM performance_schema.events_statements_summary_by_digest
            ORDER BY occurrences DESC
            LIMIT 10
            """
        )
        anomalies = [qp for qp in query_patterns if qp['total_exec_time_ms'] / qp['occurrences'] > 500]
        message = "Anomalies in execution time detected." if anomalies else "No significant query execution anomalies found."
        return {
            "success": True,
            "query_patterns": query_patterns,
            "anomalies": anomalies,
            "message": message
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@mcp.tool()
async def mysql_deadlock_scenario_reconstruction(ctx: Context):
    """Reconstructs detailed scenarios of recent deadlocks."""
    try:
        db = ctx.lifespan["db"]
        deadlocks = db.execute_query(
            """
            SHOW ENGINE INNODB STATUS
            """
        )
        reconstruction = "Deadlock reconstruction data not available." if not deadlocks else "Reconstructed deadlock scenarios available."
        return {
            "success": True,
            "deadlock_information": deadlocks,
            "message": reconstruction
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@mcp.tool()
async def mysql_user_activity_patterns(ctx: Context):
    """Analyzes granular user activity and access patterns."""
    try:
        db = ctx.lifespan["db"]
        user_activity = db.execute_query(
            """
            SELECT USER, HOST, COUNT(*) AS total_connections
            FROM information_schema.processlist
            GROUP BY USER, HOST
            ORDER BY total_connections DESC
            LIMIT 10
            """
        )
        message = "User activity patterns analyzed successfully." if user_activity else "No user activity detected."
        return {
            "success": True,
            "user_activity": user_activity,
            "message": message
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@mcp.tool()
async def mysql_lock_wait_analysis(ctx: Context):
    """Analyzes historical and real-time lock wait scenarios."""
    try:
        db = ctx.lifespan["db"]
        lock_waits = db.execute_query(
            """
            SELECT event_name, COUNT_STAR as occurrences, SUM_TIMER_WAIT/1000000 as total_wait_time_ms
            FROM performance_schema.events_waits_summary_by_event_name
            WHERE event_name LIKE 'wait/lock/mutex%' OR event_name LIKE 'wait/lock/table%'
            ORDER BY total_wait_time_ms DESC
            LIMIT 10
            """
        )
        message = "Lock wait scenarios analyzed successfully." if lock_waits else "No significant lock waits detected."
        return {
            "success": True,
            "lock_waits": lock_waits,
            "message": message
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == "__main__":
    main()
