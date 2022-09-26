"""mssql target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import SQLConnector, SQLSink
import sqlalchemy

class mssqlConnector(SQLConnector):
    """The connector for mssql.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for mssql.

        Args:
            config: The configuration for the connector.
        """
        return f"mssql+pyodbc://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?driver=ODBC+Driver+17+for+SQL+Server"

        #return f"mssql+pymssql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

    def _create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.
        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            raise NotImplementedError("Adding columns is not supported.")

        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                sql_type,
            )
        )
        self.connection.execute(
            sqlalchemy.DDL(
                "ALTER TABLE %(table)s ADD %(create_column)s",
                {
                    "table": full_table_name,
                    "create_column": create_column_clause,
                },
            )
        )

class mssqlSink(SQLSink):
    """mssql target sink class."""

    connector_class = mssqlConnector
