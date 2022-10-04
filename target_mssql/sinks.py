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
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.


    def create_table_with_records(
        self,
        full_table_name: Optional[str],
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            records: records to load.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        full_table_name = full_table_name or self.full_table_name
        if primary_keys is None:
            primary_keys = self.key_properties
        partition_keys = partition_keys or None
        self.connector.prepare_table(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
            schema=schema,
            as_temp_table=as_temp_table,
        )
        self.bulk_insert_records(
            full_table_name=full_table_name, schema=schema, records=records
        )


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

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        meta = sqlalchemy.MetaData()
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
            )
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys

            columntype = self.to_sql_type(property_jsonschema)

            if isinstance(columntype, sqlalchemy.types.VARCHAR) and is_primary_key:
                columntype = sqlalchemy.types.VARCHAR(255)

            columns.append(
                sqlalchemy.Column(
                    property_name,
                    columntype,
                    primary_key=is_primary_key,
                )
            )

        _ = sqlalchemy.Table(full_table_name, meta, *columns)
        meta.create_all(self._engine)


class mssqlSink(SQLSink):
    """mssql target sink class."""

    connector_class = mssqlConnector

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.
        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )
        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        self.logger.info("Inserting with SQL: %s", insert_sql)
        # self.connection.execute(f"SET IDENTITY_INSERT { full_table_name } ON")
        # self.logger.info(f"Enabled identity insert on { full_table_name }")
        self.connector.connection.execute(insert_sql, records)
        # self.connection.execute(f"SET IDENTITY_INSERT { full_table_name } OFF")
        # self.logger.info(f"Disabled identity insert on { full_table_name }")

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.
