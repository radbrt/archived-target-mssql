from __future__ import annotations

from singer_sdk.sinks import SQLConnector, SQLSink
from sqlalchemy.dialects import mssql
from typing import Any, Generic, Mapping, TypeVar, Union, cast
from singer_sdk.helpers._typing import (
    get_datelike_property_type
)
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
        #return f"mssql+pyodbc://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?driver=ODBC+Driver+17+for+SQL+Server"

        connection_url = sqlalchemy.engine.url.URL(
            drivername="mssql+pymssql",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"]
        )
        return str(connection_url)
        # return f"mssql+pymssql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"


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

            # In MSSQL, Primary keys can not be more than 900 bytes. Setting at 255
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

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.
        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name, column_name
        )

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type) == str(current_type):
            # Nothing to do
            return

        if not self.allow_column_alter:
            raise NotImplementedError(
                "Altering columns is not supported. "
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            )
        try:
            self.connection.execute(
                f"""ALTER TABLE { str(full_table_name) } 
                ALTER COLUMN { str(column_name) } { str(compatible_sql_type) }"""
            )
        except Exception as e:
            raise RuntimeError(
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            ) from e

        # self.connection.execute(
        #     sqlalchemy.DDL(
        #         "ALTER TABLE %(table)s ALTER COLUMN %(col_name)s %(col_type)s",
        #         {
        #             "table": full_table_name,
        #             "col_name": column_name,
        #             "col_type": compatible_sql_type,
        #         },
        #     )
        # )


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

        try:
            self.connection.execute(
                f"""ALTER TABLE { str(full_table_name) } 
                ADD { str(create_column_clause) } """
            )

        except Exception as e:
            raise RuntimeError(
                f"Could not create column '{create_column_clause}' "
                f"on table '{full_table_name}'."
            ) from e


    def _jsonschema_type_check(self, jsonschema_type: dict, type_check: tuple[str]) -> bool:
        """Return True if the jsonschema_type supports the provided type.
        Args:
            jsonschema_type: The type dict.
            type_check: A tuple of type strings to look for.
        Returns:
            True if the schema suports the type.
        """
        if "type" in jsonschema_type:
            if isinstance(jsonschema_type["type"], (list, tuple)):
                for t in jsonschema_type["type"]:
                    if t in type_check:
                        return True
            else:
                if jsonschema_type.get("type") in type_check:
                    return True

        if any(t in type_check for t in jsonschema_type.get("anyOf", ())):
            return True

        return False



    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Convert JSON Schema type to a SQL type.
        Args:
            jsonschema_type: The JSON Schema object.
        Returns:
            The SQL type.
        """
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATETIME())
                if datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
                if datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

            maxlength = jsonschema_type.get("maxLength")
            return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR(2000))

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.INTEGER())
        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NUMERIC(22, 16))
        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, mssql.VARCHAR(2000))

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR(2000))

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR(2000))

        return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR(2000))


    def create_temp_table_from_table(self, from_table_name):
        """Temp table from another table."""

        # ddl = sqlalchemy.DDL(
        #     """
        #     SELECT TOP 0 *
        #     into #%(from_table_name)s
        #     FROM %(from_table_name)s
        #     """,
        #     {"from_table_name": from_table_name},
        # )

        ddl = f"""
            SELECT TOP 0 *
            into {from_table_name}_tmp
            FROM {from_table_name}
        """

        self.connection.execute(ddl)

