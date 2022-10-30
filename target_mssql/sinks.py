"""mssql target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import SQLSink
from typing import Any, Optional, List, Dict

import sqlalchemy
from target_mssql.connector import mssqlConnector


class mssqlSink(SQLSink):
    """mssql target sink class."""

    connector_class = mssqlConnector

    # Copied purely to help with type hints
    @property
    def connector(self) -> mssqlConnector:
        """The connector object.
        Returns:
            The connector object.
        """
        return self._connector


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

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        # First we need to be sure the main table is already created
        self.connector.prepare_table(
            full_table_name=self.full_table_name,
            schema=self.schema,
            primary_keys=self.key_properties,
            as_temp_table=False,
        )
        # Create a temp table (Creates from the table above)
        self.connector.create_temp_table_from_table(
            from_table_name=self.full_table_name
        )
        # Insert into temp table
        self.bulk_insert_records(
            full_table_name=f"#{self.full_table_name}",
            schema=self.schema,
            records=context["records"],
        )
        # Merge data from Temp table to main table
        self.merge_upsert_from_table(
            from_table_name=f"#{self.full_table_name}",
            to_table_name=f"{self.full_table_name}",
            schema=self.schema,
            join_keys=self.key_properties,
        )
        self.connector.truncate_table(self.temp_table_name)


    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
    ) -> Optional[int]:
        """Merge upsert data from one table to another.
        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.
        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        # TODO think about sql injeciton,
        # issue here https://github.com/MeltanoLabs/target-postgres/issues/22


        # INSERT
        join_condition = " and ".join(
            [f"temp.{key} = target.{key}" for key in join_keys]
        )

        merge_sql = f"""
            MERGE INTO {to_table_name} AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    {", ".join([f"target.{key} = temp.{key}" for key in schema["properties"].keys() if key not in join_keys])}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(schema["properties"].keys())})
                VALUES ({", ".join([f"temp.{key}" for key in schema["properties"].keys()])})
        """

        self.connection.execute(merge_sql)
