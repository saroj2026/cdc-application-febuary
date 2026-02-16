"""
Add missing values to PostgreSQL enum type used by ConnectionModel.database_type.

Fixes errors like:
  psycopg2.errors.InvalidTextRepresentation: invalid input value for enum databasetype: "snowflake"
"""

from __future__ import annotations

from sqlalchemy import text

try:
    from ingestion.database.session import engine
except Exception as e:
    raise SystemExit(f"Failed to import engine: {e}")


def add_enum_value_if_missing(enum_type: str, value: str) -> None:
    """
    Add a value to a PostgreSQL enum type if it does not already exist.
    Uses a DO block for compatibility across PostgreSQL versions.
    """
    sql = text(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                WHERE t.typname = :enum_type
                  AND e.enumlabel = :enum_value
            ) THEN
                EXECUTE format('ALTER TYPE %I ADD VALUE %L', :enum_type, :enum_value);
            END IF;
        END $$;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"enum_type": enum_type, "enum_value": value})


def main() -> None:
    # The enum type name in DB as indicated by error: "databasetype"
    enum_type_name = "databasetype"

    # Ensure required enum values exist
    for v in ("aws_s3", "snowflake"):
        add_enum_value_if_missing(enum_type_name, v)

    print("✅ Ensured enum 'databasetype' has values: aws_s3, snowflake")


if __name__ == "__main__":
    main()



