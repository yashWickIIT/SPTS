from functools import lru_cache

from sqlalchemy import MetaData, Table, create_engine, func, inspect, select, text

try:
    from .config import get_main_database_url
except ImportError:
    from config import get_main_database_url


@lru_cache(maxsize=1)
def get_main_engine():
    return create_engine(get_main_database_url(), pool_pre_ping=True)


def get_main_dialect_name() -> str:
    return get_main_engine().dialect.name


def execute_raw_sql(sql: str):
    engine = get_main_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        if result.returns_rows:
            return [tuple(row) for row in result.fetchall()]
        conn.commit()
        return []


def list_user_tables() -> list[str]:
    inspector = inspect(get_main_engine())
    return inspector.get_table_names()


def get_table_columns(table_name: str) -> list[dict]:
    inspector = inspect(get_main_engine())
    return inspector.get_columns(table_name)


def get_table_foreign_keys(table_name: str) -> list[dict]:
    inspector = inspect(get_main_engine())
    return inspector.get_foreign_keys(table_name)


def is_textual_column_type(type_obj) -> bool:
    type_name = str(type_obj).upper()
    return any(
        marker in type_name
        for marker in ["CHAR", "TEXT", "CLOB", "STRING", "VARCHAR", "NCHAR", "NVARCHAR"]
    )


def _reflect_table(table_name: str) -> Table:
    engine = get_main_engine()
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def count_distinct_non_null(table_name: str, column_name: str) -> int:
    table = _reflect_table(table_name)
    column = table.c[column_name]
    stmt = select(func.count(column.distinct())).where(column.is_not(None))
    with get_main_engine().connect() as conn:
        value = conn.execute(stmt).scalar_one()
    return int(value or 0)


def fetch_distinct_non_null_values(table_name: str, column_name: str, limit: int) -> list:
    table = _reflect_table(table_name)
    column = table.c[column_name]
    stmt = select(column).where(column.is_not(None)).distinct().limit(limit)
    with get_main_engine().connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [row[0] for row in rows]
