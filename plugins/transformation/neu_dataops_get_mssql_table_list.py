from airflow.providers.odbc.hooks.odbc import OdbcHook


def get_mssql_table_list(mssql_conn_id, database):
    hook = OdbcHook(odbc_conn_id=mssql_conn_id, database=database)

    sql = "select schema_name(tab.schema_id) as [schema_name]," \
            "tab.[name] as table_name," \
            "substring(column_names, 1, len(column_names)-1) as [pk] " \
            "from sys.tables tab " \
            "left outer join sys.indexes pk " \
            "on tab.object_id = pk.object_id " \
            "and pk.is_primary_key = 1 " \
            "cross apply (select col.[name] + ', ' " \
                    "from sys.index_columns ic " \
                        "inner join sys.columns col on ic.object_id = col.object_id and ic.column_id = col.column_id " \
                    "where ic.object_id = tab.object_id and ic.index_id = pk.index_id order by col.column_id " \
                            "for xml path ('') ) D (column_names) " \
            "order by schema_name(tab.schema_id),tab.[name]"
    df = hook.get_pandas_df(sql=sql, parameters=None)
    return df