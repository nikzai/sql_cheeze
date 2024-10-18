DROP PROCEDURE IF EXISTS db_tool.insert_with_null(full_table_name text);

CREATE OR REPLACE PROCEDURE db_tool.insert_with_null(full_table_name text)
LANGUAGE plpgsql
AS $$
DECLARE
    column_list     TEXT;
    column_data     TEXT;
    col             TEXT;
    insert_query    TEXT;
    i_schema_name   TEXT;
    i_table_name    TEXT;
BEGIN
    i_schema_name := split_part(full_table_name, '.', 1);
    i_table_name := split_part(full_table_name, '.', 2);
    
    column_list := (
        SELECT string_agg(cols.column_name, ',')
          FROM information_schema.columns cols
         WHERE cols.table_schema = i_schema_name
           AND cols.table_name = i_table_name  
    );
    
    FOR col IN SELECT unnest(string_to_array(column_list, ','))
    LOOP
        column_data := (
            SELECT string_agg(
                    CASE
                        WHEN cols.column_name = col THEN 'NULL'
                        WHEN cols.data_type = 'integer' THEN FLOOR(RANDOM() * 1000)::VARCHAR
                        WHEN cols.data_type = 'numeric' THEN ROUND((RANDOM() * 1000)::numeric, 2)::VARCHAR
                        WHEN cols.data_type = 'date' THEN '''' || (DATE '2020-01-01' + (RANDOM() * (DATE '2024-01-01' - DATE '2020-01-01')) * INTERVAL '1 day')::VARCHAR || ''''
                        WHEN cols.data_type = 'timestamp without time zone' THEN '''' || (NOW() + (RANDOM() * 365) * INTERVAL '1 day')::VARCHAR || ''''
                        WHEN cols.data_type = 'boolean' THEN (RANDOM() > 0.5)::VARCHAR
                        WHEN cols.data_type = 'character varying' THEN '''' || SUBSTRING(md5(RANDOM()::text) FROM 1 FOR 10)|| ''''
                        WHEN cols.data_type = 'text' THEN '''' ||md5(RANDOM()::text) || ''''
                    END, ', '
                    ORDER BY cols.ordinal_position
                   )
              FROM information_schema.columns cols
             WHERE cols.table_schema = i_schema_name
               AND cols.table_name = i_table_name
        );
    
        insert_query := format('
            INSERT INTO %s.%s (%s)
            SELECT %s',
            i_schema_name,
            i_table_name,
            column_list,
            column_data 
        );

        EXECUTE insert_query;
    
    END LOOP;
END;
$$;
