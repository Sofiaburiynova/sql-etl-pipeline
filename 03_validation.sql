
SELECT 
    'branch' AS table_name, 
    md5(string_agg(row_to_json(t.*)::text, '' ORDER BY branch_id)) AS table_hash,
    count(*) AS row_count
FROM core.branch t;


SELECT 
    'operation_type' AS table_name, 
    md5(string_agg(row_to_json(t.*)::text, '' ORDER BY operation_type_id)) AS table_hash,
    count(*) AS row_count
FROM core.operation_type t;

SELECT 
    'payment_document' AS table_name, 
    md5(string_agg(row_to_json(t.*)::text, '' ORDER BY payment_document_id)) AS table_hash,
    count(*) AS row_count
FROM core.payment_document t;