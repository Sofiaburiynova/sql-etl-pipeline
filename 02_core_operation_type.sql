ALTER TABLE core.payment_document 
    ALTER COLUMN operation_type_id DROP NOT NULL;

ALTER TABLE core.payment_document 
    ALTER COLUMN branch_id DROP NOT NULL;

DROP PROCEDURE IF EXISTS core.p_payment_document_load;

CREATE OR REPLACE PROCEDURE core.p_payment_document_load()
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt bigint;
    v_error_text text;
BEGIN


CALL meta.p_log('core.p_payment_document_load','core.payment_document',1,'Start load to core.payment_document');



DROP TABLE IF EXISTS _BUF_PAYMENT_DOCUMENT;

CREATE TEMP TABLE _BUF_PAYMENT_DOCUMENT
(
    source_system_id int2,
    changed int2,
    draft int2,
    abs_id varchar(32),
    number varchar(32),
    document_date date,
    perform_date date,
    value_date date,
    payer_name varchar(512),
    receiver_inn varchar(32),
    _md5 char(32),
    _key varchar(45),
    _src_operation_code varchar(50),
    _src_branch_key varchar(50)
);


INSERT INTO _BUF_PAYMENT_DOCUMENT
(
    source_system_id, changed, draft,
    abs_id, number, document_date, perform_date, value_date,
    payer_name, receiver_inn,
    _md5, _key,
    _src_operation_code, _src_branch_key
)
SELECT
    1 AS source_system_id,
    1 AS changed,
    0 AS draft,
    payment_document_id::varchar,
    document_number::varchar,
    CAST(document_date AS date),
    CAST(execution_date AS date),
    CAST(value_date AS date),
    payer_name::varchar,
    recipient_inn::varchar,
    md5(CAST((document_number,document_date,execution_date,value_date,payer_name,recipient_inn,operation_code,branch_id,username) AS text)),
    payment_document_id::varchar AS _key,
    operation_code::varchar,
    branch_id::varchar
FROM raw_data.payment_document;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_payment_document_load','core.payment_document',2,'Inserted data to _BUF_PAYMENT_DOCUMENT', v_cnt);




WITH dub AS (
    SELECT ctid, ROW_NUMBER() OVER (PARTITION BY _key ORDER BY ctid) AS rn
    FROM _BUF_PAYMENT_DOCUMENT
)
DELETE FROM _BUF_PAYMENT_DOCUMENT
WHERE ctid IN (SELECT ctid FROM dub WHERE rn > 1);

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_payment_document_load','core.payment_document',2,'Deleted duplicates from _BUF_PAYMENT_DOCUMENT', v_cnt);




UPDATE core.payment_document tgt
SET 
    source_system_id = buf.source_system_id,
    changed = 2,
    draft = CASE WHEN op.operation_type_id IS NULL OR br.branch_id IS NULL THEN 1 ELSE 0 END,
    abs_id = buf.abs_id,
    number = buf.number,
    document_date = buf.document_date,
    perform_date = buf.perform_date,
    value_date = buf.value_date,
    payer_name = buf.payer_name,
    receiver_inn = buf.receiver_inn,
    _md5 = buf._md5,
    _modifydatetime = now(),
    operation_type_id = op.operation_type_id,
    branch_id = br.branch_id
FROM _BUF_PAYMENT_DOCUMENT buf
LEFT JOIN core.operation_type op ON op._key = buf._src_operation_code
LEFT JOIN core.branch br ON br._key = buf._src_branch_key
WHERE buf._key = tgt._key
  AND buf._md5 != tgt._md5;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_payment_document_load','core.payment_document',2,'Updated rows in core.payment_document', v_cnt);


-

INSERT INTO core.payment_document
(
    source_system_id, changed, draft,
    abs_id, number, document_date, perform_date, value_date,
    payer_name, receiver_inn,
    _md5, _modifydatetime, _key,
    operation_type_id, branch_id
)
SELECT
    buf.source_system_id,
    buf.changed,
    CASE WHEN op.operation_type_id IS NULL OR br.branch_id IS NULL THEN 1 ELSE 0 END,
    buf.abs_id,
    buf.number,
    buf.document_date,
    buf.perform_date,
    buf.value_date,
    buf.payer_name,
    buf.receiver_inn,
    buf._md5,
    now(),
    buf._key,
    op.operation_type_id,
    br.branch_id
FROM _BUF_PAYMENT_DOCUMENT buf
LEFT JOIN core.operation_type op ON op._key = buf._src_operation_code
LEFT JOIN core.branch br ON br._key = buf._src_branch_key
LEFT JOIN core.payment_document tgt ON tgt._key = buf._key
WHERE tgt._key IS NULL;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_payment_document_load','core.payment_document',2,'Inserted new rows into core.payment_document', v_cnt);




CALL meta.p_log('core.p_payment_document_load','core.payment_document',3,'Finish load to core.payment_document');


EXCEPTION
    WHEN data_exception THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('core.p_payment_document_load','core.payment_document',4,'DATA ERROR: ' || v_error_text);
        RAISE;

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('core.p_payment_document_load','core.payment_document',4,'ERROR: ' || v_error_text);
        RAISE;

END;
$$;