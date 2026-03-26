SELECT DISTINCT
       1 AS source_system_id,
       1 AS changed,
       0 AS draft,
       operation_code AS code,
       operation_type AS name,
       md5(CAST((operation_type,username) AS text)) AS _md5,
       operation_code AS _key
FROM raw_data.payment_document;

DROP PROCEDURE IF EXISTS core.p_operation_type_load;

CREATE OR REPLACE PROCEDURE core.p_operation_type_load()
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt bigint;
    v_error_text text;
BEGIN


CALL meta.p_log('core.p_operation_type_load','core.operation_type',1,'Start load to core.operation_type');



DROP TABLE IF EXISTS _BUF_OPERATION_TYPE;

CREATE TEMPORARY TABLE _BUF_OPERATION_TYPE
(
    source_system_id INT2,
    changed INT2,
    draft INT2,
    code VARCHAR(50),
    name VARCHAR(255),
    _md5 CHAR(32),
    _key VARCHAR(50)
);SELECT DISTINCT
       1 AS source_system_id,
       1 AS changed,
       0 AS draft,
       operation_code AS code,
       operation_type AS name,
       md5(CAST((operation_type,username) AS text)) AS _md5,
       operation_code AS _key
FROM raw_data.payment_document;

DROP PROCEDURE IF EXISTS core.p_operation_type_load;

CREATE OR REPLACE PROCEDURE core.p_operation_type_load()
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt bigint;
    v_error_text text;
BEGIN


CALL meta.p_log('core.p_operation_type_load','core.operation_type',1,'Start load to core.operation_type');



DROP TABLE IF EXISTS _BUF_OPERATION_TYPE;

CREATE TEMPORARY TABLE _BUF_OPERATION_TYPE
(
    source_system_id INT2,
    changed INT2,
    draft INT2,
    code VARCHAR(50),
    name VARCHAR(255),
    _md5 CHAR(32),
    _key VARCHAR(50)
);

-- MAPPING
INSERT INTO _BUF_OPERATION_TYPE
(
    source_system_id, changed, draft,
    code, name,
    _md5, _key
)
SELECT DISTINCT
       1 AS source_system_id,
       1 AS changed,
       0 AS draft,
       operation_code::varchar,
       operation_type::varchar,
       md5(CAST((operation_type,username) AS text)),
       operation_code::varchar AS _key
FROM raw_data.payment_document;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Inserted data to _BUF_OPERATION_TYPE', v_cnt);




WITH dub AS (
    SELECT ctid,
           ROW_NUMBER() OVER (PARTITION BY _key ORDER BY ctid) AS rn
    FROM _BUF_OPERATION_TYPE
)
DELETE FROM _BUF_OPERATION_TYPE
WHERE ctid IN (SELECT ctid FROM dub WHERE rn > 1);

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Deleted duplicates from _BUF_OPERATION_TYPE', v_cnt);



DROP TABLE IF EXISTS _TMP_DEL_OP;

CREATE TEMPORARY TABLE _TMP_DEL_OP AS
SELECT tgt._key
FROM core.operation_type tgt
LEFT JOIN _BUF_OPERATION_TYPE buf ON buf._key = tgt._key
WHERE tgt.changed IN (1,2)
  AND buf._key IS NULL;

UPDATE core.operation_type
   SET changed = 3,
       _modifydatetime = now()
 WHERE _key IN (SELECT _key FROM _TMP_DEL_OP);

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Updated changed=3 for missed rows in core.operation_type', v_cnt);




UPDATE core.operation_type tgt
   SET source_system_id = buf.source_system_id,
       changed = 2,
       draft = 0,
       code = buf.code,
       name = buf.name,
       _md5 = buf._md5,
       _modifydatetime = now()
FROM _BUF_OPERATION_TYPE buf
WHERE buf._key = tgt._key
  AND buf._md5 != tgt._md5;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Updated rows in core.operation_type', v_cnt);




INSERT INTO core.operation_type
(
    source_system_id, changed, draft,
    code, name,
    _md5, _modifydatetime, _key
)
SELECT
       buf.source_system_id,
       buf.changed,
       0 AS draft,
       buf.code,
       buf.name,
       buf._md5,
       now(),
       buf._key
FROM _BUF_OPERATION_TYPE buf
LEFT JOIN core.operation_type tgt ON tgt._key = buf._key
WHERE tgt._key IS NULL;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Inserted new rows into core.operation_type', v_cnt);




CALL meta.p_log('core.p_operation_type_load','core.operation_type',3,'Finish load to core.operation_type');



EXCEPTION
    WHEN data_exception THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('core.p_operation_type_load','core.operation_type',4,'DATA ERROR: ' || v_error_text);
        RAISE;

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('core.p_operation_type_load','core.operation_type',4,'ERROR: ' || v_error_text);
        RAISE;
END;
$$;

-- MAPPING
INSERT INTO _BUF_OPERATION_TYPE
(
    source_system_id, changed, draft,
    code, name,
    _md5, _key
)
SELECT DISTINCT
       1 AS source_system_id,
       1 AS changed,
       0 AS draft,
       operation_code::varchar,
       operation_type::varchar,
       md5(CAST((operation_type,username) AS text)),
       operation_code::varchar AS _key
FROM raw_data.payment_document;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Inserted data to _BUF_OPERATION_TYPE', v_cnt);




WITH dub AS (
    SELECT ctid,
           ROW_NUMBER() OVER (PARTITION BY _key ORDER BY ctid) AS rn
    FROM _BUF_OPERATION_TYPE
)
DELETE FROM _BUF_OPERATION_TYPE
WHERE ctid IN (SELECT ctid FROM dub WHERE rn > 1);

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Deleted duplicates from _BUF_OPERATION_TYPE', v_cnt);



DROP TABLE IF EXISTS _TMP_DEL_OP;

CREATE TEMPORARY TABLE _TMP_DEL_OP AS
SELECT tgt._key
FROM core.operation_type tgt
LEFT JOIN _BUF_OPERATION_TYPE buf ON buf._key = tgt._key
WHERE tgt.changed IN (1,2)
  AND buf._key IS NULL;

UPDATE core.operation_type
   SET changed = 3,
       _modifydatetime = now()
 WHERE _key IN (SELECT _key FROM _TMP_DEL_OP);

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Updated changed=3 for missed rows in core.operation_type', v_cnt);




UPDATE core.operation_type tgt
   SET source_system_id = buf.source_system_id,
       changed = 2,
       draft = 0,
       code = buf.code,
       name = buf.name,
       _md5 = buf._md5,
       _modifydatetime = now()
FROM _BUF_OPERATION_TYPE buf
WHERE buf._key = tgt._key
  AND buf._md5 != tgt._md5;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Updated rows in core.operation_type', v_cnt);




INSERT INTO core.operation_type
(
    source_system_id, changed, draft,
    code, name,
    _md5, _modifydatetime, _key
)
SELECT
       buf.source_system_id,
       buf.changed,
       0 AS draft,
       buf.code,
       buf.name,
       buf._md5,
       now(),
       buf._key
FROM _BUF_OPERATION_TYPE buf
LEFT JOIN core.operation_type tgt ON tgt._key = buf._key
WHERE tgt._key IS NULL;

GET DIAGNOSTICS v_cnt = ROW_COUNT;
CALL meta.p_log('core.p_operation_type_load','core.operation_type',2,'Inserted new rows into core.operation_type', v_cnt);




CALL meta.p_log('core.p_operation_type_load','core.operation_type',3,'Finish load to core.operation_type');



EXCEPTION
    WHEN data_exception THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('core.p_operation_type_load','core.operation_type',4,'DATA ERROR: ' || v_error_text);
        RAISE;

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('core.p_operation_type_load','core.operation_type',4,'ERROR: ' || v_error_text);
        RAISE;
END;
$$;