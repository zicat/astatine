# Date Hour Expansion Operator

The date hour expansion operator is a flap map operator that can expand the input row to multiple rows by the date or date & hour field.

## Date Expansion Operator

```sql
<@template.udf_time />

-- {"name":"n1","ts": 1739311800, "start_ts": 1739225400, "end_ts": 1739398200}
CREATE TABLE source (
    name        STRING ,
    ts          INT,
    start_ts    INT,
    end_ts      INT
) <@template.table_socket_source hostname = 'host.docker.internal' />

CREATE VIEW flap_map_date
FROM source WITH(
   'product.type' = 'RowData'
) FLAT MAP WITH (
   'identity' = 'date_expansion',
   'timezone' = 'GMT',
   'field.start-ts' = 'start_ts',
   'field.end-ts' = 'end_ts',
   'field.current-ts' = 'ts',
   'max-count' = '4'
);

CREATE VIEW flap_map_date_print AS
SELECT name
      ,to_timestamp(ts)
      ,to_timestamp(start_ts)
      ,to_timestamp(end_ts)
      ,CAST(`date` AS STRING)
FROM flap_map_date;

PRINT FROM flap_map_date_print;
```

Input:

```shell
$ nc -l 9999
{"name":"n1","ts": 1739311800, "start_ts": 1739225400, "end_ts": 1739398200}
```

Output:

```text
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-11]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-10]
```

## Date Hour Expansion Operator

```sql
<@template.udf_time />

-- {"name":"n1","ts": 1739311800, "start_ts": 1739225400, "end_ts": 1739398200}
CREATE TABLE source (
    name        STRING ,
    ts          INT,
    start_ts    INT,
    end_ts      INT
) <@template.table_socket_source hostname = 'host.docker.internal' />

CREATE VIEW flap_map_date
FROM source WITH(
   'product.type' = 'RowData'
) FLAT MAP WITH (
   'identity' = 'date_hour_expansion',
   'timezone' = 'GMT',
   'field.start-ts' = 'start_ts',
   'field.end-ts' = 'end_ts',
   'field.current-ts' = 'ts',
   'max-count' = '6'
);

CREATE VIEW flap_map_hour_date_print AS
SELECT name
      ,to_timestamp(ts)
      ,to_timestamp(start_ts)
      ,to_timestamp(end_ts)
      ,CAST(`date` AS STRING)
      ,`hour`
FROM flap_map_date;

PRINT FROM flap_map_hour_date_print;
```

Input:

```shell
$ nc -l 9999
{"name":"n1","ts": 1739311800, "start_ts": 1739225400, "end_ts": 1739398200}
```

Output:

```text
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12, 22]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12, 21]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12, 20]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12, 19]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12, 18]
+I[n1, 2025-02-11T22:10, 2025-02-10T22:10, 2025-02-12T22:10, 2025-02-12, 17]
```

## Note

1. The identity of the operator is `date_expansion` OR `date_hour_expansion`.
2. The operator named `date_expansion` will add a new field `date` type `DATE` to the output row.
   The operator named `date_hour_expansion` will add two new fields `date` type `DATE` and `hour` type `INT` to the output row.

   If the input row has field `date`, the operator will output `date1`.
   
   If the input row has field `hour`, the operator will output `hour1`.

3. The param `timezone` is the timezone of the date field.
4. The param `field.start-ts` is the start timestamp(INT/LONG/Timestamp), if not set, the start timestamp is zero.
5. The param `field.current-ts` is the timestamp(INT/LONG/Timestamp) of rows, it must be not null.
6. The param `field.end-ts` is the end timestamp(INT/LONG/Timestamp), if not set, the end timestamp is `field.current-ts`.
7. The param `max-count` is the max count output of a row.

   The real count of the rows is `MIN(date(COALESCE(field.end-ts, field.ts)) - date(COALESCE(field.start-ts, 0)) + 1, max-count)`, Example of `date_expansion`:

   1) field.end-ts = 20240106, field.start-ts = 20240104, max-count = 4, the date value include(20240106,20240105,20240104).
   2) field.end-ts = 20240106, field.start-ts = 20240101, max-count = 4, the date value include(20240106,20240105,20240104,20240103).