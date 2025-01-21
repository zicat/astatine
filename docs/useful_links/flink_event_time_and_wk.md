# Flink Event Time and Watermark

[Event Time and Watermark](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/concepts/time/#event-time-and-watermarks) is very important concept in Flink. It can effectively control data latency and out-of-order issues, which can affect the accuracy and timeliness of jobs.

In this section, Let's use some examples to learn how to use Event Time and Watermark in Flink.

For example, we need to develop a streaming-sql job of requests count per minute as follows:
```sql
CREATE TABLE events (
    request_id    BIGINT,
    ts            TIMESTAMP(3)
);

SELECT TUMBLE_START(INTERVAL '1' MINUTE), COUNT(*) 
FROM events
GROUP BY TUMBLE(INTERVAL '1' MINUTE);
```

For Window Operators, Flink needs to choose an appropriate time for triggering the computations.

## Process Time Trigger

First, introduce a way as trigger by using process time
```sql
CREATE TABLE events (
    request_id    BIGINT,
    ts            TIMESTAMP(3)
);

SELECT TUMBLE_START(proctime(), INTERVAL '1' MINUTE), COUNT(*) 
FROM events
GROUP BY TUMBLE(proctime(), INTERVAL '1' MINUTE);
```
Flink provide the function proctime() to generator the proctime for each record.

The ts of events does not correlate with the computation logic, which will lead to accuracy issues when data latency.
For example data was processed as follows:

| request_id | ts       | proctime |
|------------|----------|----------|
| 1          | 10:00:08 | 10:02:00 |
| 2          | 10:00:09 | 10:02:00 |
| 3          | 10:00:10 | 10:02:00 |
| 4          | 10:00:11 | 10:02:00 |

The output result is (10:02:00, 4) when proctime is at 10:02:00 for some reason like data backlog, but the correct result is (10:00:00, 4)

## Event Time Trigger

Second, introduce a way as trigger by using event time
```sql
CREATE TABLE events (
    request_id    BIGINT,
    ts            TIMESTAMP(3)
);

SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE), COUNT(*) 
FROM events
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);
```

Event time can effectively solve the problem of data latency. After using event time, the output is (10:00:00, 4)

Let's adjust example data was arrived as follows:

| request_id | ts       |
|------------|----------|
| 1          | 10:00:08 |
| 2          | 10:00:09 |
| 3          | 10:00:10 |
| 4          | 10:00:11 |
| 5          | 10:01:00 |
| 6          | 10:00:59 |

Because the record of (5, 10:01:00) arrive before than (6, 10:00:59), so when flink receive (5, 10:01:00), it will trigger the computation of the window [10:00:00, 10:01:00)

The output result is (10:00:00, 4), but in some cases we want to include the record of (6, 10:00:59) and the expected result is (10:00:00, 5).

So using event time trigger can't resolve the issue of out-of-order data.

## Watermark Trigger

Third, introduce a way as trigger by using watermark
```sql
CREATE TABLE events (
    request_id    BIGINT,
    ts            TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
);

SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE), COUNT(*) 
FROM events
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);
```

Watermark is a tool that measures the overall time of data. It's generally calculated based on event time and the value of the watermark only increases, never decreases.


For example above, we define the WATERMARK base on (ts - 3s)

Let's just using above test data and add the watermark column as follows:

| request_id | ts       | watermark                                                   |
|------------|----------|-------------------------------------------------------------|
| 1          | 10:00:08 | 10:00:05                                                    |
| 2          | 10:00:09 | 10:00:06                                                    |
| 3          | 10:00:10 | 10:00:07                                                    |
| 4          | 10:00:11 | 10:00:08                                                    | 
| 5          | 10:01:00 | 10:00:57                                                    | 
| 6          | 10:00:59 | `10:00:57`(not 10:00:56, because watermark never decreases) |
| 7          | 10:01:03 | 10:01:00                                                    |

The record of (5, 10:01:00) will not trigger the window [10:00:00, 10:01:00) because the watermark is 10:00:57 and
The record of (7, 10:01:03) will trigger the [10:00:00, 10:01:00) because the watermark is 10:01:00.

The output result is (10:00:00, 5) after the record of (7, 10:01:03) was arrived.

The negative effect of watermark is the introduction of computational delay. 

For example above, the result of [10:00:00, 10:01:00) is outputted at 10:01:03(3s delay). For streaming job, there is a tradeoff between latency and accuracy.