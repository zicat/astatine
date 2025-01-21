# How to Optimize Multi-Metrics in Same Dimensions With Flink SQL

## Introduction

Assume a scenario where there is a dataset containing the following fields: `sid`, `country`, `os`, `delay`, `ts`. Now,
we need to group by country and os to construct three metrics:

- The proportion of users with a delay of less than 200 ms per minute.
- The proportion of users with a delay of less than 400 ms per minute.
- The average delay per minute.

## Implementation

### 3 Group By Operators

```sql
CREATE TABLE ouput_table (
    ts          TIMESTAMP(3),
    metric      STRING,
    country     STRING,
    os          STRING,
    numerator   BIGINT,
    denominator BIGINT
)

INSERT INTO ouput_table
SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE) AS ts
     ,country
     ,os
     ,'delay_below_200' AS metric
     ,SUM(CASE WHEN delay < 200 THEN 1 ELSE 0 END) AS numerator
     ,COUNT(1) AS denominator
FROM source
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), country, os;

INSERT INTO ouput_table
SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE) AS ts
     ,country
     ,os
     ,'delay_below_400' AS metric
     ,SUM(CASE WHEN delay < 400 THEN 1 ELSE 0 END) AS numerator
     ,COUNT(1) AS denominator
FROM source
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), country, os;

INSERT INTO ouput_table
SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE) AS ts
     ,country
     ,os
     ,'delay_avg' AS metric
     ,SUM(delay) AS numerator
     ,COUNT(1) AS denominator
FROM source
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), country, os;
```

In the code above, we used three Group By operators to calculate three metrics. The drawback of this approach is that
each Group By operator performs an aggregation on the data, resulting in the data being shuffled three times, which
leads to poorer performance.

### 1 Group By Operator With Table Function

```java
public class DelayTableFunction extends TableFunction<Row> {

    @FunctionHint(
            output =
            @DataTypeHint(
                    """
                            ROW<
                            metric         STRING,
                            numerator      BIGINT,
                            denominator    BIGINT>"""))
    public void eval(
            Long delay200Cnt, Long delay400Cnt, Long sumDelay, Long cnt)
            throws Exception {
        // convert one record to 3 records with (metric, numerator, denominator)
        collect(Row.of("delay_below_200", delay200Cnt, cnt));
        collect(Row.of("delay_below_400", delay400Cnt, cnt));
        collect(Row.of("delay_avg", sumDelay, cnt));
    }
}
```

```sql
   
CREATE TEMPORARY FUNCTION IF NOT EXISTS delay_table_function AS 'DelayTableFunction' LANGUAGE JAVA;

CREATE TABLE ouput_table (
    ts          TIMESTAMP(3),
    country     STRING,
    os          STRING,
    metric      STRING,
    numerator   BIGINT,
    denominator BIGINT
)

CREATE VIEW group_result AS 
SELECT TUMBLE_START(ts, INTERVAL '1' MINUTE) AS ts
     ,country
     ,os
     ,SUM(CASE WHEN delay < 200 THEN 1 ELSE 0 END) AS delay_200_cnt
     ,SUM(CASE WHEN delay < 400 THEN 1 ELSE 0 END) AS delay_400_cnt
     ,SUM(delay) AS sum_delay
     ,COUNT(1) AS cnt
FROM source
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), country, os;

INSERT INTO ouput_table
SELECT a.ts
      ,a.country
      ,a.os
      ,b.metric
      ,b.numerator
      ,b.denominator
FROM group_result AS a 
LEFT JOIN lateral TABLE(delay_table_function(
    delay_200_cnt,
    delay_400_cnt,
    sum_delay,
    cnt
)) AS b ON TRUE;    
```

In the code above, we used one Group By operator to calculate four atomic metrics. By converting the atomic metrics into
three target metrics using a Table Function, we only need to shuffle the data once, resulting in better performance.