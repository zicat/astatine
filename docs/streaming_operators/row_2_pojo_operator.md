# Row2Pojo Operator

The row_2_pojo operator is a streaming map operator that converts the input row to a pojo object.

```sql
CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'host.docker.internal' />

-- astatine sql
CREATE STREAM stream_source
FROM source
MAP WITH (
    'identity' = 'row_2_pojo',
    'parallelism' = '2',
    'mapping.class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore',
    'return.class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore'
);

PRINT FROM stream_source;
```

Note:
1. The identity of the operator is `row_2_pojo`.
2. The param `parallelism` is the parallelism of the operator, it must be a positive integer, default -1 means following previous stream parallelism.
3. The `mapping.class` is set the mapping class name.
4. The `return.class` is set the return class name, the `return.class` must be equals `mapping.class` or it's super class.
5. If the field name of pojo class is different from the input row, you can use the `@Row2PojoProperty` to alias it.
    ```java
    public class NameScore {
        
        @Row2PojoProperty("name")
        private String name2;
        private int score;
        
        public void setName(String name2) {
            this.name2 = name2;
        }
        
        public void setScore(int score) {
            this.score = score;
        }
    }
    ```