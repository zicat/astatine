# Develop User Defined Functions in Astatine

Flink allow user to [develop functions](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/functions/udfs/), Astatine support to manage user-defined functions and register in sql.

This document introduces how to develop user-defined functions and register in sql.

1. Define a java file like HashCode.java in [astatine-functions](../astatine-functions/src/main/java/name/zicat/astatine/functions/math/HashCode.java).
    
    ```java
   /** HashCode. */
   public class HashCode extends ScalarFunction {
   
       private static final int DEFAULT_SEED = 12331233;
       private static final String PARAM_SEED = "hashcode.seed";
       
       private transient HashFunction hashFunction;
       
       @Override
       public void open(FunctionContext context) throws Exception {
           super.open(context);
           hashFunction =
           Hashing.murmur3_128(
           Integer.parseInt(context.getJobParameter(PARAM_SEED, String.valueOf(DEFAULT_SEED))));
       }
       
       public long eval(String str) {
          if (str == null) {
              return 0;
          }
          return hashFunction.newHasher().putString(str, StandardCharsets.UTF_8).hash().asLong();
       }
   }
    ```
2. Rebuild project

    ```shell
    $ mvn clean install -Pdocker
    ......
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time:  3.745 s
    [INFO] Finished at: 2025-01-22T16:34:31+08:00
    ```

3. Use the function in the SQL

    ```sql
   
    SET 'jobparam.hashcode.seed' = '${hashcode\.seed}';
    CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS hash_code AS 'name.zicat.astatine.functions.math.HashCode' LANGUAGE JAVA;
    
    CREATE TABLE events (
        request_id    STRING
    );
    
    SELECT request_id, hash_code(request_id) AS hash_code
    FROM events;
    ```
    
    Note:
    - The udf params must start with `jobparam.` like `jobparam.hashcode.seed`.
    - Set the function param first then defined function.
   
4. Using template 
  
    Astatine suggest user define the function in the template file, the function.ftl file is in [astatine-sql](../astatine-sql/template/function.ftl).
    
    ```ftl
    <#macro udf_math hashcode\.seed = '12331233'>
    SET 'jobparam.hashcode.seed' = '${hashcode\.seed}';
    CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS hash_code AS 'name.zicat.astatine.functions.math.HashCode' LANGUAGE JAVA;
    </#macro>
     ```
   
    Using in the SQL with the following code:

    ```sql
    <#import "env_local.ftl" as template>
    <@template.udf_math hashcode\.seed = '2222222' />
   
    CREATE TABLE events (
        request_id    STRING
    );
    
    SELECT request_id, hash_code(request_id) AS hash_code
    FROM events;
    ```

  