# The first streaming sql job based on local docker in local environment

Goto [Compile and Build astatine](build_project.md) to build astatine first before read following content.

1. Create a file like test.sql, input the content as below and save it.

    ```text
    -- define streaming source
    CREATE TABLE source (
        name STRING ,
        score INT
    ) WITH (
      'connector' = 'socket',
      'hostname' = 'host.docker.internal',
      'port' = '9999',
      'byte-delimiter' = '10',
      'format' = 'json'
    );
    
    -- create logic view
    CREATE VIEW source_double AS SELECT name, score * 2 FROM source;
    
    -- keyword 'PRINT FROM' is used to print result to terminal
    PRINT FROM source_double;
    ``` 

2. Run it

    - Since the source above is a local-socket-text stream with port 9999, open a terminal to create the socket by `nc` command as below:

       ```shell
       $ nc -l 9999
       ```

    - Open new terminal and run the job as below:

       ```shell
       # macOs/Linux arm64 users
       $ docker run --network=host -t -i -e sql="$(cat test.sql)" -e java_opts="-Xmx3g" astatine-sql:latest
       # Linux amd64 users
       $ docker run --network=host -t -i -e sql="$(cat test.sql)" -e java_opts="-Xmx3g" astatine-sql-linux-amd64:latest
       ......
       Source: source[1] -> Calc[2] -> Sink: ... (1/1)(...) switched from INITIALIZING to RUNNING.
       ```

    - Switch to the `nc` terminal and input data as below:

      ```shell
      $ nc -l 9999
      {"name":"s1","score":100}
      {"name":"s2","score":100}
      ```

    - Switch to the `docker run` terminal, the inputted records are successfully calculated and printed as below:

       ```shell
       $ sql=$(cat test.sql);docker run --network=host -t -i -e sql="$sql" -e java_opts="-Xmx3g" astatine-sql:latest
       ......
       Source: source[1] -> Calc[2] -> Sink: ... (1/1)(...) switched from INITIALIZING to RUNNING.
       {"name":"s1","score":200}
       {"name":"s2","score":200}
       ```

3. Learning more about SQL expressions

   Astatine SQL is fully compatible with Flink-SQL. To learn more about SQL expressions, please refer to [Flink-Sql](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/overview/)

4. Using template

   Templates are supported to develop sql, useful in multi environments.

   Templates are like macros in C language. Before running SQL, Astatine is responsible for replacing the corresponding content of the templates. The templates are defined in [template directory](../astatine-sql/template).
   ```sql
   -- define streaming source
   CREATE TABLE source (
      name  STRING,
      score INT
   ) <@template.table_socket_source hostname = 'host.docker.internal' />
   
   -- create logic view
   CREATE VIEW source_double AS SELECT name, score * 2 FROM source;
   
   -- keyword 'PRINT FROM' is used to print result to terminal
   PRINT FROM source_double;
   ```