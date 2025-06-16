# Socket Source Connector

Astatine support to read data from socket connector. The source connector is useful when testing some operators on local.

## How to Create Socket Source Table
```
-- define socket source table
CREATE TABLE source_socket(
    ...
) WITH (
  'connector' = 'socket',
  'hostname' = 'host.docker.internal',
  'port' = '9999',
  'byte-delimiter' = '10',
  'format' = 'json'
);

-- define with template
CREATE TABLE source_socket(
    ...
) <@template.table_socket_source  
     hostname = 'host.docker.internal'
     port = '9999'
     byte\-delimiter = '10'
     format = 'json'/>
```

## Connector Options

| Option          | Type   | Default | Description                                                         |
|-----------------|--------|---------|---------------------------------------------------------------------|
| connector       | String | (none)  | Specify what connector to use, for Socket Source Table use 'socket' |
| hostname        | String | (none)  | Specify the endpoint                                                |
| port            | Int    | (none)  | Specify the port                                                    |
| format          | String | (none)  | Specify the schema of socket                                        |
| byte\-delimiter | Int    | 10      | Specify the socket records split char, 10 means '\n'                |

- Use 'nc -l 9999' command to start the socket with port 9999.