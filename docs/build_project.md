# Compile and Build astatine

Compiling Astatine depends on the JDK 17, Maven 3.9 and docker, Please make sure you have installed them before building astatine.

## Check the environment

```shell
$ mvn -version
Apache Maven 3.9.9 (8e8579a9e76f7d015ee5ec7bfcdc97d260186937)
Maven home: ...
Java version: 17.0.13

$ docker version # check whether docker is installed
Client:
 Cloud integration: v1.0.29
 Version:           20.10.22
 ...
Server: Docker Desktop 4.16.2 (95914)
 Engine:
  Version:          20.10.22
 ... 
 
$  docker ps # check docker whether is running
CONTAINER ID   IMAGE             COMMAND           CREATED        STATUS      PORTS                    NAMES 
```

Tips: For macOs users, Go To [Docker-Release-Notes](https://docs.docker.com/desktop/release-notes/) to install a suitable docker version. Please download Docker Desktop version < 4.7.0 if your macOs version <= 10.14.

## Compile and Build Docker image by source code

```shell
$ git clone $astatine_repo
$ cd astatine
$ mvn clean install -DskipTests -Pdocker
[INFO] Reactor Summary for astatine 1.0-SNAPSHOT:
[INFO]
[INFO] astatine ........................................... SUCCESS [  0.091 s]
[INFO] astatine-streaming-sql ............................. SUCCESS [  0.766 s]
[INFO] astatine-streaming-sql-parser ...................... SUCCESS [ 27.799 s]
[INFO] astatine-streaming-sql-runtime ..................... SUCCESS [  4.706 s]
[INFO] astatine-functions ................................. SUCCESS [  1.965 s]
[INFO] astatine-connectors ................................ SUCCESS [  0.041 s]
[INFO] astatine-connector-socket .......................... SUCCESS [  0.504 s]
[INFO] astatine-connector-http ............................ SUCCESS [  0.576 s]
[INFO] astatine-formats ................................... SUCCESS [  0.006 s]
[INFO] astatine-format-protobuf ........................... SUCCESS [  2.008 s]
[INFO] astatine-sql-client ................................ SUCCESS [ 14.003 s]
[INFO] astatine-sql-docker ................................ SUCCESS [ 42.531 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
$ docker image ls|grep astatine-sql-docker # check develop docker image building successfully
astatine-sql-docker  latest  72c4ffb818d5   About a minute ago   826MB
```