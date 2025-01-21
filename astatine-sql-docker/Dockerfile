FROM openjdk:17-jdk

WORKDIR /app/astatine
ADD target/astatine-sql-docker-final.jar .
ADD target/test-classes ./template
ADD sbin/start_docker_job.sh .
ENTRYPOINT ["bash", "start_docker_job.sh"]
