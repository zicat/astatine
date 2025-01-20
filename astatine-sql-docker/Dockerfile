FROM hub.agoralab.co/data-platform/openjdk:17

WORKDIR /app/astatine
ADD target/astatine-sql-docker-final.jar .
ADD target/test-classes ./template
ADD sbin/start_docker_job.sh .
ENTRYPOINT ["bash", "start_docker_job.sh"]
