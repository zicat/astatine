#!/bin/bash

# shellcheck disable=SC2154
java_opts=${java_opts:-"-Xmx4g"}
java "${java_opts}" \
  --add-opens java.base/java.net=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-exports=java.base/sun.net.util=ALL-UNNAMED \
  --add-exports=java.management/sun.management=ALL-UNNAMED \
  --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -Xbootclasspath/a:/app/astatine/template \
  -jar astatine-sql-docker-final.jar "${sql}"