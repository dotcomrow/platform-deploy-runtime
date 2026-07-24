FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml ./
RUN mvn -B -ntp dependency:go-offline
COPY src ./src
RUN mvn -B -ntp package

FROM flink:1.20.3-scala_2.12-java17
USER root
COPY --from=build /workspace/target/platform-deploy-flink-job-1.0.0.jar /opt/flink/usrlib/platform-deploy-flink-job.jar
COPY run-platform-deploy-prepare.sh /opt/flink/bin/run-platform-deploy-prepare.sh
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && chmod +x /opt/flink/bin/run-platform-deploy-prepare.sh \
  && chown -R flink:flink /opt/flink/usrlib /opt/flink/bin/run-platform-deploy-prepare.sh
USER flink
ENTRYPOINT ["/opt/flink/bin/run-platform-deploy-prepare.sh"]
