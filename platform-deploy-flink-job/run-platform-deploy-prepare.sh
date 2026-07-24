#!/usr/bin/env sh
set -eu

FLINK_REST_TARGET="${FLINK_REST_TARGET:-flink-rest.kafka.svc.cluster.local:8081}"

exec /opt/flink/bin/flink run \
  -m "${FLINK_REST_TARGET}" \
  -c com.suncoast.platform.deploy.flink.PlatformDeployJob \
  /opt/flink/usrlib/platform-deploy-flink-job.jar \
  "$@"
