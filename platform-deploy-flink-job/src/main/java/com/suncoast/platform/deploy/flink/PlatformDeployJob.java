package com.suncoast.platform.deploy.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public final class PlatformDeployJob {
  private PlatformDeployJob() {
  }

  public static void main(String[] args) throws Exception {
    Config config = Config.fromArgs(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(1);

    env.fromData(config.operationId)
      .map(new PlatformDeployPreparer(config))
      .name("prepare-platform-deploy-request")
      .setParallelism(1)
      .print()
      .name("print-platform-deploy-prepare-summary")
      .setParallelism(1);

    env.execute("Platform Deploy Prepare - " + config.operationId);
  }

  static final class PlatformDeployPreparer implements MapFunction<String, String> {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Config config;

    PlatformDeployPreparer(Config config) {
      this.config = config;
    }

    @Override
    public String map(String ignored) throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode payload = null;
      try {
        payload = decodePayload(mapper, config.operationPayloadBase64);
        validate(payload);
        String preparedAt = Instant.now().toString();
        payload.put("prepared_at", preparedAt);
        payload.put("prepared_by", "platform-deploy-flink-job");
        payload.put("source", config.source);
        payload.put("nifi_target_topic", config.preparedTopic);

        String rendered = mapper.writeValueAsString(payload);
        publishPreparedRequest(rendered);
        callbackPrepared(mapper, payload, preparedAt);
        return mapper.writeValueAsString(summary(mapper, payload, preparedAt));
      } catch (Exception exc) {
        callbackFailure(mapper, payload, exc);
        throw exc;
      }
    }

    private ObjectNode decodePayload(ObjectMapper mapper, String encoded) throws IOException {
      if (encoded == null || encoded.isBlank()) {
        throw new IllegalArgumentException("--operation-payload-base64 is required");
      }
      byte[] bytes;
      try {
        bytes = Base64.getUrlDecoder().decode(encoded);
      } catch (IllegalArgumentException ignored) {
        bytes = Base64.getDecoder().decode(encoded);
      }
      JsonNode decoded = mapper.readTree(new String(bytes, StandardCharsets.UTF_8));
      if (!decoded.isObject()) {
        throw new IllegalArgumentException("Decoded operation payload must be a JSON object.");
      }
      return (ObjectNode) decoded;
    }

    private void validate(ObjectNode payload) {
      List<String> missing = new ArrayList<>();
      require(payload, missing, "operation_id");
      require(payload, missing, "app_id");
      require(payload, missing, "app_key");
      require(payload, missing, "operation_type");
      require(payload, missing, "sequence");
      require(payload, missing, "site_key");
      require(payload, missing, "keycloak_realm");
      require(payload, missing, "deployment_strategy");
      require(payload, missing, "source_repository");

      if (!config.operationId.equals(text(payload, "operation_id"))) {
        throw new IllegalArgumentException("operation_id payload mismatch.");
      }
      if (!config.appId.isBlank() && !config.appId.equals(text(payload, "app_id"))) {
        throw new IllegalArgumentException("app_id payload mismatch.");
      }
      if (!config.operationType.isBlank() && !config.operationType.equals(text(payload, "operation_type"))) {
        throw new IllegalArgumentException("operation_type payload mismatch.");
      }

      String operationType = text(payload, "operation_type");
      if (!List.of("create", "update", "redeploy", "delete", "destroy").contains(operationType)) {
        throw new IllegalArgumentException("Unsupported operation_type: " + operationType);
      }
      String realm = text(payload, "keycloak_realm");
      if (!List.of("internal", "external").contains(realm)) {
        throw new IllegalArgumentException("keycloak_realm must be internal or external.");
      }
      String strategy = text(payload, "deployment_strategy");
      if ("terraform_cloud".equals(strategy)) {
        require(payload, missing, "terraform_project");
        require(payload, missing, "github_repository");
        require(payload, missing, "github_ref");
        require(payload, missing, "terraform_cloud_organization");
      }

      if (!missing.isEmpty()) {
        throw new IllegalArgumentException("Missing required deployment fields: " + String.join(", ", missing));
      }
    }

    private void require(ObjectNode payload, List<String> missing, String field) {
      if (text(payload, field).isBlank()) {
        missing.add(field);
      }
    }

    private String text(JsonNode payload, String field) {
      return payload.path(field).asText("").trim();
    }

    private void publishPreparedRequest(String value) throws Exception {
      Properties props = new Properties();
      props.put("bootstrap.servers", config.kafkaBootstrapServers);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("acks", "all");
      props.put("client.id", "platform-deploy-flink-job-" + config.operationId);
      if (!config.kafkaSecurityProtocol.isBlank()) {
        props.put("security.protocol", config.kafkaSecurityProtocol);
      }
      if (!config.kafkaSaslMechanism.isBlank()) {
        props.put("sasl.mechanism", config.kafkaSaslMechanism);
      }
      if (!config.kafkaUsername.isBlank()) {
        props.put(
          "sasl.jaas.config",
          "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" +
            escapeJaas(config.kafkaUsername) +
            "\" password=\"" +
            escapeJaas(config.kafkaPassword) +
            "\";"
        );
      }

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
        producer.send(new ProducerRecord<>(config.preparedTopic, config.operationId, value)).get();
      }
    }

    private String escapeJaas(String value) {
      return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private void callbackPrepared(ObjectMapper mapper, ObjectNode payload, String preparedAt) throws Exception {
      ObjectNode result = mapper.createObjectNode();
      result.put("prepared_by", "platform-deploy-flink-job");
      result.put("prepared_topic", config.preparedTopic);
      result.put("prepared_at", preparedAt);
      result.put("source", config.source);

      ObjectNode body = mapper.createObjectNode();
      body.put("app_id", text(payload, "app_id"));
      body.put("operation_type", text(payload, "operation_type"));
      body.put("prepared_topic", config.preparedTopic);
      body.put("prepared_at", preparedAt);
      body.set("result_json", result);
      postServiceJson(mapper, "/internal/operations/" + config.operationId + "/prepared", body);
    }

    private void callbackFailure(ObjectMapper mapper, ObjectNode payload, Exception exc) {
      try {
        ObjectNode body = mapper.createObjectNode();
        body.put("app_id", payload == null ? config.appId : text(payload, "app_id"));
        body.put("operation_type", payload == null ? config.operationType : text(payload, "operation_type"));
        body.put("status", "failed");
        body.put("error_message", truncate(exc.getMessage(), 900));
        body.put("log_excerpt", truncate(exc.toString(), 2000));
        ObjectNode result = mapper.createObjectNode();
        result.put("prepared_by", "platform-deploy-flink-job");
        result.put("prepare_failed", true);
        result.put("prepared_topic", config.preparedTopic);
        body.set("result_json", result);
        postServiceJson(mapper, "/internal/operations/" + config.operationId + "/finish", body);
      } catch (Exception callbackError) {
        System.err.println("Failed to send platform deploy failure callback: " + callbackError.getMessage());
      }
    }

    private void postServiceJson(ObjectMapper mapper, String path, ObjectNode body) throws Exception {
      if (config.platformDeployServiceUrl.isBlank() || config.operationToken.isBlank()) {
        return;
      }
      URI uri = URI.create(trimTrailingSlash(config.platformDeployServiceUrl) + path);
      HttpRequest request = HttpRequest.newBuilder(uri)
        .timeout(Duration.ofMillis(config.httpTimeoutMs))
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + config.operationToken)
        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body), StandardCharsets.UTF_8))
        .build();
      HttpResponse<String> response = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(config.connectTimeoutMs))
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      if (response.statusCode() < 200 || response.statusCode() > 299) {
        throw new IllegalStateException("platform-deploy-service callback failed HTTP " + response.statusCode() + ": " + truncate(response.body(), 1000));
      }
    }

    private ObjectNode summary(ObjectMapper mapper, ObjectNode payload, String preparedAt) {
      ObjectNode summary = mapper.createObjectNode();
      summary.put("operation_id", config.operationId);
      summary.put("app_id", text(payload, "app_id"));
      summary.put("app_key", text(payload, "app_key"));
      summary.put("operation_type", text(payload, "operation_type"));
      summary.put("prepared_topic", config.preparedTopic);
      summary.put("prepared_at", preparedAt);
      summary.put("status", "prepared");
      return summary;
    }
  }

  static final class Config implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    final String operationId;
    final String appId;
    final String operationType;
    final String operationToken;
    final String operationPayloadBase64;
    final String preparedTopic;
    final String platformDeployServiceUrl;
    final String source;
    final String kafkaBootstrapServers;
    final String kafkaSecurityProtocol;
    final String kafkaSaslMechanism;
    final String kafkaUsername;
    final String kafkaPassword;
    final int connectTimeoutMs;
    final int httpTimeoutMs;

    Config(Map<String, String> args) {
      this.operationId = requireArg(args, "operation-id");
      this.appId = args.getOrDefault("app-id", "");
      this.operationType = args.getOrDefault("operation-type", "");
      this.operationToken = args.getOrDefault("operation-token", "");
      this.operationPayloadBase64 = requireArg(args, "operation-payload-base64");
      this.preparedTopic = args.getOrDefault("prepared-topic", env("PLATFORM_DEPLOY_PREPARED_TOPIC", "batch.platform.deploy.prepared.v1"));
      this.platformDeployServiceUrl = args.getOrDefault("platform-deploy-service-url", "http://platform-deploy-service.directus.svc.cluster.local:8080");
      this.source = args.getOrDefault("source", "platform-deploy-service");
      this.kafkaBootstrapServers = args.getOrDefault("kafka-bootstrap-servers", env("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.internal.lan:9092"));
      this.kafkaSecurityProtocol = args.getOrDefault("kafka-security-protocol", env("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"));
      this.kafkaSaslMechanism = args.getOrDefault("kafka-sasl-mechanism", env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"));
      this.kafkaUsername = args.getOrDefault("kafka-username", env("KAFKA_USERNAME", ""));
      this.kafkaPassword = args.getOrDefault("kafka-password", env("KAFKA_PASSWORD", ""));
      this.connectTimeoutMs = parseInt(args.get("connect-timeout-ms"), 5_000);
      this.httpTimeoutMs = parseInt(args.get("http-timeout-ms"), 60_000);
    }

    static Config fromArgs(String[] args) {
      return new Config(ArgParser.parse(args));
    }

    private String requireArg(Map<String, String> args, String name) {
      String value = args.getOrDefault(name, "").trim();
      if (value.isBlank()) {
        throw new IllegalArgumentException("--" + name + " is required");
      }
      return value;
    }
  }

  static final class ArgParser {
    private ArgParser() {
    }

    static Map<String, String> parse(String[] args) {
      LinkedHashMap<String, String> values = new LinkedHashMap<>();
      for (int i = 0; i < args.length; i += 1) {
        String arg = args[i];
        if (!arg.startsWith("--")) {
          continue;
        }
        String keyValue = arg.substring(2);
        int equals = keyValue.indexOf('=');
        if (equals >= 0) {
          values.put(keyValue.substring(0, equals), keyValue.substring(equals + 1));
        } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          values.put(keyValue, args[i + 1]);
          i += 1;
        } else {
          values.put(keyValue, "true");
        }
      }
      return values;
    }
  }

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value.trim();
  }

  private static int parseInt(String raw, int fallback) {
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException ignored) {
      return fallback;
    }
  }

  private static String trimTrailingSlash(String value) {
    String result = value == null ? "" : value.trim();
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  private static String truncate(String value, int max) {
    if (value == null || value.length() <= max) {
      return value == null ? "" : value;
    }
    return value.substring(0, max) + "...";
  }
}
