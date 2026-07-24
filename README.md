# platform-deploy-runtime

Runtime components for the platform application deployment control plane.

This repo owns the deploy API service and Flink-side preparation jobs. Directus
schema and app registry collections stay in `k8s-cms`; the NiFi orchestration
flow stays in `dataflow-platform-deploy-app`.

## Components

- `platform-deploy-service/` - internal API used by the Organization Management
  UI and Hasura/Gravitee actions to queue app deploy/destroy operations and
  update deploy-time Vault secrets.
- `platform-deploy-flink-job/` - Flink batch job that validates/enriches a
  queued operation and publishes the prepared NiFi request.
- `manifests/` - Kubernetes resources for the runtime service and Flink jar
  upload hook.
- `.github/workflows/` - image build/publish workflows for runtime images.
- `docs/platform-deploy-service-openapi.yaml` - static OpenAPI copy for the
  deploy service contract.

## Deployment Flow

1. Internal Organization Management creates or updates a `platform_apps` row in
   Directus.
2. The UI calls `platform-deploy-service`.
3. `platform-deploy-service` creates a `platform_app_operations` row and submits
   `platform-deploy-flink-job` through Flink REST.
4. The Flink job validates the payload, publishes to
   `batch.platform.deploy.prepared.v1`, and calls the service callback.
5. `dataflow-platform-deploy-app` consumes the prepared Kafka request and runs
   the NiFi orchestration flow.

## Validation

```sh
cd platform-deploy-service
npm run build

cd ../platform-deploy-flink-job
mvn -B -ntp package
```

Secrets are resolved at runtime from Vault/Kubernetes. Do not commit secret
values to this repo.

`POST /internal/secrets/platform-deploy` writes the deploy executor credentials
to Vault paths `secret/data/platform-deploy-service` and
`secret/data/platform-deploy-service/github`. The service merges the submitted
keys with existing KV data so the internal service token remains intact.
