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

## Image Publishing

The GitHub Actions image workflows publish to GHCR using the repository
`GITHUB_TOKEN` by default. If GHCR rejects the push with `write_package`, add a
repository secret named `GHCR_PAT` with `write:packages` access and, when the PAT
owner is not the workflow actor, a `GHCR_USER` secret containing that username.
For existing org packages, the package must also grant this repository write
access under the package's Actions access settings.

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

`platform-deploy-service` accepts either its service-internal token from
`secret/data/platform-deploy-service#token` or the configured Directus platform
management service token. In Kubernetes, that Directus token is resolved from
`secret/data/directus/gravitee/clients/<graphql-api-client-id>#token`, matching
the token forwarded by Gravitee after it exchanges the user bearer token through
`directus-auth-bridge`.

`GET /internal/secrets/platform-deploy` loads the operator-managed Terraform
Cloud, Cloudflare, and source clone credentials for the Organization Management
dialog. `POST /internal/secrets/platform-deploy` writes those same fields to
Vault paths `secret/data/platform-deploy-service` and
`secret/data/platform-deploy-service/github`; `github_token` is also kept on the
main path so the full dialog field set is visible together. The auth gateway admin token is
resolved from the existing internal `secret/data/auth-gateway-admin-api#value`
secret, not from this UI/API. The service merges submitted keys with existing
KV data so internal tokens remain intact.
