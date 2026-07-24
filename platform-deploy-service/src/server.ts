import express, { NextFunction, Request, Response } from "express";
import helmet from "helmet";
import morgan from "morgan";
import rateLimit from "express-rate-limit";
import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Agent, Dispatcher, request as undiciRequest, setGlobalDispatcher } from "undici";
import { z } from "zod";

const envSchema = z.object({
  PORT: z.string().default("8080"),
  TRUST_PROXY_HOPS: z.string().default("1"),
  RATE_WINDOW_MS: z.string().default("60000"),
  RATE_MAX: z.string().default("120"),
  REQUEST_TIMEOUT_MS: z.string().default("10000"),
  DIRECTUS_BASE_URL: z.string().default("http://directus-service.directus.svc.cluster.local:8055"),
  DIRECTUS_HEALTH_PATH: z.string().default("/server/health"),
  DIRECTUS_STATIC_TOKEN: z.string().default(""),
  DIRECTUS_TOKEN_VAULT_PATH: z.string().default("secret/data/directus/gravitee/openapi/admin"),
  DIRECTUS_TOKEN_VAULT_KEY: z.string().default("token"),
  INTERNAL_TOKEN: z.string().default(""),
  INTERNAL_TOKEN_VAULT_PATH: z.string().default("secret/data/platform-deploy-service"),
  INTERNAL_TOKEN_VAULT_KEY: z.string().default("token"),
  VAULT_ADDR: z.string().default("http://vault.vault.svc.cluster.local:8200"),
  VAULT_TOKEN_FILE: z.string().default("/vault-secrets/vault-token"),
  TOKEN_CACHE_SECONDS: z.string().default("300"),
  OPENAPI_SERVER_URL: z.string().default("http://platform-deploy-service.directus.svc.cluster.local:8080"),
  FLINK_REST_URL: z.string().default("http://flink-rest.kafka.svc.cluster.local:8081"),
  FLINK_JAR_NAME: z.string().default("platform-deploy-flink-job.jar"),
  FLINK_ENTRY_CLASS: z.string().default("com.suncoast.platform.deploy.flink.PlatformDeployJob"),
  FLINK_PARALLELISM: z.string().default("1"),
  PLATFORM_DEPLOY_PREPARED_TOPIC: z.string().default("batch.platform.deploy.prepared.v1"),
  PLATFORM_DEPLOY_SERVICE_URL: z.string().default("http://platform-deploy-service.directus.svc.cluster.local:8080"),
  OPERATION_CALLBACK_TOKEN_TTL_SECONDS: z.string().default("21600"),
  GITHUB_API_BASE: z.string().default("https://api.github.com"),
  TFE_API_BASE: z.string().default("https://app.terraform.io/api/v2"),
  DEFAULT_INITIAL_DEPLOY_WORKFLOW: z.string().default("initial-deploy.yml"),
  DEFAULT_GITHUB_PRODUCTION_REF: z.string().default("prod"),
  DEFAULT_GITHUB_PREVIEW_REF: z.string().default("dev"),
  TERRAFORM_RUN_TIMEOUT_SECONDS: z.string().default("7200"),
  TERRAFORM_RUN_POLL_SECONDS: z.string().default("20"),
  DEFAULT_OPENOBSERVE_BROWSER_RUM_VERSION: z.string().default("0.3.1"),
  DEFAULT_ORG_NAME: z.string().default("suncoast-systems")
});

const env = envSchema.parse(process.env);
const PORT = Math.max(1, Math.min(65535, Number(env.PORT) || 8080));
const RATE_WINDOW_MS = Math.max(1000, Number(env.RATE_WINDOW_MS) || 60_000);
const RATE_MAX = Math.max(1, Number(env.RATE_MAX) || 120);
const REQUEST_TIMEOUT_MS = Math.max(1000, Number(env.REQUEST_TIMEOUT_MS) || 10_000);
const DIRECTUS_BASE_URL = env.DIRECTUS_BASE_URL.replace(/\/+$/, "");
const DIRECTUS_HEALTH_PATH = env.DIRECTUS_HEALTH_PATH.startsWith("/")
  ? env.DIRECTUS_HEALTH_PATH
  : `/${env.DIRECTUS_HEALTH_PATH}`;
const VAULT_ADDR = env.VAULT_ADDR.replace(/\/+$/, "");
const TOKEN_CACHE_SECONDS = Math.max(5, Number(env.TOKEN_CACHE_SECONDS) || 300);
const FLINK_REST_URL = env.FLINK_REST_URL.replace(/\/+$/, "");
const FLINK_PARALLELISM = Math.max(1, Number(env.FLINK_PARALLELISM) || 1);
const OPERATION_CALLBACK_TOKEN_TTL_SECONDS = Math.max(300, Number(env.OPERATION_CALLBACK_TOKEN_TTL_SECONDS) || 21_600);
const GITHUB_API_BASE = env.GITHUB_API_BASE.replace(/\/+$/, "");
const TFE_API_BASE = env.TFE_API_BASE.replace(/\/+$/, "");
const TERRAFORM_RUN_TIMEOUT_SECONDS = Math.max(300, Number(env.TERRAFORM_RUN_TIMEOUT_SECONDS) || 7200);
const TERRAFORM_RUN_POLL_SECONDS = Math.max(5, Number(env.TERRAFORM_RUN_POLL_SECONDS) || 20);

type JsonRecord = Record<string, unknown>;
type OperationType = "create" | "update" | "redeploy" | "delete" | "destroy";
type OperationStatus = "queued" | "running" | "succeeded" | "failed" | "canceled";
type DeploymentStatus = "not_deployed" | "queued" | "deploying" | "deployed" | "failed" | "destroying" | "destroyed";
type DeploymentStrategy = "terraform_cloud" | "local_terraform";

type DirectusListResponse<T> = {
  data?: T[];
};

type DirectusItemResponse<T> = {
  data?: T;
};

type PlatformOrganization = {
  id: string;
  organization_key?: string | null;
  name?: string | null;
  default_domain?: string | null;
  settings_json?: JsonRecord | null;
};

type PlatformApp = {
  id: string;
  organization_id?: string | PlatformOrganization | null;
  app_key: string;
  display_name?: string | null;
  site_key: string;
  keycloak_realm: "internal" | "external";
  domain?: string | null;
  production_hostname?: string | null;
  preview_hostname?: string | null;
  production_url?: string | null;
  preview_url?: string | null;
  app_auth_slug_production?: string | null;
  app_auth_slug_preview?: string | null;
  deployment_strategy?: DeploymentStrategy | string | null;
  terraform_workspace_production?: string | null;
  terraform_workspace_preview?: string | null;
  terraform_project?: string | null;
  template_source_repo?: string | null;
  template_ref?: string | null;
  config_json?: JsonRecord | null;
};

type PlatformOperation = {
  id: string;
  app_id: string | PlatformApp;
  operation_type: OperationType;
  status: OperationStatus;
  result_json?: JsonRecord | null;
};

type VaultCacheEntry = {
  expiresAt: number;
  value: string;
};

const vaultCache = new Map<string, VaultCacheEntry>();

setGlobalDispatcher(
  new Agent({
    keepAliveTimeout: 10_000,
    keepAliveMaxTimeout: 10_000
  })
);

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null;
}

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function asBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "on"].includes(normalized)) return true;
    if (["false", "0", "no", "off"].includes(normalized)) return false;
  }
  return fallback;
}

function truncate(value: string, max = 1200): string {
  return value.length <= max ? value : `${value.slice(0, max)}...`;
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left, "utf8");
  const rightBuffer = Buffer.from(right, "utf8");
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

function bearerToken(req: Request): string {
  const authorization = asString(req.header("authorization"));
  return authorization.toLowerCase().startsWith("bearer ") ? authorization.slice(7).trim() : "";
}

function callbackTokenExpiresAt(): string {
  return new Date(Date.now() + OPERATION_CALLBACK_TOKEN_TTL_SECONDS * 1000).toISOString();
}

function operationPayloadBase64(payload: JsonRecord): string {
  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
}

function extractErrorMessage(payload: unknown, fallback: string): string {
  const root = asRecord(payload);
  const directError = asString(root?.error);
  if (directError) return directError;
  const errors = Array.isArray(root?.errors) ? root?.errors : [];
  const firstError = asRecord(errors[0]);
  const firstMessage = asString(firstError?.message);
  if (firstMessage) return firstMessage;
  return fallback;
}

async function parseJsonResponse(text: string): Promise<unknown> {
  if (!text.trim()) {
    return null;
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

async function httpJson<T>(
  url: string,
  init: {
    method?: Dispatcher.HttpMethod;
    headers?: Record<string, string>;
    body?: unknown;
    timeoutMs?: number;
    dispatcher?: Dispatcher;
  } = {}
): Promise<{ statusCode: number; payload: T; text: string }> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), init.timeoutMs ?? REQUEST_TIMEOUT_MS);
  try {
    const response = await undiciRequest(url, {
      method: init.method ?? "GET",
      body: init.body === undefined ? undefined : JSON.stringify(init.body),
      headers: {
        accept: "application/json",
        ...(init.body === undefined ? {} : { "content-type": "application/json" }),
        ...(init.headers ?? {})
      },
      dispatcher: init.dispatcher,
      signal: controller.signal
    });
    const text = await response.body.text();
    const payload = await parseJsonResponse(text);
    return { statusCode: response.statusCode, payload: payload as T, text };
  } finally {
    clearTimeout(timer);
  }
}

async function vaultToken(): Promise<string> {
  const token = await readFile(env.VAULT_TOKEN_FILE, "utf8");
  return token.trim();
}

async function vaultValue(path: string, key: string): Promise<string> {
  const cacheKey = `${path}#${key}`;
  const cached = vaultCache.get(cacheKey);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.value;
  }

  const token = await vaultToken();
  const normalizedPath = path.replace(/^\/+/, "").replace(/^v1\//, "");
  const result = await httpJson<JsonRecord>(`${VAULT_ADDR}/v1/${normalizedPath}`, {
    headers: { "x-vault-token": token },
    timeoutMs: REQUEST_TIMEOUT_MS
  });
  if (result.statusCode >= 400) {
    throw new Error(`Vault read ${path} failed: ${result.statusCode} ${truncate(result.text, 500)}`);
  }

  const payload = asRecord(result.payload) ?? {};
  const data = asRecord(payload.data) ?? {};
  const nested = asRecord(data.data);
  const value = asString((nested ?? data)[key]);
  if (!value) {
    throw new Error(`Vault read ${path} did not return key ${key}`);
  }
  vaultCache.set(cacheKey, {
    value,
    expiresAt: Date.now() + TOKEN_CACHE_SECONDS * 1000
  });
  return value;
}

async function directusToken(): Promise<string> {
  if (env.DIRECTUS_STATIC_TOKEN) {
    return env.DIRECTUS_STATIC_TOKEN;
  }
  return vaultValue(env.DIRECTUS_TOKEN_VAULT_PATH, env.DIRECTUS_TOKEN_VAULT_KEY);
}

async function internalToken(): Promise<string> {
  if (env.INTERNAL_TOKEN) {
    return env.INTERNAL_TOKEN;
  }
  if (!env.INTERNAL_TOKEN_VAULT_PATH) {
    return "";
  }
  return vaultValue(env.INTERNAL_TOKEN_VAULT_PATH, env.INTERNAL_TOKEN_VAULT_KEY);
}

async function directusJson<T>(path: string, init: { method?: Dispatcher.HttpMethod; body?: unknown; timeoutMs?: number } = {}): Promise<T> {
  const token = await directusToken();
  const result = await httpJson<unknown>(`${DIRECTUS_BASE_URL}${path}`, {
    method: init.method ?? "GET",
    body: init.body,
    timeoutMs: init.timeoutMs ?? REQUEST_TIMEOUT_MS,
    headers: { authorization: `Bearer ${token}` }
  });
  if (result.statusCode >= 400) {
    throw new Error(`Directus ${init.method ?? "GET"} ${path} failed: ${result.statusCode} ${truncate(extractErrorMessage(result.payload, result.text), 700)}`);
  }
  return result.payload as T;
}

function queryString(params: Record<string, string | number | boolean | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      search.set(key, String(value));
    }
  }
  const rendered = search.toString();
  return rendered ? `?${rendered}` : "";
}

async function getApp(appId: string): Promise<PlatformApp> {
  const fields = [
    "id",
    "organization_id.id",
    "organization_id.organization_key",
    "organization_id.name",
    "organization_id.default_domain",
    "organization_id.settings_json",
    "app_key",
    "display_name",
    "site_key",
    "keycloak_realm",
    "deployment_strategy",
    "domain",
    "production_hostname",
    "preview_hostname",
    "production_url",
    "preview_url",
    "app_auth_slug_production",
    "app_auth_slug_preview",
    "terraform_workspace_production",
    "terraform_workspace_preview",
    "terraform_project",
    "template_source_repo",
    "template_ref",
    "config_json"
  ].join(",");
  const response = await directusJson<DirectusItemResponse<PlatformApp>>(`/items/platform_apps/${encodeURIComponent(appId)}${queryString({ fields })}`);
  if (!response.data?.id) {
    throw Object.assign(new Error(`Platform app ${appId} was not found`), { status: 404 });
  }
  return response.data;
}

async function createOperation(
  app: PlatformApp,
  operationType: OperationType,
  inputJson: JsonRecord,
  executionProvider: DeploymentStrategy
): Promise<PlatformOperation> {
  const response = await directusJson<DirectusItemResponse<PlatformOperation>>("/items/platform_app_operations", {
    method: "POST",
    body: {
      id: randomUUID(),
      app_id: app.id,
      operation_type: operationType,
      status: "queued",
      execution_provider: executionProvider,
      requested_at: new Date().toISOString(),
      terraform_workspace: app.terraform_workspace_production || app.app_key,
      input_json: inputJson,
      result_json: {}
    }
  });
  if (!response.data?.id) {
    throw new Error("Directus did not return a platform operation id");
  }
  return response.data;
}

async function getOperation(operationId: string): Promise<PlatformOperation> {
  const fields = "id,app_id,operation_type,status,result_json";
  const response = await directusJson<DirectusItemResponse<PlatformOperation>>(
    `/items/platform_app_operations/${encodeURIComponent(operationId)}${queryString({ fields })}`
  );
  if (!response.data?.id) {
    throw Object.assign(new Error(`Platform operation ${operationId} was not found`), { status: 404 });
  }
  return response.data;
}

async function updateOperation(operationId: string, patch: JsonRecord): Promise<void> {
  await directusJson<DirectusItemResponse<PlatformOperation>>(`/items/platform_app_operations/${encodeURIComponent(operationId)}`, {
    method: "PATCH",
    body: patch
  });
}

async function updateApp(appId: string, patch: JsonRecord): Promise<void> {
  await directusJson<DirectusItemResponse<PlatformApp>>(`/items/platform_apps/${encodeURIComponent(appId)}`, {
    method: "PATCH",
    body: patch
  });
}

function organizationFromApp(app: PlatformApp): PlatformOrganization | null {
  return asRecord(app.organization_id) as PlatformOrganization | null;
}

function settingsFromApp(app: PlatformApp): JsonRecord {
  const organization = organizationFromApp(app);
  const orgSettings = asRecord(organization?.settings_json) ?? {};
  const appConfig = asRecord(app.config_json) ?? {};
  const deployment = asRecord(appConfig.deployment) ?? {};
  const orgAppRepository = asRecord(orgSettings.appRepository) ?? asRecord(orgSettings.app_repository) ?? {};
  const deploymentAppRepository = asRecord(deployment.appRepository) ?? asRecord(deployment.app_repository) ?? {};
  return {
    ...orgSettings,
    deployment: {
      ...(asRecord(orgSettings.deployment) ?? {}),
      ...(asRecord(deployment) ?? {})
    },
    appRepository: {
      ...orgAppRepository,
      ...deploymentAppRepository
    },
    template: {
      ...(asRecord(orgSettings.template) ?? {}),
      ...(asRecord(deployment.template) ?? {})
    }
  };
}

function deploymentSettings(app: PlatformApp): JsonRecord {
  return asRecord(settingsFromApp(app).deployment) ?? {};
}

function templateSettings(app: PlatformApp): JsonRecord {
  return asRecord(settingsFromApp(app).template) ?? {};
}

function repositorySettings(app: PlatformApp): JsonRecord {
  const settings = settingsFromApp(app);
  return {
    ...(asRecord(settings.template) ?? {}),
    ...(asRecord(settings.appRepository) ?? {})
  };
}

function domainFor(app: PlatformApp): string {
  return asString(app.domain)
    || asString(organizationFromApp(app)?.default_domain)
    || asString(deploymentSettings(app).baseDomain, "suncoast.systems");
}

function productionHostname(app: PlatformApp): string {
  return asString(app.production_hostname, `${app.app_key}.${domainFor(app)}`);
}

function previewHostname(app: PlatformApp): string {
  return asString(app.preview_hostname, `${app.app_key}-preview.${domainFor(app)}`);
}

function productionUrl(app: PlatformApp): string {
  return asString(app.production_url, `https://${productionHostname(app)}`);
}

function previewUrl(app: PlatformApp): string {
  return asString(app.preview_url, `https://${previewHostname(app)}`);
}

function sourceRepository(app: PlatformApp): string {
  return asString(app.template_source_repo) || asString(repositorySettings(app).repository);
}

function templateProdRef(app: PlatformApp): string {
  const settings = repositorySettings(app);
  return asString(app.template_ref) || asString(settings.prodRef) || asString(settings.ref, "prod");
}

function templatePreviewRef(app: PlatformApp): string {
  return asString(repositorySettings(app).previewRef, "dev");
}

function keycloakAuthHost(app: PlatformApp): string {
  return asString(deploymentSettings(app).keycloakAuthHost, "auth-origin.suncoast.systems");
}

function authGatewayUrl(app: PlatformApp): string {
  return asString(deploymentSettings(app).appAuthGatewayUrl);
}

function authGatewayAdminUrl(app: PlatformApp): string {
  return asString(deploymentSettings(app).appAuthGatewayAdminUrl);
}

function terraformProject(app: PlatformApp): string {
  return asString(app.terraform_project) || asString(deploymentSettings(app).terraformProject);
}

function githubSettings(app: PlatformApp): JsonRecord {
  const settings = settingsFromApp(app);
  return {
    ...(asRecord(settings.github) ?? {}),
    ...(asRecord(deploymentSettings(app).github) ?? {})
  };
}

function getStringAtPath(source: JsonRecord, paths: string[][]): string {
  for (const path of paths) {
    let current: unknown = source;
    for (const segment of path) {
      current = asRecord(current)?.[segment];
    }
    const value = asString(current);
    if (value) return value;
  }
  return "";
}

function deploymentStrategy(app: PlatformApp): DeploymentStrategy {
  const configured =
    asString(app.deployment_strategy)
    || getStringAtPath(deploymentSettings(app), [["deploymentStrategy"], ["deployment_strategy"], ["provider"]]);
  return configured === "local_terraform" ? "local_terraform" : "terraform_cloud";
}

function parseGitHubRepository(value: string): { owner: string; repo: string; fullName: string } | null {
  const candidate = value.trim().replace(/\.git$/i, "");
  const sshMatch = candidate.match(/^git@github\.com:([^/]+)\/(.+)$/i);
  if (sshMatch) {
    const owner = sshMatch[1].trim();
    const repo = sshMatch[2].trim();
    return owner && repo ? { owner, repo, fullName: `${owner}/${repo}` } : null;
  }

  const shorthandMatch = candidate.match(/^([^/\s]+)\/([^/\s]+)$/);
  if (shorthandMatch) {
    const owner = shorthandMatch[1].trim();
    const repo = shorthandMatch[2].trim();
    return owner && repo ? { owner, repo, fullName: `${owner}/${repo}` } : null;
  }

  try {
    const parsed = new URL(candidate);
    if (!parsed.hostname.toLowerCase().endsWith("github.com")) {
      return null;
    }
    const [owner, repo] = parsed.pathname.split("/").filter(Boolean);
    return owner && repo ? { owner, repo, fullName: `${owner}/${repo}` } : null;
  } catch {
    return null;
  }
}

function githubRepository(app: PlatformApp): { owner: string; repo: string; fullName: string } | null {
  const configured = asString(githubSettings(app).repository)
    || asString(githubSettings(app).repo)
    || sourceRepository(app);
  return configured ? parseGitHubRepository(configured) : null;
}

function githubRef(app: PlatformApp, operationType: OperationType): string {
  const settings = githubSettings(app);
  if (operationType === "destroy" || operationType === "delete") {
    return asString(settings.destroyRef)
      || asString(settings.destroy_ref)
      || asString(settings.productionRef)
      || asString(settings.production_ref)
      || templateProdRef(app)
      || env.DEFAULT_GITHUB_PRODUCTION_REF;
  }
  return asString(settings.productionRef)
    || asString(settings.production_ref)
    || asString(settings.prodRef)
    || asString(settings.prod_ref)
    || templateProdRef(app)
    || env.DEFAULT_GITHUB_PRODUCTION_REF;
}

function initialDeployWorkflow(app: PlatformApp): string {
  const settings = githubSettings(app);
  return asString(settings.initialDeployWorkflow)
    || asString(settings.initial_deploy_workflow)
    || asString(settings.initialWorkflow)
    || env.DEFAULT_INITIAL_DEPLOY_WORKFLOW;
}

function tfeAgentPoolId(app: PlatformApp): string {
  const settings = deploymentSettings(app);
  const github = githubSettings(app);
  return asString(github.tfeAgentPoolId)
    || asString(github.tfe_agent_pool_id)
    || asString(settings.tfeAgentPoolId)
    || asString(settings.tfe_agent_pool_id);
}

function terraformCloudOrganization(app: PlatformApp): string {
  const repository = githubRepository(app);
  const settings = deploymentSettings(app);
  const github = githubSettings(app);
  return asString(settings.terraformCloudOrganization)
    || asString(settings.terraform_cloud_organization)
    || asString(settings.tfCloudOrganization)
    || asString(settings.tf_cloud_organization)
    || asString(github.terraformCloudOrganization)
    || asString(github.terraform_cloud_organization)
    || asString(github.tfCloudOrganization)
    || asString(github.tf_cloud_organization)
    || repository?.owner
    || env.DEFAULT_ORG_NAME;
}

function openObserveBrowserRumVersion(app: PlatformApp): string {
  const settings = deploymentSettings(app);
  const github = githubSettings(app);
  return asString(github.openObserveBrowserRumVersion)
    || asString(github.openobserve_browser_rum_version)
    || asString(settings.openObserveBrowserRumVersion)
    || asString(settings.openobserve_browser_rum_version)
    || env.DEFAULT_OPENOBSERVE_BROWSER_RUM_VERSION;
}

function githubRepositoryVariables(app: PlatformApp): JsonRecord {
  const configuredVariables =
    asRecord(githubSettings(app).variables)
    || asRecord(githubSettings(app).repoVariables)
    || asRecord(githubSettings(app).repository_variables)
    || {};
  const variables: JsonRecord = {
    KEYCLOAK_REALM: app.keycloak_realm,
    TFE_PROJECT: terraformProject(app),
    DIRECTUS_CONTENT_SITE_KEY: app.site_key,
    KEYCLOAK_AUTH_HOST: keycloakAuthHost(app),
    APP_AUTH_GATEWAY_URL: authGatewayUrl(app),
    APP_AUTH_GATEWAY_ADMIN_URL: authGatewayAdminUrl(app),
    APP_BASE_DOMAIN: domainFor(app),
    APP_AUTH_BASE_URL_PRODUCTION: productionUrl(app),
    APP_AUTH_BASE_URL_PREVIEW: previewUrl(app),
    ...configuredVariables
  };

  if (!asString(variables.TFE_AGENT_POOL_ID)) {
    const agentPoolId = tfeAgentPoolId(app);
    if (agentPoolId) variables.TFE_AGENT_POOL_ID = agentPoolId;
  }
  if (!asString(variables.OPENOBSERVE_BROWSER_RUM_VERSION)) {
    variables.OPENOBSERVE_BROWSER_RUM_VERSION = openObserveBrowserRumVersion(app);
  }

  return Object.fromEntries(Object.entries(variables).filter(([, value]) => asString(value) !== ""));
}

function operationSequence(operationType: OperationType): string {
  if (operationType === "delete" || operationType === "destroy") {
    return "destroy";
  }
  if (operationType === "update" || operationType === "redeploy") {
    return "recreate";
  }
  return "create";
}

function appIdFromOperation(operation: PlatformOperation): string {
  return typeof operation.app_id === "string" ? operation.app_id : asString(operation.app_id?.id);
}

function buildRunnerInput(app: PlatformApp, operationType: OperationType, operationId: string): JsonRecord {
  const sequence = operationSequence(operationType);
  const repository = githubRepository(app);
  return {
    operation_id: operationId,
    operation_type: operationType,
    sequence,
    deployment_strategy: deploymentStrategy(app),
    app_id: app.id,
    app_key: app.app_key,
    site_key: app.site_key,
    keycloak_realm: app.keycloak_realm,
    domain: domainFor(app),
    production_hostname: productionHostname(app),
    preview_hostname: previewHostname(app),
    production_url: productionUrl(app),
    preview_url: previewUrl(app),
    source_repository: sourceRepository(app),
    template_prod_ref: templateProdRef(app),
    template_preview_ref: templatePreviewRef(app),
    terraform_project: terraformProject(app),
    terraform_cloud_organization: terraformCloudOrganization(app),
    tfe_agent_pool_id: tfeAgentPoolId(app),
    keycloak_auth_host: keycloakAuthHost(app),
    app_auth_gateway_url: authGatewayUrl(app),
    app_auth_gateway_admin_url: authGatewayAdminUrl(app),
    app_auth_slug_production: asString(app.app_auth_slug_production, app.app_key),
    app_auth_slug_preview: asString(app.app_auth_slug_preview, `${app.app_key}-preview`),
    terraform_workspace_production: asString(app.terraform_workspace_production, app.app_key),
    terraform_workspace_preview: asString(app.terraform_workspace_preview, `${app.app_key}-preview`),
    github_api_base: GITHUB_API_BASE,
    github_repository: repository?.fullName ?? "",
    github_initial_workflow: initialDeployWorkflow(app),
    github_ref: githubRef(app, operationType),
    tfe_api_base: TFE_API_BASE,
    terraform_run_timeout_seconds: TERRAFORM_RUN_TIMEOUT_SECONDS,
    terraform_run_poll_seconds: TERRAFORM_RUN_POLL_SECONDS,
    openobserve_browser_rum_version: openObserveBrowserRumVersion(app),
    github_repository_variables: githubRepositoryVariables(app)
  };
}

async function resolveFlinkJarId(): Promise<string> {
  const result = await httpJson<JsonRecord>(`${FLINK_REST_URL}/jars`, { timeoutMs: REQUEST_TIMEOUT_MS });
  if (result.statusCode >= 400) {
    throw new Error(`Flink jar list failed: ${result.statusCode} ${truncate(result.text, 1000)}`);
  }

  const payload = asRecord(result.payload) ?? {};
  const files = Array.isArray(payload.files) ? payload.files : [];
  const candidates = files
    .map((file) => asRecord(file))
    .filter((file): file is JsonRecord => Boolean(file))
    .filter((file) => {
      const id = asString(file.id);
      const name = asString(file.name);
      return id.endsWith(env.FLINK_JAR_NAME) || name.endsWith(env.FLINK_JAR_NAME);
    })
    .sort((left, right) => Number(right.uploaded ?? 0) - Number(left.uploaded ?? 0));

  const jarId = asString(candidates[0]?.id);
  if (!jarId) {
    throw new Error(`Flink jar ${env.FLINK_JAR_NAME} is not uploaded.`);
  }
  return jarId;
}

async function submitFlinkPrepareJob(
  app: PlatformApp,
  operation: PlatformOperation,
  input: JsonRecord,
  operationToken: string
): Promise<{ jarId: string; jobId: string }> {
  const jarId = await resolveFlinkJarId();
  const args = [
    "--operation-id", operation.id,
    "--app-id", app.id,
    "--operation-type", operation.operation_type,
    "--operation-token", operationToken,
    "--operation-payload-base64", operationPayloadBase64(input),
    "--prepared-topic", env.PLATFORM_DEPLOY_PREPARED_TOPIC,
    "--platform-deploy-service-url", env.PLATFORM_DEPLOY_SERVICE_URL,
    "--source", "platform-deploy-service"
  ];

  const result = await httpJson<JsonRecord>(`${FLINK_REST_URL}/jars/${encodeURIComponent(jarId)}/run`, {
    method: "POST",
    timeoutMs: REQUEST_TIMEOUT_MS,
    body: {
      entryClass: env.FLINK_ENTRY_CLASS,
      parallelism: FLINK_PARALLELISM,
      programArgsList: args
    }
  });
  if (result.statusCode >= 400) {
    throw new Error(`Flink prepare job submission failed: ${result.statusCode} ${truncate(result.text, 1000)}`);
  }

  const payload = asRecord(result.payload) ?? {};
  const jobId = asString(payload.jobid) || asString(payload.jobId);
  if (!jobId) {
    throw new Error(`Flink prepare job submission did not return a job id: ${truncate(result.text, 1000)}`);
  }
  return { jarId, jobId };
}

async function enforceInternalAuth(req: Request): Promise<void> {
  const expected = await internalToken();
  if (!expected) {
    return;
  }
  const actual = asString(req.header("authorization"));
  if (!safeEqual(actual, `Bearer ${expected}`)) {
    throw Object.assign(new Error("Unauthorized"), { status: 401 });
  }
}

async function enforceInternalOrOperationAuth(req: Request, operationId: string): Promise<PlatformOperation> {
  const operation = await getOperation(operationId);
  const expectedInternal = await internalToken();
  const authorization = asString(req.header("authorization"));
  if (!expectedInternal || safeEqual(authorization, `Bearer ${expectedInternal}`)) {
    return operation;
  }

  const token = bearerToken(req);
  const resultJson = asRecord(operation.result_json) ?? {};
  const expectedTokenHash = asString(resultJson.prepare_token_sha256);
  const expiresAt = asString(resultJson.prepare_token_expires_at);
  if (
    token
    && expectedTokenHash
    && (!expiresAt || Date.parse(expiresAt) > Date.now())
    && safeEqual(sha256(token), expectedTokenHash)
  ) {
    return operation;
  }

  throw Object.assign(new Error("Unauthorized"), { status: 401 });
}

function operationTypeFromBody(body: JsonRecord, fallback: OperationType): OperationType {
  const value = asString(body.operation_type) || asString(body.operationType);
  if (value === "create" || value === "update" || value === "redeploy" || value === "delete" || value === "destroy") {
    return value;
  }
  return fallback;
}

async function queueOperation(appId: string, operationType: OperationType): Promise<JsonRecord> {
  const app = await getApp(appId);
  const appSourceRepo = sourceRepository(app);
  if (!appSourceRepo) {
    throw Object.assign(new Error("App source repository is not configured."), { status: 422 });
  }
  const executionProvider = deploymentStrategy(app);
  if (executionProvider === "terraform_cloud") {
    if (!terraformProject(app)) {
      throw Object.assign(new Error("TFE_PROJECT is required. Set the app Terraform project before deploying."), { status: 422 });
    }
    if (!githubRepository(app)) {
      throw Object.assign(new Error("A GitHub repository is required for Terraform Cloud deployments."), { status: 422 });
    }
  }

  const operationInput = buildRunnerInput(app, operationType, "pending");
  const operation = await createOperation(app, operationType, operationInput, executionProvider);
  const input = buildRunnerInput(app, operationType, operation.id);
  await updateOperation(operation.id, { input_json: input });

  const queuedStatus: DeploymentStatus = operationSequence(operationType) === "destroy" ? "destroying" : "queued";
  await updateApp(app.id, {
    deployment_status: queuedStatus,
    last_error: null
  });

  try {
    const operationToken = randomBytes(32).toString("base64url");
    const tokenExpiresAt = callbackTokenExpiresAt();
    await updateOperation(operation.id, {
      result_json: {
        prepare_token_sha256: sha256(operationToken),
        prepare_token_expires_at: tokenExpiresAt,
        prepared_topic: env.PLATFORM_DEPLOY_PREPARED_TOPIC
      }
    });
    const flinkJob = await submitFlinkPrepareJob(app, operation, input, operationToken);
    const currentOperation = await getOperation(operation.id);
    await updateOperation(operation.id, {
      result_json: {
        ...(asRecord(currentOperation.result_json) ?? {}),
        prepare_token_sha256: sha256(operationToken),
        prepare_token_expires_at: tokenExpiresAt,
        prepared_topic: env.PLATFORM_DEPLOY_PREPARED_TOPIC,
        flink_jar_id: flinkJob.jarId,
        flink_job_id: flinkJob.jobId,
        prepare_submitted_at: new Date().toISOString()
      }
    });
    return {
      ok: true,
      app_id: app.id,
      operation_id: operation.id,
      flink_job_id: flinkJob.jobId,
      prepared_topic: env.PLATFORM_DEPLOY_PREPARED_TOPIC
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to create deploy job.";
    await updateOperation(operation.id, {
      status: "failed",
      finished_at: new Date().toISOString(),
      error_message: message
    });
    await updateApp(app.id, {
      deployment_status: "failed",
      last_error: message
    });
    throw error;
  }
}

const openApiSpec = {
  openapi: "3.0.3",
  info: {
    title: "Platform Deploy API",
    version: "1.0.0",
    description: "Internal deployment control plane for Directus-managed shell app deployments."
  },
  servers: [{ url: env.OPENAPI_SERVER_URL }],
  components: {
    parameters: {
      AppIdPath: {
        name: "id",
        in: "path",
        required: true,
        schema: { type: "string", format: "uuid" },
        description: "Directus platform_apps id."
      }
    },
    schemas: {
      QueueDeployRequest: {
        type: "object",
        additionalProperties: false,
        properties: {
          operation_type: {
            type: "string",
            enum: ["create", "update", "redeploy"],
            description: "Deployment operation to queue."
          }
        }
      },
      QueueDestroyRequest: {
        type: "object",
        additionalProperties: false,
        properties: {
          operation_type: {
            type: "string",
            enum: ["destroy", "delete"],
            description: "Destroy operation to queue."
          }
        }
      },
      QueueOperationResponse: {
        type: "object",
        required: ["ok", "app_id", "operation_id", "flink_job_id", "prepared_topic"],
        properties: {
          ok: { type: "boolean" },
          app_id: { type: "string" },
          operation_id: { type: "string" },
          flink_job_id: { type: "string" },
          prepared_topic: { type: "string" }
        },
        additionalProperties: true
      },
      ErrorResponse: {
        type: "object",
        properties: {
          error: {
            type: "object",
            properties: {
              message: { type: "string" },
              status: { type: "integer" }
            }
          }
        }
      }
    }
  },
  paths: {
    "/healthz": { get: { operationId: "healthz", responses: { "200": { description: "Service health" } } } },
    "/readyz": { get: { operationId: "readyz", responses: { "200": { description: "Dependency readiness" } } } },
    "/internal/apps/{id}/deploy": {
      post: {
        operationId: "queueDeploy",
        parameters: [{ $ref: "#/components/parameters/AppIdPath" }],
        requestBody: {
          required: false,
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/QueueDeployRequest" }
            }
          }
        },
        responses: {
          "200": {
            description: "Deploy queued",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QueueOperationResponse" }
              }
            }
          },
          "422": {
            description: "Deploy configuration is incomplete",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ErrorResponse" }
              }
            }
          }
        }
      }
    },
    "/internal/apps/{id}/destroy": {
      post: {
        operationId: "queueDestroy",
        parameters: [{ $ref: "#/components/parameters/AppIdPath" }],
        requestBody: {
          required: false,
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/QueueDestroyRequest" }
            }
          }
        },
        responses: {
          "200": {
            description: "Destroy queued",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/QueueOperationResponse" }
              }
            }
          }
        }
      }
    }
  }
};

const app = express();
app.set("trust proxy", Math.max(0, Number(env.TRUST_PROXY_HOPS) || 1));
app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json({ limit: "512kb" }));
app.use(morgan("combined"));
app.use(rateLimit({ windowMs: RATE_WINDOW_MS, limit: RATE_MAX, standardHeaders: "draft-7", legacyHeaders: false }));

app.get("/healthz", (_req, res) => {
  res.status(200).json({ ok: true, service: "platform-deploy-service", version: "1.0.0" });
});

app.get("/readyz", async (_req, res) => {
  try {
    const token = await directusToken();
    const result = await httpJson<unknown>(`${DIRECTUS_BASE_URL}${DIRECTUS_HEALTH_PATH}`, {
      headers: { authorization: `Bearer ${token}` },
      timeoutMs: REQUEST_TIMEOUT_MS
    });
    if (result.statusCode >= 400) {
      res.status(503).json({ ok: false, reason: "directus_health_failed", status: result.statusCode });
      return;
    }
    const flinkResult = await httpJson<unknown>(`${FLINK_REST_URL}/overview`, { timeoutMs: REQUEST_TIMEOUT_MS });
    if (flinkResult.statusCode >= 400) {
      res.status(503).json({ ok: false, reason: "flink_health_failed", status: flinkResult.statusCode });
      return;
    }
    res.status(200).json({ ok: true, directus_base_url: DIRECTUS_BASE_URL, flink_rest_url: FLINK_REST_URL });
  } catch (error) {
    res.status(503).json({
      ok: false,
      reason: "platform_deploy_dependency_failed",
      error: error instanceof Error ? truncate(error.message, 500) : "Unknown readiness error"
    });
  }
});

app.get("/openapi.json", (_req, res) => {
  res.status(200).json(openApiSpec);
});

app.post("/internal/apps/:id/deploy", async (req, res, next) => {
  try {
    await enforceInternalAuth(req);
    const body = asRecord(req.body) ?? {};
    const result = await queueOperation(req.params.id, operationTypeFromBody(body, "redeploy"));
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/internal/apps/:id/destroy", async (req, res, next) => {
  try {
    await enforceInternalAuth(req);
    const body = asRecord(req.body) ?? {};
    const result = await queueOperation(req.params.id, operationTypeFromBody(body, "destroy"));
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/internal/operations/:id/start", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    const body = asRecord(req.body) ?? {};
    const appId = asString(body.app_id) || appIdFromOperation(operation);
    const operationType = operationTypeFromBody(body, operation.operation_type);
    await updateOperation(req.params.id, {
      status: "running",
      started_at: new Date().toISOString()
    });
    if (appId) {
      await updateApp(appId, {
        deployment_status: operationSequence(operationType) === "destroy" ? "destroying" : "deploying",
        last_error: null
      });
    }
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post("/internal/operations/:id/prepared", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    const body = asRecord(req.body) ?? {};
    const bodyResultJson = asRecord(body.result_json) ?? {};
    const preparedAt = asString(body.prepared_at, new Date().toISOString());
    const preparedTopic = asString(body.prepared_topic, env.PLATFORM_DEPLOY_PREPARED_TOPIC);
    await updateOperation(req.params.id, {
      result_json: {
        ...(asRecord(operation.result_json) ?? {}),
        ...bodyResultJson,
        prepared_at: preparedAt,
        prepared_topic: preparedTopic
      }
    });
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post("/internal/operations/:id/finish", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    const body = asRecord(req.body) ?? {};
    const appId = asString(body.app_id) || appIdFromOperation(operation);
    const operationType = operationTypeFromBody(body, operation.operation_type);
    const succeeded = asString(body.status) === "succeeded";
    const deploymentStatus: DeploymentStatus = succeeded
      ? operationSequence(operationType) === "destroy" ? "destroyed" : "deployed"
      : "failed";
    const errorMessage = asString(body.error_message);
    const bodyResultJson = asRecord(body.result_json) ?? {};
    await updateOperation(req.params.id, {
      status: succeeded ? "succeeded" : "failed",
      finished_at: new Date().toISOString(),
      result_json: {
        ...(asRecord(operation.result_json) ?? {}),
        ...bodyResultJson
      },
      error_message: errorMessage || null,
      log_excerpt: asString(body.log_excerpt) || null,
      terraform_run_id: asString(body.terraform_run_id) || undefined,
      terraform_run_url: asString(body.terraform_run_url) || undefined
    });
    if (appId) {
      await updateApp(appId, {
        deployment_status: deploymentStatus,
        last_deployed_at: succeeded && deploymentStatus === "deployed" ? new Date().toISOString() : undefined,
        last_error: succeeded ? null : errorMessage || "Deployment failed."
      });
    }
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.use((_req, res) => {
  res.status(404).json({ error: { message: "Not found", status: 404 } });
});

app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
  const status = Math.max(400, Math.min(599, Number((err as { status?: number }).status) || 500));
  res.status(status).json({
    error: {
      message: err instanceof Error ? err.message : "Internal server error",
      status
    }
  });
});

app.listen(PORT, () => {
  console.log(`[platform-deploy-service] listening on :${PORT}`);
});
