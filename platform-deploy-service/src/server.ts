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
  DIRECTUS_TOKEN_VAULT_PATH: z.string().default("secret/data/directus/gravitee/clients/<path:secret/data/keycloak-client-id-graphql-api#value>"),
  DIRECTUS_TOKEN_VAULT_KEY: z.string().default("token"),
  INTERNAL_TOKEN: z.string().default(""),
  INTERNAL_TOKEN_VAULT_PATH: z.string().default("secret/data/platform-deploy-service"),
  INTERNAL_TOKEN_VAULT_KEY: z.string().default("token"),
  RETURN_PLATFORM_DEPLOY_SECRET_VALUES: z.string().default("true"),
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
  PLATFORM_DEPLOY_NOTIFICATIONS_ENABLED: z.string().default("true"),
  PLATFORM_NOTIFICATION_SERVICE_URL: z.string().default("http://platform-notification-service.directus.svc.cluster.local:8080"),
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
const VAULT_PATH_REF_PATTERN = /<path:([^#>]+)#([^>]+)>/g;
const FLINK_REST_URL = env.FLINK_REST_URL.replace(/\/+$/, "");
const PLATFORM_DEPLOY_NOTIFICATIONS_ENABLED = asBoolean(env.PLATFORM_DEPLOY_NOTIFICATIONS_ENABLED, true);
const PLATFORM_NOTIFICATION_SERVICE_URL = env.PLATFORM_NOTIFICATION_SERVICE_URL.replace(/\/+$/, "");
const FLINK_PARALLELISM = Math.max(1, Number(env.FLINK_PARALLELISM) || 1);
const OPERATION_CALLBACK_TOKEN_TTL_SECONDS = Math.max(300, Number(env.OPERATION_CALLBACK_TOKEN_TTL_SECONDS) || 21_600);
const GITHUB_API_BASE = env.GITHUB_API_BASE.replace(/\/+$/, "");
const TFE_API_BASE = env.TFE_API_BASE.replace(/\/+$/, "");
const TERRAFORM_RUN_TIMEOUT_SECONDS = Math.max(300, Number(env.TERRAFORM_RUN_TIMEOUT_SECONDS) || 7200);
const TERRAFORM_RUN_POLL_SECONDS = Math.max(5, Number(env.TERRAFORM_RUN_POLL_SECONDS) || 20);

type JsonRecord = Record<string, unknown>;
type OperationType = "create" | "update" | "redeploy" | "delete" | "destroy";
type OperationStatus = "queued" | "running" | "succeeded" | "failed" | "canceled";
type OperationStepStatus = OperationStatus;
type DeploymentStatus = "not_deployed" | "queued" | "deploying" | "deployed" | "failed" | "destroying" | "destroyed";
type DeploymentStrategy = "terraform_cloud" | "local_terraform";
const ACTIVE_OPERATION_STATUSES = new Set<OperationStatus>(["queued", "running"]);

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

type NotificationChannel = "browser_push" | "email" | "sms";

type PlatformOperationStep = {
  id: string;
  operation_id: string | PlatformOperation;
  app_id?: string | PlatformApp | null;
  step_key: string;
  step_label?: string | null;
  status: OperationStepStatus;
  sequence?: number | null;
  message?: string | null;
  result_json?: JsonRecord | null;
  error_message?: string | null;
  log_excerpt?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  date_created?: string | null;
  date_updated?: string | null;
};

type PlatformDeploySecretsInput = {
  tfe_token: string;
  tfe_agent_pool_id: string;
  tfe_organization: string;
  cloudflare_token: string;
  cloudflare_account_id: string;
  cloudflare_zone_id: string;
  github_token: string;
};

const emptyPlatformDeploySecrets = (): PlatformDeploySecretsInput => ({
  tfe_token: "",
  tfe_agent_pool_id: "",
  tfe_organization: "",
  cloudflare_token: "",
  cloudflare_account_id: "",
  cloudflare_zone_id: "",
  github_token: ""
});

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

function redactText(value: string): string {
  return value
    .replace(/Bearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [redacted]")
    .replace(/([?&](?:token|access_token|refresh_token|key|secret)=)[^&\s]+/gi, "$1[redacted]");
}

function redactJsonValue(value: unknown, key = ""): unknown {
  if (/token|secret|password|credential|private[_-]?key/i.test(key)) {
    return "[redacted]";
  }
  if (typeof value === "string") {
    return redactText(value);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => redactJsonValue(entry));
  }
  const record = asRecord(value);
  if (record) {
    return Object.fromEntries(Object.entries(record).map(([entryKey, entryValue]) => [
      entryKey,
      redactJsonValue(entryValue, entryKey)
    ]));
  }
  return value;
}

function redactJsonRecord(value: JsonRecord): JsonRecord {
  return redactJsonValue(value) as JsonRecord;
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

async function vaultValueRaw(path: string, key: string): Promise<string> {
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

async function resolveVaultPath(path: string): Promise<string> {
  const matches = [...path.matchAll(VAULT_PATH_REF_PATTERN)];
  if (!matches.length) {
    return path;
  }
  let resolved = path;
  for (const match of matches) {
    const token = match[0];
    const refPath = match[1];
    const refKey = match[2];
    const refValue = await vaultValueRaw(refPath, refKey);
    resolved = resolved.replace(token, refValue);
  }
  return resolved;
}

async function vaultValue(path: string, key: string): Promise<string> {
  return vaultValueRaw(await resolveVaultPath(path), key);
}

async function vaultKv2Data(path: string): Promise<JsonRecord> {
  const token = await vaultToken();
  const resolvedPath = await resolveVaultPath(path);
  const normalizedPath = resolvedPath.replace(/^\/+/, "").replace(/^v1\//, "");
  const result = await httpJson<JsonRecord>(`${VAULT_ADDR}/v1/${normalizedPath}`, {
    headers: { "x-vault-token": token },
    timeoutMs: REQUEST_TIMEOUT_MS
  });
  if (result.statusCode === 404) {
    return {};
  }
  if (result.statusCode >= 400) {
    throw new Error(`Vault read ${resolvedPath} failed: ${result.statusCode} ${truncate(result.text, 500)}`);
  }
  const payload = asRecord(result.payload) ?? {};
  const data = asRecord(payload.data) ?? {};
  return asRecord(data.data) ?? data;
}

async function writeVaultKv2Data(path: string, patch: JsonRecord, removeKeys: string[] = []): Promise<void> {
  const token = await vaultToken();
  const resolvedPath = await resolveVaultPath(path);
  const normalizedPath = resolvedPath.replace(/^\/+/, "").replace(/^v1\//, "");
  const existing = await vaultKv2Data(path);
  for (const key of removeKeys) {
    delete existing[key];
  }
  const nextData = {
    ...existing,
    ...patch
  };
  const result = await httpJson<JsonRecord>(`${VAULT_ADDR}/v1/${normalizedPath}`, {
    method: "POST",
    body: { data: nextData },
    headers: { "x-vault-token": token },
    timeoutMs: REQUEST_TIMEOUT_MS
  });
  if (result.statusCode >= 400) {
    throw new Error(`Vault write ${resolvedPath} failed: ${result.statusCode} ${truncate(result.text, 500)}`);
  }
  for (const key of [...Object.keys(patch), ...removeKeys]) {
    vaultCache.delete(`${resolvedPath}#${key}`);
  }
}

function platformDeploySecretsInput(body: JsonRecord): PlatformDeploySecretsInput {
  const candidate = asRecord(body.body) ?? body;
  const input: PlatformDeploySecretsInput = {
    tfe_token: asString(candidate.tfe_token),
    tfe_agent_pool_id: asString(candidate.tfe_agent_pool_id),
    tfe_organization: asString(candidate.tfe_organization),
    cloudflare_token: asString(candidate.cloudflare_token),
    cloudflare_account_id: asString(candidate.cloudflare_account_id),
    cloudflare_zone_id: asString(candidate.cloudflare_zone_id),
    github_token: asString(candidate.github_token)
  };
  const missing = Object.entries(input)
    .filter(([, value]) => !value.trim())
    .map(([key]) => key);
  if (missing.length) {
    throw Object.assign(new Error(`Missing required platform deploy secret values: ${missing.join(", ")}`), { status: 422 });
  }
  if (!input.tfe_agent_pool_id.startsWith("apool-")) {
    throw Object.assign(new Error("tfe_agent_pool_id must start with apool-."), { status: 422 });
  }
  return input;
}

function platformDeploySecretsOutput(
  serviceData: JsonRecord,
  githubData: JsonRecord,
): PlatformDeploySecretsInput {
  return {
    tfe_token: asString(serviceData.tfe_token, asString(serviceData["tfe-token"], asString(serviceData.tf_api_token))),
    tfe_agent_pool_id: asString(serviceData.tfe_agent_pool_id, asString(serviceData["tfe-agent-pool-id"])),
    tfe_organization: asString(
      serviceData.tfe_organization,
      asString(serviceData["tfe-organization"], asString(serviceData.tf_cloud_organization)),
    ),
    cloudflare_token: asString(serviceData.cloudflare_token, asString(serviceData["cloudflare-token"])),
    cloudflare_account_id: asString(serviceData.cloudflare_account_id, asString(serviceData["cloudflare-account-id"])),
    cloudflare_zone_id: asString(serviceData.cloudflare_zone_id, asString(serviceData["cloudflare-zone-id"])),
    github_token: asString(
      githubData.token,
      asString(githubData.github_token, asString(serviceData.github_token, asString(serviceData["github-token"]))),
    )
  };
}

function configuredPlatformDeploySecretKeys(values: PlatformDeploySecretsInput): string[] {
  return Object.entries(values)
    .filter(([, value]) => value.trim().length > 0)
    .map(([key]) => key);
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

async function acceptedServiceAuthTokens(): Promise<string[]> {
  const tokens = new Set<string>();
  const expectedInternal = await internalToken();
  if (expectedInternal) {
    tokens.add(expectedInternal);
  }
  try {
    const expectedDirectus = await directusToken();
    if (expectedDirectus) {
      tokens.add(expectedDirectus);
    }
  } catch (error) {
    console.warn(`[platform-deploy-service] directus auth token unavailable: ${error instanceof Error ? truncate(error.message, 500) : "unknown error"}`);
  }
  return [...tokens];
}

async function directusJson<T>(path: string, init: { method?: Dispatcher.HttpMethod; body?: unknown; timeoutMs?: number } = {}): Promise<T> {
  const token = await directusToken();
  const method = init.method ?? "GET";
  const result = await httpJson<unknown>(`${DIRECTUS_BASE_URL}${path}`, {
    method,
    body: init.body,
    timeoutMs: init.timeoutMs ?? REQUEST_TIMEOUT_MS,
    headers: {
      authorization: `Bearer ${token}`,
      ...(method === "GET" ? { "cache-control": "no-store" } : {})
    }
  });
  if (result.statusCode >= 400) {
    throw new Error(`Directus ${method} ${path} failed: ${result.statusCode} ${truncate(extractErrorMessage(result.payload, result.text), 700)}`);
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

function operationActive(operation: PlatformOperation): boolean {
  return ACTIVE_OPERATION_STATUSES.has(operation.status);
}

function operationStatusPayload(operation: PlatformOperation): JsonRecord {
  const appId = appIdFromOperation(operation);
  return {
    ok: true,
    operation_id: operation.id,
    app_id: appId || null,
    operation_type: operation.operation_type,
    status: operation.status,
    active: operationActive(operation)
  };
}

function terminalOperationIgnoredPayload(operation: PlatformOperation, callbackName: string): JsonRecord {
  return {
    ...operationStatusPayload(operation),
    ignored: true,
    reason: `Operation is ${operation.status}; ${callbackName} callback was not applied.`
  };
}

function requireActiveOperation(operation: PlatformOperation, callbackName: string): void {
  if (!operationActive(operation)) {
    throw Object.assign(
      new Error(`Operation ${operation.id} is ${operation.status}; ${callbackName} callback cannot be applied.`),
      { status: 409 }
    );
  }
}

async function getActiveOperationForApp(appId: string): Promise<PlatformOperation | null> {
  const params = new URLSearchParams();
  params.set("fields", "id,operation_type,status");
  params.set("filter[app_id][_eq]", appId);
  params.set("sort", "-date_created");
  params.set("limit", "25");
  const response = await directusJson<DirectusListResponse<PlatformOperation>>(
    `/items/platform_app_operations?${params.toString()}`
  );
  return response.data?.find((operation) => operation.status === "queued" || operation.status === "running") ?? null;
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

function operationStepStatus(value: unknown, fallback: OperationStepStatus): OperationStepStatus {
  const normalized = asString(value).toLowerCase();
  if (normalized === "queued" || normalized === "running" || normalized === "succeeded" || normalized === "failed" || normalized === "canceled") {
    return normalized;
  }
  return fallback;
}

function stepSequence(stepKey: string): number {
  const order: Record<string, number> = {
    queued: 5,
    prepare: 10,
    "prepare-submit": 12,
    orchestration: 20,
    "prod-deploy": 30,
    "preview-deploy": 40,
    "preview-destroy": 50,
    "prod-destroy": 60,
    finish: 90
  };
  return order[stepKey] ?? 500;
}

function stepLabel(stepKey: string): string {
  return stepKey
    .split("-")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function stringList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((entry) => asString(entry)).filter(Boolean);
  }
  const direct = asString(value);
  return direct ? direct.split(",").map((entry) => entry.trim()).filter(Boolean) : [];
}

function notificationChannels(value: unknown): NotificationChannel[] {
  const channels = stringList(value)
    .map((entry) => entry.toLowerCase())
    .filter((entry): entry is NotificationChannel => entry === "browser_push" || entry === "email" || entry === "sms");
  return [...new Set(channels)];
}

function notificationContextForStep(operation: PlatformOperation, resultJson: JsonRecord): JsonRecord {
  return asRecord(resultJson.notification_context)
    ?? asRecord((asRecord(operation.result_json) ?? {}).notification_context)
    ?? {};
}

function notificationChannelsForContext(context: JsonRecord): NotificationChannel[] {
  const browserPush = asRecord(context.browser_push) ?? {};
  if (asBoolean(browserPush.available, false) && asString(browserPush.subscription_id)) {
    return ["browser_push"];
  }
  const fallbackChannels = notificationChannels(context.fallback_channels);
  return fallbackChannels.length ? fallbackChannels : ["email", "sms"];
}

function notificationRecipientsForContext(context: JsonRecord, channels: NotificationChannel[]): JsonRecord[] {
  const browserPush = asRecord(context.browser_push) ?? {};
  const user = asRecord(context.user) ?? {};
  const recipients: JsonRecord[] = [];

  if (channels.includes("browser_push")) {
    const subscriptionId = asString(browserPush.subscription_id);
    if (subscriptionId) {
      recipients.push({
        type: "browser_subscription",
        id: subscriptionId,
        channels: ["browser_push"],
        data: {
          browser_installation_id: asString(browserPush.browser_installation_id) || null,
          notification_context_id: asString(context.context_id) || null,
          notification_thread_id: asString(context.thread_id) || null
        }
      });
    }
    return recipients;
  }

  const email = asString(user.email);
  const phone = asString(user.phone);
  if (channels.includes("email") && email) {
    recipients.push({
      type: "email",
      address: email,
      display_name: asString(user.display_name) || undefined,
      channels: ["email"]
    });
  }
  if (channels.includes("sms") && phone) {
    recipients.push({
      type: "phone",
      address: phone,
      display_name: asString(user.display_name) || undefined,
      channels: ["sms"]
    });
  }
  if (!recipients.length && asString(user.user_id)) {
    recipients.push({
      type: "user",
      id: asString(user.user_id),
      display_name: asString(user.display_name) || undefined,
      channels
    });
  }
  return recipients;
}

function notificationSeverityForStep(status: OperationStepStatus): string {
  if (status === "failed" || status === "canceled") {
    return "error";
  }
  if (status === "succeeded") {
    return "success";
  }
  return "info";
}

function notificationPriorityForStep(status: OperationStepStatus): string {
  return status === "failed" || status === "canceled" ? "high" : "normal";
}

async function emitPlatformOperationStepNotification(
  operation: PlatformOperation,
  stepKey: string,
  event: JsonRecord
): Promise<void> {
  if (!PLATFORM_DEPLOY_NOTIFICATIONS_ENABLED || !PLATFORM_NOTIFICATION_SERVICE_URL) {
    return;
  }

  const resultJson = asRecord(event.result_json) ?? {};
  const context = notificationContextForStep(operation, resultJson);
  if (!Object.keys(context).length) {
    return;
  }

  const status = operationStepStatus(event.status, "running");
  const appId = asString(event.app_id) || appIdFromOperation(operation);
  const stepTitle = asString(event.step_label) || stepLabel(stepKey);
  const message = truncate(redactText(asString(event.message, `${stepTitle} is ${status}.`)), 2000);
  const channels = notificationChannelsForContext(context);
  const browserPush = asRecord(context.browser_push) ?? {};
  const fallbackChannels = notificationChannels(context.fallback_channels);
  const recipients = notificationRecipientsForContext(context, channels);
  const user = asRecord(context.user) ?? {};

  try {
    const token = await directusToken();
    const result = await httpJson<JsonRecord>(`${PLATFORM_NOTIFICATION_SERVICE_URL}/internal/notifications`, {
      method: "POST",
      timeoutMs: REQUEST_TIMEOUT_MS,
      headers: { authorization: `Bearer ${token}` },
      body: {
        event_key: `platform.deploy.${operation.operation_type}.${stepKey}.${status}`,
        source: "platform-deploy-service",
        severity: notificationSeverityForStep(status),
        priority: notificationPriorityForStep(status),
        app_id: appId || undefined,
        actor_user_id: asString(user.user_id) || undefined,
        channels,
        recipients,
        subject: `Platform ${operation.operation_type}: ${stepTitle} ${status}`,
        body: message,
        data: {
          operation_id: operation.id,
          operation_type: operation.operation_type,
          app_id: appId || null,
          step_key: stepKey,
          step_label: stepTitle,
          status,
          message,
          result_json: redactJsonRecord(resultJson),
          error_message: asString(event.error_message) || null
        },
        metadata: {
          notification_context_id: asString(context.context_id) || null,
          notification_thread_id: asString(context.thread_id, operation.id),
          requested_at: asString(context.requested_at) || null,
          selected_channels: channels,
          fallback_channels: fallbackChannels,
          fallback_reason: channels.includes("browser_push") ? null : asString(browserPush.reason, "browser_push_unavailable"),
          browser_push: redactJsonRecord(browserPush),
          source_step_status: status
        },
        idempotency_key: `platform-deploy:${operation.id}:${stepKey}:${status}`,
        correlation_id: asString(context.thread_id, operation.id)
      }
    });
    if (result.statusCode >= 400) {
      console.warn(`[platform-deploy-service] notification request failed HTTP ${result.statusCode}: ${truncate(result.text, 700)}`);
    }
  } catch (error) {
    console.warn(`[platform-deploy-service] notification request failed: ${error instanceof Error ? truncate(error.message, 700) : "unknown error"}`);
  }
}

function optionalInt(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(0, Math.floor(parsed)) : undefined;
}

function isDirectusOperationStepUniqueError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes("platform_app_operation_steps")
    && (message.includes("unique") || message.includes("duplicate"));
}

function sleepMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function operationStepId(operationId: string, stepKey: string): string {
  const digest = sha256(`${operationId}:${stepKey}`).slice(0, 32);
  return [
    digest.slice(0, 8),
    digest.slice(8, 12),
    digest.slice(12, 16),
    digest.slice(16, 20),
    digest.slice(20, 32)
  ].join("-");
}

async function getOperationStep(operationId: string, stepKey: string): Promise<PlatformOperationStep | null> {
  const params = new URLSearchParams();
  params.set("fields", "id,operation_id,app_id,step_key,status,result_json");
  params.set("filter[operation_id][_eq]", operationId);
  params.set("sort", "sequence,date_created");
  params.set("limit", "100");
  params.set("_cb", randomUUID());
  const response = await directusJson<DirectusListResponse<PlatformOperationStep>>(
    `/items/platform_app_operation_steps?${params.toString()}`
  );
  return response.data?.find((step) => step.step_key === stepKey) ?? null;
}

async function listOperationSteps(operationId: string): Promise<PlatformOperationStep[]> {
  const params = new URLSearchParams();
  params.set("fields", [
    "id",
    "operation_id",
    "app_id",
    "step_key",
    "step_label",
    "status",
    "sequence",
    "message",
    "result_json",
    "error_message",
    "log_excerpt",
    "started_at",
    "finished_at",
    "duration_ms",
    "date_created",
    "date_updated"
  ].join(","));
  params.set("filter[operation_id][_eq]", operationId);
  params.set("sort", "sequence,date_created");
  params.set("limit", "100");
  const response = await directusJson<DirectusListResponse<PlatformOperationStep>>(
    `/items/platform_app_operation_steps?${params.toString()}`
  );
  return response.data ?? [];
}

async function upsertOperationStep(
  operation: PlatformOperation,
  stepKey: string,
  values: JsonRecord = {}
): Promise<void> {
  const now = new Date().toISOString();
  const status = operationStepStatus(values.status, "running");
  const appId = asString(values.app_id) || appIdFromOperation(operation);
  const existing = await getOperationStep(operation.id, stepKey);
  const deterministicStepId = operationStepId(operation.id, stepKey);
  const incomingResult = redactJsonRecord(asRecord(values.result_json) ?? asRecord(values.result) ?? {});
  const durationMs = optionalInt(values.duration_ms);
  const notificationEvent = (): JsonRecord => ({
    app_id: appId || undefined,
    step_label: asString(values.step_label) || asString(values.label) || stepLabel(stepKey),
    status,
    message: truncate(redactText(asString(values.message)), 2000) || `${stepLabel(stepKey)} is ${status}.`,
    result_json: incomingResult,
    error_message: truncate(redactText(asString(values.error_message)), 4000) || undefined,
    started_at: asString(values.started_at) || undefined,
    finished_at: asString(values.finished_at) || undefined,
    duration_ms: durationMs
  });
  const buildPatch = (step: PlatformOperationStep | null): JsonRecord => {
    const existingResult = redactJsonRecord(asRecord(step?.result_json) ?? {});
    const patch: JsonRecord = {
      operation_id: operation.id,
      app_id: appId || undefined,
      step_key: stepKey,
      step_label: asString(values.step_label) || asString(values.label) || stepLabel(stepKey),
      status,
      sequence: optionalInt(values.sequence) ?? stepSequence(stepKey),
      message: truncate(redactText(asString(values.message)), 2000) || null,
      result_json: {
        ...existingResult,
        ...incomingResult
      },
      error_message: truncate(redactText(asString(values.error_message)), 4000) || null,
      log_excerpt: truncate(redactText(asString(values.log_excerpt)), 12000) || null,
      started_at: asString(values.started_at) || (status === "running" && !step ? now : undefined),
      finished_at: asString(values.finished_at) || (status === "succeeded" || status === "failed" || status === "canceled" ? now : undefined),
      date_updated: now
    };
    if (durationMs !== undefined) {
      patch.duration_ms = durationMs;
    }
    return patch;
  };

  const updateExistingStep = async (step: PlatformOperationStep): Promise<void> => {
    await directusJson<DirectusItemResponse<PlatformOperationStep>>(`/items/platform_app_operation_steps/${encodeURIComponent(step.id)}`, {
      method: "PATCH",
      body: buildPatch(step)
    });
  };

  const updateDeterministicStep = async (): Promise<boolean> => {
    try {
      await directusJson<DirectusItemResponse<PlatformOperationStep>>(`/items/platform_app_operation_steps/${encodeURIComponent(deterministicStepId)}`, {
        method: "PATCH",
        body: buildPatch({
          id: deterministicStepId,
          operation_id: operation.id,
          app_id: appId || null,
          step_key: stepKey,
          status,
          result_json: {}
        })
      });
      return true;
    } catch {
      return false;
    }
  };

  const updateExistingStepByUniqueFilter = async (): Promise<boolean> => {
    const patch = buildPatch({
      id: deterministicStepId,
      operation_id: operation.id,
      app_id: appId || null,
      step_key: stepKey,
      status,
      result_json: {}
    });
    delete patch.operation_id;
    delete patch.step_key;

    try {
      const response = await directusJson<DirectusListResponse<PlatformOperationStep>>("/items/platform_app_operation_steps", {
        method: "PATCH",
        body: {
          query: {
            filter: {
              _and: [
                { operation_id: { _eq: operation.id } },
                { step_key: { _eq: stepKey } }
              ]
            }
          },
          data: patch
        }
      });
      return Boolean(response.data?.length);
    } catch {
      return false;
    }
  };

  if (existing?.id) {
    await updateExistingStep(existing);
    await emitPlatformOperationStepNotification(operation, stepKey, notificationEvent());
    return;
  }

  try {
    await directusJson<DirectusItemResponse<PlatformOperationStep>>("/items/platform_app_operation_steps", {
      method: "POST",
      body: {
        id: deterministicStepId,
        date_created: now,
        ...buildPatch(null)
      }
    });
    await emitPlatformOperationStepNotification(operation, stepKey, notificationEvent());
  } catch (error) {
    if (!isDirectusOperationStepUniqueError(error)) {
      throw error;
    }

    if (await updateDeterministicStep()) {
      await emitPlatformOperationStepNotification(operation, stepKey, notificationEvent());
      return;
    }

    if (await updateExistingStepByUniqueFilter()) {
      await emitPlatformOperationStepNotification(operation, stepKey, notificationEvent());
      return;
    }

    for (let attempt = 1; attempt <= 6; attempt += 1) {
      await sleepMs(100 * attempt);
      const racedExisting = await getOperationStep(operation.id, stepKey);
      if (racedExisting?.id) {
        await updateExistingStep(racedExisting);
        await emitPlatformOperationStepNotification(operation, stepKey, notificationEvent());
        return;
      }
    }
    throw error;
  }
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

function buildRunnerInput(
  app: PlatformApp,
  operationType: OperationType,
  operationId: string,
  notificationContext: JsonRecord = {}
): JsonRecord {
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
    github_repository_variables: githubRepositoryVariables(app),
    ...(Object.keys(notificationContext).length ? { notification_context: notificationContext } : {})
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
  const expectedTokens = await acceptedServiceAuthTokens();
  if (!expectedTokens.length) {
    throw Object.assign(new Error("Internal auth is not configured."), { status: 503 });
  }
  const actual = asString(req.header("authorization"));
  if (!expectedTokens.some((token) => safeEqual(actual, `Bearer ${token}`))) {
    throw Object.assign(new Error("Unauthorized"), { status: 401 });
  }
}

async function enforceInternalOrOperationAuth(req: Request, operationId: string): Promise<PlatformOperation> {
  const operation = await getOperation(operationId);
  const expectedTokens = await acceptedServiceAuthTokens();
  const authorization = asString(req.header("authorization"));
  if (expectedTokens.some((token) => safeEqual(authorization, `Bearer ${token}`))) {
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

function unwrapActionBody(body: unknown): JsonRecord {
  let current = asRecord(body) ?? {};
  for (let depth = 0; depth < 4; depth += 1) {
    const input = asRecord(current.input);
    if (input) {
      current = input;
      continue;
    }
    const nestedBody = asRecord(current.body);
    if (nestedBody) {
      current = nestedBody;
      continue;
    }
    break;
  }
  return current;
}

function notificationContextFromBody(body: JsonRecord): JsonRecord {
  const context = asRecord(body.notification_context) ?? asRecord(body.notificationContext) ?? {};
  return redactJsonRecord(context);
}

async function queueOperation(appId: string, operationType: OperationType, body: JsonRecord = {}): Promise<JsonRecord> {
  const app = await getApp(appId);
  const activeOperation = await getActiveOperationForApp(app.id);
  if (activeOperation) {
    throw Object.assign(
      new Error(`Platform app ${app.app_key} already has a ${activeOperation.status} ${activeOperation.operation_type} operation.`),
      { status: 409 }
    );
  }

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

  const notificationContext = notificationContextFromBody(body);
  const operationInput = buildRunnerInput(app, operationType, "pending", notificationContext);
  const operation = await createOperation(app, operationType, operationInput, executionProvider);
  const input = buildRunnerInput(app, operationType, operation.id, notificationContext);
  await updateOperation(operation.id, { input_json: input });
  await upsertOperationStep(operation, "queued", {
    status: "queued",
    app_id: app.id,
    message: `Queued ${operationType} for ${app.keycloak_realm}/${app.app_key}.`,
    result_json: {
      operation_type: operationType,
      sequence: operationSequence(operationType),
      deployment_strategy: executionProvider,
      ...(Object.keys(notificationContext).length ? { notification_context: notificationContext } : {})
    }
  });

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
        prepared_topic: env.PLATFORM_DEPLOY_PREPARED_TOPIC,
        ...(Object.keys(notificationContext).length ? { notification_context: notificationContext } : {})
      }
    });
    await upsertOperationStep(operation, "prepare-submit", {
      status: "running",
      app_id: app.id,
      message: "Submitting deployment prepare job to Flink.",
      result_json: {
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
    await upsertOperationStep(operation, "prepare-submit", {
      status: "succeeded",
      app_id: app.id,
      message: "Flink prepare job was submitted.",
      result_json: {
        flink_jar_id: flinkJob.jarId,
        flink_job_id: flinkJob.jobId,
        prepared_topic: env.PLATFORM_DEPLOY_PREPARED_TOPIC
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
    await upsertOperationStep(operation, "prepare-submit", {
      status: "failed",
      app_id: app.id,
      message: "Failed to submit deployment prepare job to Flink.",
      error_message: message,
      log_excerpt: error instanceof Error ? error.stack || error.message : String(error)
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
      },
      OperationIdPath: {
        name: "id",
        in: "path",
        required: true,
        schema: { type: "string", format: "uuid" },
        description: "Directus platform_app_operations id."
      },
      StepKeyPath: {
        name: "stepKey",
        in: "path",
        required: true,
        schema: { type: "string", minLength: 1 },
        description: "Stable deployment step key such as prod-deploy or preview-destroy."
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
          },
          notification_context: {
            type: "object",
            additionalProperties: true,
            description: "Optional browser/fallback notification routing context supplied by the requesting UI."
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
          },
          notification_context: {
            type: "object",
            additionalProperties: true,
            description: "Optional browser/fallback notification routing context supplied by the requesting UI."
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
      OperationStepStatusRequest: {
        type: "object",
        additionalProperties: true,
        properties: {
          status: {
            type: "string",
            enum: ["queued", "running", "succeeded", "failed", "canceled"]
          },
          app_id: { type: "string", format: "uuid" },
          step_label: { type: "string" },
          sequence: { type: "integer", minimum: 0 },
          message: { type: "string" },
          result_json: { type: "object", additionalProperties: true },
          error_message: { type: "string" },
          log_excerpt: { type: "string" },
          started_at: { type: "string", format: "date-time" },
          finished_at: { type: "string", format: "date-time" },
          duration_ms: { type: "integer", minimum: 0 }
        }
      },
      OperationStep: {
        type: "object",
        additionalProperties: true,
        properties: {
          id: { type: "string", format: "uuid" },
          operation_id: { type: "string", format: "uuid" },
          app_id: { type: "string", format: "uuid", nullable: true },
          step_key: { type: "string" },
          step_label: { type: "string", nullable: true },
          status: {
            type: "string",
            enum: ["queued", "running", "succeeded", "failed", "canceled"]
          },
          sequence: { type: "integer" },
          message: { type: "string", nullable: true },
          result_json: { type: "object", additionalProperties: true },
          error_message: { type: "string", nullable: true },
          log_excerpt: { type: "string", nullable: true },
          started_at: { type: "string", format: "date-time", nullable: true },
          finished_at: { type: "string", format: "date-time", nullable: true },
          duration_ms: { type: "integer", nullable: true },
          date_created: { type: "string", format: "date-time" },
          date_updated: { type: "string", format: "date-time", nullable: true }
        }
      },
      OperationStepStatusResponse: {
        type: "object",
        required: ["ok"],
        additionalProperties: false,
        properties: {
          ok: { type: "boolean" },
          ignored: { type: "boolean" },
          reason: { type: "string" },
          operation_id: { type: "string", format: "uuid" },
          app_id: { type: "string", format: "uuid", nullable: true },
          operation_type: {
            type: "string",
            enum: ["create", "update", "redeploy", "delete", "destroy"]
          },
          status: {
            type: "string",
            enum: ["queued", "running", "succeeded", "failed", "canceled"]
          },
          active: { type: "boolean" }
        }
      },
      OperationStatusResponse: {
        type: "object",
        required: ["ok", "operation_id", "operation_type", "status", "active"],
        additionalProperties: false,
        properties: {
          ok: { type: "boolean" },
          operation_id: { type: "string", format: "uuid" },
          app_id: { type: "string", format: "uuid", nullable: true },
          operation_type: {
            type: "string",
            enum: ["create", "update", "redeploy", "delete", "destroy"]
          },
          status: {
            type: "string",
            enum: ["queued", "running", "succeeded", "failed", "canceled"]
          },
          active: { type: "boolean" }
        }
      },
      ListOperationStepsResponse: {
        type: "object",
        required: ["ok", "operation_id", "steps"],
        additionalProperties: false,
        properties: {
          ok: { type: "boolean" },
          operation_id: { type: "string", format: "uuid" },
          steps: {
            type: "array",
            items: { $ref: "#/components/schemas/OperationStep" }
          }
        }
      },
      SavePlatformDeploySecretsRequest: {
        type: "object",
        required: [
          "tfe_token",
          "tfe_agent_pool_id",
          "tfe_organization",
          "cloudflare_token",
          "cloudflare_account_id",
          "cloudflare_zone_id",
          "github_token"
        ],
        additionalProperties: false,
        properties: {
          tfe_token: { type: "string", minLength: 1 },
          tfe_agent_pool_id: { type: "string", minLength: 1 },
          tfe_organization: { type: "string", minLength: 1 },
          cloudflare_token: { type: "string", minLength: 1 },
          cloudflare_account_id: { type: "string", minLength: 1 },
          cloudflare_zone_id: { type: "string", minLength: 1 },
          github_token: { type: "string", minLength: 1 }
        }
      },
      GetPlatformDeploySecretsResponse: {
        type: "object",
        required: [
          "ok",
          "values_returned",
          "values_redacted",
          "configured_keys",
          "tfe_token",
          "tfe_agent_pool_id",
          "tfe_organization",
          "cloudflare_token",
          "cloudflare_account_id",
          "cloudflare_zone_id",
          "github_token"
        ],
        additionalProperties: false,
        properties: {
          ok: { type: "boolean" },
          values_returned: { type: "boolean" },
          values_redacted: { type: "boolean" },
          configured_keys: {
            type: "array",
            items: { type: "string" }
          },
          tfe_token: { type: "string" },
          tfe_agent_pool_id: { type: "string" },
          tfe_organization: { type: "string" },
          cloudflare_token: { type: "string" },
          cloudflare_account_id: { type: "string" },
          cloudflare_zone_id: { type: "string" },
          github_token: { type: "string" }
        }
      },
      SavePlatformDeploySecretsResponse: {
        type: "object",
        required: ["ok", "vault_paths", "saved_keys", "saved_keys_by_path"],
        properties: {
          ok: { type: "boolean" },
          vault_paths: {
            type: "array",
            items: { type: "string" }
          },
          saved_keys: {
            type: "array",
            items: { type: "string" }
          },
          saved_keys_by_path: {
            type: "object",
            additionalProperties: {
              type: "array",
              items: { type: "string" }
            }
          }
        },
        additionalProperties: false
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
    },
    "/internal/operations/{id}/steps": {
      get: {
        operationId: "listOperationSteps",
        parameters: [{ $ref: "#/components/parameters/OperationIdPath" }],
        responses: {
          "200": {
            description: "Detailed status steps for a platform app operation",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ListOperationStepsResponse" }
              }
            }
          }
        }
      }
    },
    "/internal/operations/{id}/status": {
      get: {
        operationId: "getOperationStatus",
        parameters: [{ $ref: "#/components/parameters/OperationIdPath" }],
        responses: {
          "200": {
            description: "Current operation status",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/OperationStatusResponse" }
              }
            }
          }
        }
      }
    },
    "/internal/operations/{id}/steps/{stepKey}": {
      post: {
        operationId: "recordOperationStep",
        parameters: [
          { $ref: "#/components/parameters/OperationIdPath" },
          { $ref: "#/components/parameters/StepKeyPath" }
        ],
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/OperationStepStatusRequest" }
            }
          }
        },
        responses: {
          "200": {
            description: "Step status recorded",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/OperationStepStatusResponse" }
              }
            }
          },
          "422": {
            description: "Step key or payload was invalid",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ErrorResponse" }
              }
            }
          }
        }
      }
    },
    "/internal/secrets/platform-deploy": {
      get: {
        operationId: "getPlatformDeploySecrets",
        responses: {
          "200": {
            description: "Platform deploy secrets loaded from Vault",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/GetPlatformDeploySecretsResponse" }
              }
            }
          }
        }
      },
      post: {
        operationId: "savePlatformDeploySecrets",
        requestBody: {
          required: true,
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/SavePlatformDeploySecretsRequest" }
            }
          }
        },
        responses: {
          "200": {
            description: "Platform deploy secrets saved to Vault",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/SavePlatformDeploySecretsResponse" }
              }
            }
          },
          "422": {
            description: "One or more required secret fields are missing",
            content: {
              "application/json": {
                schema: { $ref: "#/components/schemas/ErrorResponse" }
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
  const dependencies: JsonRecord = {};

  try {
    await internalToken();
    dependencies.internal_token = { ok: true };
  } catch (error) {
    res.status(503).json({
      ok: false,
      reason: "internal_token_unavailable",
      dependencies,
      error: error instanceof Error ? truncate(error.message, 500) : "Unknown internal token readiness error"
    });
    return;
  }

  try {
    const token = await directusToken();
    const result = await httpJson<unknown>(`${DIRECTUS_BASE_URL}${DIRECTUS_HEALTH_PATH}`, {
      headers: { authorization: `Bearer ${token}` },
      timeoutMs: REQUEST_TIMEOUT_MS
    });
    dependencies.directus = { ok: result.statusCode < 400, status: result.statusCode };
  } catch (error) {
    dependencies.directus = {
      ok: false,
      error: error instanceof Error ? truncate(error.message, 500) : "Unknown Directus readiness error"
    };
  }

  try {
    const flinkResult = await httpJson<unknown>(`${FLINK_REST_URL}/overview`, { timeoutMs: REQUEST_TIMEOUT_MS });
    dependencies.flink = { ok: flinkResult.statusCode < 400, status: flinkResult.statusCode };
  } catch (error) {
    dependencies.flink = {
      ok: false,
      error: error instanceof Error ? truncate(error.message, 500) : "Unknown Flink readiness error"
    };
  }

  res.status(200).json({
    ok: true,
    directus_base_url: DIRECTUS_BASE_URL,
    flink_rest_url: FLINK_REST_URL,
    dependencies
  });
});

app.get("/openapi.json", (_req, res) => {
  res.status(200).json(openApiSpec);
});

app.post("/internal/apps/:id/deploy", async (req, res, next) => {
  try {
    await enforceInternalAuth(req);
    const body = unwrapActionBody(req.body);
    const result = await queueOperation(req.params.id, operationTypeFromBody(body, "redeploy"), body);
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/internal/apps/:id/destroy", async (req, res, next) => {
  try {
    await enforceInternalAuth(req);
    const body = unwrapActionBody(req.body);
    const result = await queueOperation(req.params.id, operationTypeFromBody(body, "destroy"), body);
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

app.get("/internal/secrets/platform-deploy", async (req, res, next) => {
  try {
    await enforceInternalAuth(req);
    const [serviceData, githubData] = await Promise.all([
      vaultKv2Data("secret/data/platform-deploy-service"),
      vaultKv2Data("secret/data/platform-deploy-service/github")
    ]);
    const values = platformDeploySecretsOutput(serviceData, githubData);
    const valuesReturned = asBoolean(env.RETURN_PLATFORM_DEPLOY_SECRET_VALUES, true);
    res.status(200).json({
      ok: true,
      values_returned: valuesReturned,
      values_redacted: !valuesReturned,
      configured_keys: configuredPlatformDeploySecretKeys(values),
      ...(valuesReturned ? values : emptyPlatformDeploySecrets())
    });
  } catch (error) {
    next(error);
  }
});

app.post("/internal/secrets/platform-deploy", async (req, res, next) => {
  try {
    await enforceInternalAuth(req);
    const body = unwrapActionBody(req.body);
    const input = platformDeploySecretsInput(body);
    await writeVaultKv2Data("secret/data/platform-deploy-service", {
      tfe_token: input.tfe_token,
      tfe_agent_pool_id: input.tfe_agent_pool_id,
      tfe_organization: input.tfe_organization,
      cloudflare_token: input.cloudflare_token,
      cloudflare_account_id: input.cloudflare_account_id,
      cloudflare_zone_id: input.cloudflare_zone_id,
      github_token: input.github_token
    }, ["app_auth_gateway_admin_token", "app-auth-gateway-admin-token"]);
    await writeVaultKv2Data("secret/data/platform-deploy-service/github", {
      token: input.github_token,
      github_token: input.github_token
    });
    res.status(200).json({
      ok: true,
      vault_paths: [
        "secret/data/platform-deploy-service",
        "secret/data/platform-deploy-service/github"
      ],
      saved_keys: [
        "tfe_token",
        "tfe_agent_pool_id",
        "tfe_organization",
        "cloudflare_token",
        "cloudflare_account_id",
        "cloudflare_zone_id",
        "github_token"
      ],
      saved_keys_by_path: {
        "secret/data/platform-deploy-service": [
          "tfe_token",
          "tfe_agent_pool_id",
          "tfe_organization",
          "cloudflare_token",
          "cloudflare_account_id",
          "cloudflare_zone_id",
          "github_token"
        ],
        "secret/data/platform-deploy-service/github": [
          "token",
          "github_token"
        ]
      }
    });
  } catch (error) {
    next(error);
  }
});

app.post("/internal/operations/:id/start", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    requireActiveOperation(operation, "start");
    const body = unwrapActionBody(req.body);
    const appId = asString(body.app_id) || appIdFromOperation(operation);
    const operationType = operationTypeFromBody(body, operation.operation_type);
    await updateOperation(req.params.id, {
      status: "running",
      started_at: new Date().toISOString()
    });
    await upsertOperationStep(operation, "orchestration", {
      status: "running",
      app_id: appId,
      message: "NiFi deployment orchestration started.",
      result_json: {
        operation_type: operationType,
        sequence: operationSequence(operationType)
      }
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
    requireActiveOperation(operation, "prepared");
    const body = unwrapActionBody(req.body);
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
    await upsertOperationStep(operation, "prepare", {
      status: "succeeded",
      app_id: asString(body.app_id) || appIdFromOperation(operation),
      message: "Flink prepared the deployment payload and published it to NiFi.",
      result_json: {
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

app.post("/internal/operations/:id/steps/:stepKey", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    if (!operationActive(operation)) {
      res.status(200).json(terminalOperationIgnoredPayload(operation, "step"));
      return;
    }
    const body = unwrapActionBody(req.body);
    const stepKey = asString(req.params.stepKey);
    if (!stepKey) {
      throw Object.assign(new Error("stepKey is required."), { status: 422 });
    }
    await upsertOperationStep(operation, stepKey, body);
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.get("/internal/operations/:id/steps", async (req, res, next) => {
  try {
    await enforceInternalOrOperationAuth(req, req.params.id);
    const steps = await listOperationSteps(req.params.id);
    res.status(200).json({ ok: true, operation_id: req.params.id, steps });
  } catch (error) {
    next(error);
  }
});

app.get("/internal/operations/:id/status", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    res.status(200).json(operationStatusPayload(operation));
  } catch (error) {
    next(error);
  }
});

app.post("/internal/operations/:id/finish", async (req, res, next) => {
  try {
    const operation = await enforceInternalOrOperationAuth(req, req.params.id);
    if (!operationActive(operation)) {
      res.status(200).json(terminalOperationIgnoredPayload(operation, "finish"));
      return;
    }
    const body = unwrapActionBody(req.body);
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
    await upsertOperationStep(operation, "finish", {
      status: succeeded ? "succeeded" : "failed",
      app_id: appId,
      message: succeeded ? "Deployment orchestration finished successfully." : "Deployment orchestration failed.",
      result_json: bodyResultJson,
      error_message: errorMessage || null,
      log_excerpt: asString(body.log_excerpt) || null
    });
    await upsertOperationStep(operation, "orchestration", {
      status: succeeded ? "succeeded" : "failed",
      app_id: appId,
      message: succeeded ? "NiFi deployment orchestration completed." : "NiFi deployment orchestration failed before completion.",
      result_json: bodyResultJson,
      error_message: errorMessage || null,
      log_excerpt: asString(body.log_excerpt) || null
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
