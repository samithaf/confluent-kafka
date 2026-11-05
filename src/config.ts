import "dotenv/config";

export type Role = "producer" | "consumer" | "admin";
export type GroupProtocol = "consumer" | "classic";
export type AutoOffsetReset = "earliest" | "latest";

export interface AppConfig {
  role: Role;
  bootstrapServers: string;
  apiKey: string;
  apiSecret: string;
  topics: string[];
  consumerGroup: string;
  clientId: string;
  groupProtocol: GroupProtocol;
  autoOffsetReset: AutoOffsetReset;
  port: number;
  shutdownTimeoutMs: number;
  partitionsConsumedConcurrently: number;
  commitIntervalMessages: number;
  serviceName: string;
  serviceRevision: string;
  region: string;
}

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function optional(name: string, fallback: string): string {
  return process.env[name]?.trim() || fallback;
}

function integer(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got: ${raw}`);
  }
  return parsed;
}

function oneOf<T extends string>(name: string, allowed: readonly T[], fallback: T): T {
  const value = process.env[name]?.trim() || fallback;
  if (!(allowed as readonly string[]).includes(value)) {
    throw new Error(`${name} must be one of ${allowed.join(", ")}, got: ${value}`);
  }
  return value as T;
}

export function loadConfig(role: Role): AppConfig {
  const topic = process.env.KAFKA_TOPIC?.trim();
  const topicList = process.env.KAFKA_TOPICS?.trim();
  const topics = (topicList || topic || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);

  if (role === "producer" && topics.length !== 1) {
    throw new Error("The producer service requires exactly one topic in KAFKA_TOPIC or KAFKA_TOPICS");
  }
  if (role === "consumer" && topics.length === 0) {
    throw new Error("KAFKA_TOPIC or KAFKA_TOPICS is required for the consumer worker");
  }

  const serviceName = optional("K_SERVICE", "local");
  const roleSuffix = role === "admin" ? "admin" : role;

  return {
    role,
    bootstrapServers: required("KAFKA_BOOTSTRAP_SERVERS"),
    apiKey: required("KAFKA_API_KEY"),
    apiSecret: required("KAFKA_API_SECRET"),
    topics,
    consumerGroup: role === "consumer" ? required("KAFKA_CONSUMER_GROUP") : optional("KAFKA_CONSUMER_GROUP", "unused"),
    clientId: optional("KAFKA_CLIENT_ID", `cloud-run-${roleSuffix}-${serviceName}`),
    groupProtocol: oneOf<GroupProtocol>("KAFKA_GROUP_PROTOCOL", ["consumer", "classic"], "consumer"),
    autoOffsetReset: oneOf<AutoOffsetReset>("KAFKA_AUTO_OFFSET_RESET", ["earliest", "latest"], "earliest"),
    port: integer("PORT", 8080),
    shutdownTimeoutMs: integer("SHUTDOWN_TIMEOUT_MS", 8000),
    partitionsConsumedConcurrently: integer("KAFKA_PARTITIONS_CONSUMED_CONCURRENTLY", 4),
    commitIntervalMessages: integer("KAFKA_COMMIT_INTERVAL_MESSAGES", 100),
    serviceName,
    serviceRevision: optional("K_REVISION", "local"),
    region: optional("GOOGLE_CLOUD_REGION", "unknown"),
  };
}
