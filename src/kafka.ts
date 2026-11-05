import { KafkaJS } from "@confluentinc/kafka-javascript";
import type { AppConfig } from "./config";
import { errorDetails, type AppLogger } from "./logger";

const { Kafka } = KafkaJS;

interface RebalancePartition {
  topic: string;
  partition: number;
  offset?: number;
}

interface RebalanceFunctions {
  assign(assignment: RebalancePartition[]): void;
  unassign(): void;
  assignmentLost(): boolean;
}

type RebalanceCallback = (
  error: { code: number; message?: string },
  assignment: RebalancePartition[] | null,
  functions: RebalanceFunctions,
) => void | Promise<void>;

const commonConfig = {
  "bootstrap.servers": "",
  "security.protocol": "sasl_ssl",
  "sasl.mechanisms": "PLAIN",
  "client.dns.lookup": "use_all_dns_ips",
  "socket.keepalive.enable": true,
  "socket.timeout.ms": 30000,
  "reconnect.backoff.ms": 1000,
  "reconnect.backoff.max.ms": 10000,
  "statistics.interval.ms": 30000,
  log_level: 3,
} as const;

function connectionConfig(config: AppConfig) {
  return {
    ...commonConfig,
    "bootstrap.servers": config.bootstrapServers,
    "sasl.username": config.apiKey,
    "sasl.password": config.apiSecret,
    "client.id": config.clientId,
  };
}

function createKafka(config: AppConfig): KafkaJS.Kafka {
  return new Kafka(connectionConfig(config));
}

export function createProducer(config: AppConfig): KafkaJS.Producer {
  return createKafka(config).producer({
    ...connectionConfig(config),
    "enable.idempotence": true,
    acks: -1,
    "delivery.timeout.ms": 120000,
    "linger.ms": 10,
    "compression.codec": "zstd",
    "max.in.flight.requests.per.connection": 5,
    "queue.buffering.max.messages": 100000,
  });
}

export function createConsumer(config: AppConfig, logger: AppLogger): KafkaJS.Consumer {
  let consumer: KafkaJS.Consumer | undefined;

  const rebalanceCallback: RebalanceCallback = async (error, assignment) => {
    const partitions = assignment?.length ?? 0;

    if (error.code === KafkaJS.ErrorCodes.ERR__ASSIGN_PARTITIONS) {
      logger.info("Consumer group assigned partitions", { partitions });
      return;
    }

    if (error.code === KafkaJS.ErrorCodes.ERR__REVOKE_PARTITIONS) {
      logger.info("Consumer group revoked partitions; committing resolved offsets", { partitions });
      try {
        await consumer?.commitOffsets();
      } catch (commitError) {
        logger.warn("Offset commit during revoke failed", errorDetails(commitError));
      }
      return;
    }

    logger.warn("Consumer rebalance callback reported an error", {
      code: error.code,
      error: error.message,
    });
  };

  const consumerConfig: KafkaJS.ConsumerConstructorConfig = {
    ...connectionConfig(config),
    "group.id": config.consumerGroup,
    "enable.auto.commit": false,
    "auto.offset.reset": config.autoOffsetReset,
    "fetch.wait.max.ms": 500,
    "enable.partition.eof": false,
    "max.poll.interval.ms": 300000,
    rebalance_cb: rebalanceCallback,
  };

  if (config.groupProtocol === "consumer") {
    consumerConfig["group.protocol"] = "consumer";
  } else {
    consumerConfig["group.protocol"] = "classic";
    consumerConfig["partition.assignment.strategy"] = "cooperative-sticky";
    consumerConfig["session.timeout.ms"] = 45000;
    consumerConfig["heartbeat.interval.ms"] = 3000;
  }

  consumer = createKafka(config).consumer(consumerConfig);
  return consumer;
}

export function createAdmin(config: AppConfig): KafkaJS.Admin {
  return createKafka(config).admin();
}

export async function connectWithRetry(
  connect: () => Promise<void>,
  logger: AppLogger,
  label: string,
  attempts = 8,
): Promise<void> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      await connect();
      logger.info("Connected to Confluent Cloud", { client: label, attempt });
      return;
    } catch (error) {
      lastError = error;
      if (attempt === attempts) break;

      const backoffMs = Math.min(30000, 500 * 2 ** (attempt - 1));
      const jitterMs = Math.floor(Math.random() * 250);
      logger.warn("Confluent Cloud connection failed; retrying", {
        client: label,
        attempt,
        backoffMs: backoffMs + jitterMs,
        ...errorDetails(error),
      });
      await new Promise((resolve) => setTimeout(resolve, backoffMs + jitterMs));
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`Unable to connect ${label}`);
}
