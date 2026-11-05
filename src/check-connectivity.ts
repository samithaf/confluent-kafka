import { loadConfig } from "./config";
import { createLogger, errorDetails } from "./logger";
import { connectWithRetry, createAdmin } from "./kafka";

async function main(): Promise<void> {
  const config = loadConfig("admin");
  const logger = createLogger({ component: "check-connectivity" });
  const admin = createAdmin(config);

  await connectWithRetry(() => admin.connect(), logger, "admin");

  try {
    const clusterId = await admin.clusterId({ timeout: 5000 }).catch(() => "unavailable");
    const metadata = await admin.fetchTopicMetadata(
      config.topics.length > 0 ? { topics: config.topics, timeout: 15000 } : { timeout: 15000 },
    );

    logger.info("Connectivity check succeeded", {
      clusterId,
      bootstrapServers: config.bootstrapServers,
      region: config.region,
      topics: metadata.map((topic) => ({
        name: topic.name,
        partitions: topic.partitions.length,
      })),
    });
  } finally {
    await admin.disconnect();
  }
}

main().catch((error: unknown) => {
  const logger = createLogger({ component: "check-connectivity" });
  logger.error("Connectivity check failed", errorDetails(error));
  process.exit(1);
});
