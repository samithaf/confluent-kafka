import {isInteger} from 'lodash';
import {loadConfig} from './config';
import {createLogger, errorDetails} from './logger';
import {connectWithRetry, createAdmin} from './kafka';

const PARTITIONS = Number.parseInt(
  process.env.KAFKA_TOPIC_PARTITIONS?.trim() || '12',
  10,
);
const REPLICATION_FACTOR = Number.parseInt(
  process.env.KAFKA_REPLICATION_FACTOR?.trim() || '3',
  10,
);
const RETENTION_MS =
  process.env.KAFKA_RETENTION_MS?.trim() || String(7 * 24 * 60 * 60 * 1000);

async function main(): Promise<void> {
  const config = loadConfig('admin');
  const logger = createLogger({component: 'create-topics'});

  if (config.topics.length === 0) {
    throw new Error(
      'Set KAFKA_TOPIC or KAFKA_TOPICS to the topics you want to create',
    );
  }
  if (!isInteger(PARTITIONS) || PARTITIONS <= 0) {
    throw new Error('KAFKA_TOPIC_PARTITIONS must be a positive integer');
  }
  if (!isInteger(REPLICATION_FACTOR) || REPLICATION_FACTOR <= 0) {
    throw new Error('KAFKA_REPLICATION_FACTOR must be a positive integer');
  }

  const admin = createAdmin(config);
  await connectWithRetry(() => admin.connect(), logger, 'admin');

  try {
    const existing = new Set(await admin.listTopics());

    for (const topic of config.topics) {
      if (existing.has(topic)) {
        logger.info('Topic already exists', {topic});
        continue;
      }

      const created = await admin.createTopics({
        timeout: 30000,
        topics: [
          {
            topic,
            numPartitions: PARTITIONS,
            replicationFactor: REPLICATION_FACTOR,
            configEntries: [
              {
                name: 'min.insync.replicas',
                value: String(Math.min(2, REPLICATION_FACTOR)),
              },
              {name: 'cleanup.policy', value: 'delete'},
              {name: 'retention.ms', value: RETENTION_MS},
            ],
          },
        ],
      });

      logger.info(
        created ? 'Topic created' : 'Topic creation was not applied',
        {
          topic,
          partitions: PARTITIONS,
          replicationFactor: REPLICATION_FACTOR,
        },
      );
    }
  } finally {
    await admin.disconnect();
  }
}

main().catch((error: unknown) => {
  const logger = createLogger({component: 'create-topics'});
  logger.error('Topic provisioning failed', errorDetails(error));
  process.exit(1);
});
