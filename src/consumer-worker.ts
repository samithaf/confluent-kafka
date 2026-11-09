import {KafkaJS} from '@confluentinc/kafka-javascript';
import {isPlainObject} from 'lodash';
import {loadConfig} from './config';
import {
  createLogger,
  errorDetails,
  errorMessage,
  type AppLogger,
} from './logger';
import {connectWithRetry, createConsumer} from './kafka';
import {installSignalHandlers} from './lifecycle';

interface EventEnvelope {
  eventId?: string;
  type?: string;
  payload?: unknown;
}

async function processRecord(
  message: KafkaJS.KafkaMessage,
  logger: AppLogger,
): Promise<void> {
  const raw = message.value?.toString('utf8');
  if (!raw) throw new Error(`Empty message value at offset ${message.offset}`);

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(
      `Invalid JSON at offset ${message.offset}: ${errorMessage(error)}`,
    );
  }

  if (!isPlainObject(parsed)) {
    throw new Error(`Expected an object at offset ${message.offset}`);
  }

  const event = parsed as EventEnvelope;

  logger.debug('Processed event', {
    offset: message.offset,
    eventId: event.eventId,
    type: event.type,
  });
}

async function main(): Promise<void> {
  const config = loadConfig('consumer');
  const logger = createLogger({
    component: 'consumer-worker',
    region: config.region,
  });
  const consumer = createConsumer(config, logger);

  await connectWithRetry(() => consumer.connect(), logger, 'consumer');
  await consumer.subscribe({topics: config.topics});
  logger.info('Subscribed to topics', {
    topics: config.topics,
    group: config.consumerGroup,
    groupProtocol: config.groupProtocol,
  });

  const runState: {promise?: Promise<void>} = {};
  let stopping = false;

  installSignalHandlers(
    async () => {
      stopping = true;
      logger.info('Stopping consumer');
      await consumer.stop();
      if (runState.promise) await runState.promise.catch(() => undefined);

      try {
        await consumer.commitOffsets();
        logger.info('Committed final offsets');
      } catch (error) {
        logger.warn('Final offset commit failed', errorDetails(error));
      }

      await consumer.disconnect();
      logger.info('Consumer disconnected');
    },
    logger,
    config.shutdownTimeoutMs,
  );

  runState.promise = consumer.run({
    eachBatchAutoResolve: false,
    partitionsConsumedConcurrently: config.partitionsConsumedConcurrently,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      isRunning,
      isStale,
    }) => {
      let processed = 0;

      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          logger.warn(
            'Batch became stale; stopping before unresolved offsets',
            {
              topic: batch.topic,
              partition: batch.partition,
              processed,
              total: batch.messages.length,
            },
          );
          break;
        }

        await processRecord(message, logger);
        resolveOffset(message.offset);
        processed += 1;

        if (processed % 10 === 0) await heartbeat();
        if (processed % config.commitIntervalMessages === 0) {
          await commitOffsetsIfNecessary();
        }
      }

      if (!isStale()) {
        await heartbeat();
        await commitOffsetsIfNecessary();
      }

      logger.info('Batch processed', {
        topic: batch.topic,
        partition: batch.partition,
        processed,
        total: batch.messages.length,
        lag: batch.offsetLag(),
      });
    },
  });

  await runState.promise;
  if (!stopping) {
    logger.error('Consumer run loop exited unexpectedly');
    process.exit(1);
  }
}

main().catch((error: unknown) => {
  const logger = createLogger({component: 'consumer-worker'});
  logger.error('Consumer worker failed', errorDetails(error));
  process.exit(1);
});
