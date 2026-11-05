import http from "node:http";
import { randomUUID } from "node:crypto";
import { loadConfig } from "./config";
import { createLogger, errorDetails, errorMessage } from "./logger";
import { connectWithRetry, createProducer } from "./kafka";
import { installSignalHandlers } from "./lifecycle";

const MAX_BODY_BYTES = 1024 * 1024;

interface PublishRequest {
  key?: string;
  value: unknown;
  headers?: Record<string, string>;
}

class HttpError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
  ) {
    super(message);
  }
}

function sendJson(
  response: http.ServerResponse,
  statusCode: number,
  body: unknown,
  headers: Record<string, string> = {},
): void {
  const payload = JSON.stringify(body);
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload),
    ...headers,
  });
  response.end(payload);
}

async function readJson(request: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;

  for await (const chunk of request as AsyncIterable<Buffer | string>) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_BODY_BYTES) {
      throw new HttpError(413, "payload_too_large");
    }
    chunks.push(buffer);
  }

  if (size === 0) throw new HttpError(400, "empty_body");

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpError(400, "invalid_json");
  }
}

function validatePublish(body: unknown): PublishRequest {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new HttpError(400, "body_must_be_an_object");
  }

  const record = body as Record<string, unknown>;
  if (!("value" in record)) throw new HttpError(400, "value_is_required");
  if (record.key !== undefined && typeof record.key !== "string") {
    throw new HttpError(400, "key_must_be_a_string");
  }
  if (record.headers !== undefined) {
    if (typeof record.headers !== "object" || record.headers === null || Array.isArray(record.headers)) {
      throw new HttpError(400, "headers_must_be_an_object");
    }
    for (const [name, value] of Object.entries(record.headers as Record<string, unknown>)) {
      if (typeof value !== "string") {
        throw new HttpError(400, `header_${name}_must_be_a_string`);
      }
    }
  }

  return {
    key: record.key as string | undefined,
    value: record.value,
    headers: record.headers as Record<string, string> | undefined,
  };
}

async function main(): Promise<void> {
  const config = loadConfig("producer");
  const logger = createLogger({ component: "producer-service", region: config.region });
  const producer = createProducer(config);

  await connectWithRetry(() => producer.connect(), logger, "producer");
  let ready = true;

  const server = http.createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

      if (request.method === "GET" && (url.pathname === "/healthz" || url.pathname === "/readyz")) {
        sendJson(response, ready ? 200 : 503, {
          status: ready ? "ok" : "not_ready",
          service: config.serviceName,
          revision: config.serviceRevision,
        });
        return;
      }

      if (request.method === "GET" && url.pathname === "/") {
        sendJson(response, 200, {
          service: config.serviceName,
          revision: config.serviceRevision,
          endpoints: ["GET /healthz", "GET /readyz", "POST /publish"],
        });
        return;
      }

      if (request.method === "POST" && url.pathname === "/publish") {
        const body = validatePublish(await readJson(request));
        const eventId = randomUUID();
        const key = body.key ?? eventId;
        const value = typeof body.value === "string" ? body.value : JSON.stringify(body.value);

        try {
          const [metadata] = await producer.send({
            topic: config.topics[0],
            messages: [
              {
                key,
                value,
                headers: {
                  "content-type": "application/json",
                  "event-id": eventId,
                  source: config.serviceName,
                  ...body.headers,
                },
              },
            ],
          });

          logger.info("Published event", {
            eventId,
            key,
            topic: metadata.topicName,
            partition: metadata.partition,
            offset: metadata.offset,
          });

          sendJson(response, 202, {
            eventId,
            topic: metadata.topicName,
            partition: metadata.partition,
            offset: metadata.offset,
          });
        } catch (error) {
          logger.error("Publish failed", { eventId, key, ...errorDetails(error) });
          sendJson(
            response,
            503,
            { error: "publish_failed", eventId, message: errorMessage(error) },
            { "retry-after": "1" },
          );
        }
        return;
      }

      sendJson(response, 404, { error: "not_found" });
    })().catch((error: unknown) => {
      if (response.headersSent) {
        response.destroy();
        return;
      }
      if (error instanceof HttpError) {
        sendJson(response, error.statusCode, { error: error.message });
        return;
      }
      logger.error("Unhandled request error", errorDetails(error));
      sendJson(response, 500, { error: "internal_error" });
    });
  });

  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;

  installSignalHandlers(async () => {
    ready = false;
    logger.info("Stopping HTTP server");
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeIdleConnections();
    });
    logger.info("Flushing producer");
    await producer.flush({ timeout: Math.max(1000, config.shutdownTimeoutMs - 1500) });
    await producer.disconnect();
    logger.info("Producer disconnected");
  }, logger, config.shutdownTimeoutMs);

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port, () => {
      server.off("error", reject);
      resolve();
    });
  });

  logger.info("Producer service listening", { port: config.port, topic: config.topics[0] });
}

main().catch((error: unknown) => {
  const logger = createLogger({ component: "producer-service" });
  logger.error("Producer service failed to start", errorDetails(error));
  process.exit(1);
});
