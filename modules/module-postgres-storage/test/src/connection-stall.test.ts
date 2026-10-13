import { setTimeout as sleep } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, test } from 'vitest';

import { normalizePostgresStorageConfig, PostgresBucketStorageFactory } from '../../src/index.js';
import { env } from './env.js';

type ProxyOptions = {
  /** Hold upstream setup replies this long, like a remote database round trip. */
  latencyMs: number;
  /** Close a connection whose client has not answered the server's ReadyForQuery by then. */
  deadlineMs: number;
  /** Close this many connections as soon as they are accepted. */
  dropFirst: number;
};

type Proxy = AsyncDisposable & { uri: string };

// Runs in its own thread so its deadline fires while the test's event loop is blocked,
// the way a managed Postgres edge (PlanetScale closes after about 5 s) drops a stalled setup.
const PROXY_SOURCE = `
const { parentPort, workerData } = require('node:worker_threads');
const net = require('node:net');
const { upstreamHost, upstreamPort, latencyMs, deadlineMs, dropFirst } = workerData;
let accepted = 0;
const server = net.createServer((client) => {
  accepted += 1;
  if (accepted <= dropFirst) {
    client.destroy();
    return;
  }
  const upstream = net.connect(upstreamPort, upstreamHost);
  let setup = Buffer.alloc(0);
  let replied = false;
  let answered = false;
  const close = () => {
    clearTimeout(deadline);
    client.destroy();
    upstream.destroy();
  };
  const deadline = setTimeout(() => {
    if (!answered) close();
  }, deadlineMs);
  client.on('data', (chunk) => {
    if (replied) {
      answered = true;
      clearTimeout(deadline);
    }
    upstream.write(chunk);
  });
  upstream.on('data', (chunk) => {
    if (replied) {
      client.write(chunk);
      return;
    }
    setup = Buffer.concat([setup, chunk]);
    let offset = 0;
    let ready = false;
    while (offset + 5 <= setup.length) {
      if (setup[offset] === 0x5a) ready = true;
      offset += 1 + setup.readUInt32BE(offset + 1);
    }
    setTimeout(() => {
      if (client.destroyed) return;
      client.write(chunk);
      if (ready) replied = true;
    }, latencyMs);
  });
  client.on('error', close).on('close', close);
  upstream.on('error', close).on('close', close);
});
server.listen(0, '127.0.0.1', () => parentPort.postMessage(server.address().port));
`;

async function startProxy(options: ProxyOptions): Promise<Proxy> {
  const upstream = new URL(env.PG_STORAGE_TEST_URL);
  const worker = new Worker(PROXY_SOURCE, {
    eval: true,
    workerData: { ...options, upstreamHost: upstream.hostname, upstreamPort: Number(upstream.port || 5432) }
  });
  const port = await new Promise<number>((resolve, reject) => {
    worker.once('message', resolve);
    worker.once('error', reject);
  });
  const uri = new URL(env.PG_STORAGE_TEST_URL);
  uri.hostname = '127.0.0.1';
  uri.port = String(port);
  return {
    uri: uri.toString(),
    [Symbol.asyncDispose]: async () => {
      await worker.terminate();
    }
  };
}

function storageFactory(uri: string) {
  return new PostgresBucketStorageFactory({
    config: normalizePostgresStorageConfig({ type: 'postgresql', uri, sslmode: 'disable' }),
    replicationStreamNamePrefix: 'test_'
  });
}

function blockEventLoop(milliseconds: number) {
  const until = Date.now() + milliseconds;
  while (Date.now() < until) {}
}

async function selectOne(factory: PostgresBucketStorageFactory) {
  const result = await factory.db.query({ statement: 'SELECT 1 AS one' });
  return result.rows.map((row) => row.decodeWithoutCustomTypes(0));
}

describe('storage connection setup', () => {
  test('survives a blocked event loop while the storage connection is being set up', async () => {
    // PowerSync compiles the sync config synchronously right after creating its storage factory.
    // Real time on purpose: the stall has to outlast a deadline running in another thread.
    await using proxy = await startProxy({ latencyMs: 100, deadlineMs: 300, dropFirst: 0 });
    await using factory = storageFactory(proxy.uri);
    await sleep(20);
    blockEventLoop(800);

    expect(await selectOne(factory)).toEqual([1n]);
  });

  test('recovers when a stall drops a storage query that was already connecting', async () => {
    // Real time on purpose: the stall has to outlast a deadline running in another thread.
    await using proxy = await startProxy({ latencyMs: 100, deadlineMs: 300, dropFirst: 0 });
    await using factory = storageFactory(proxy.uri);
    const pending = selectOne(factory);
    await sleep(20);
    blockEventLoop(800);

    expect(await pending).toEqual([1n]);
  });

  test('a failed connection check does not fail every later query', async () => {
    await using proxy = await startProxy({ latencyMs: 0, deadlineMs: 5_000, dropFirst: 2 });
    await using factory = storageFactory(proxy.uri);

    await expect(selectOne(factory)).rejects.toThrow();
    expect(await selectOne(factory)).toEqual([1n]);
  });
});
