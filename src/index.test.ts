/*
 * index.test.ts
 *
 * Copyright (c) 2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/nanorpc-redis
 */

import { createNanoRPCServer, createNanoRPCClient } from "./index.js";

type AddRPCFunc = (a: number, b: number) => Promise<number | undefined>;

const redisUrl = "redis://:123456@127.0.0.1:6379/0";
let shutdown: (() => Promise<void>) | undefined = undefined;

const client = async () => {
  const client = createNanoRPCClient(redisUrl);
  await client.connect();

  const addRPC: AddRPCFunc = client.invoke("rpc-test", "add");

  console.log(
    await Promise.all([
      addRPC(23, 31),
      client.call("rpc-test", "add", 123, 456),
    ]),
  );

  await client.close();

  if (shutdown) {
    await shutdown();
  }
};

const server = async () => {
  const server = createNanoRPCServer("rpc-test", redisUrl);
  await server.connect();

  server.on("add", (a: number, b: number) => a + b);

  shutdown = server.run();
};

const test = async () => await Promise.all([server(), client()]);

test();
