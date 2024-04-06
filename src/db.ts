/*
 * db.ts
 *
 * Copyright (c) 2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/nanorpc-redis
 */

import { createClient } from "redis";

export const createRedis = (url: string) =>
  createClient({ url, pingInterval: 60 * 1000 });

export type RedisType = ReturnType<typeof createRedis>;

export const withRedis = async <T>(
  redis: RedisType,
  callback: (connection: typeof redis) => Promise<T>,
) => {
  const connection = redis.duplicate();
  await connection.connect();

  try {
    return await callback(connection);
  } finally {
    await connection.disconnect();
  }
};
