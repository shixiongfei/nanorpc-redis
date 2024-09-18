/*
 * client.ts
 *
 * Copyright (c) 2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/nanorpc-redis
 */

import { NanoRPCError, NanoReply, createNanoRPC } from "nanorpc-validator";
import { NanoRPCBase, NanoRPCErrCode, NanoRPCStatus } from "./base.js";
import { RedisType, withRedis } from "./db.js";

export class NanoRPCClient extends NanoRPCBase {
  constructor(redisOrUrl: RedisType | string) {
    super(redisOrUrl);
  }

  async apply<T, P extends object>(name: string, method: string, params?: P) {
    const payload = await withRedis(this.redis, async (redis) => {
      const rpc = createNanoRPC(method, params);

      const result = await Promise.all([
        redis.rPush(`NanoRPCs:${name}`, JSON.stringify(rpc)),
        redis.blPop(`NanoRPCs:${name}:${method}:${rpc.id}`, 0),
      ]);

      return result[1];
    });

    if (!payload) {
      throw new NanoRPCError(
        NanoRPCErrCode.CallError,
        `Call ${name}:${method} received null message`,
      );
    }

    const reply = JSON.parse(payload.element) as NanoReply<T>;

    const validator = this.validators.getValidator(method);

    if (validator && !validator(reply)) {
      const lines = validator.errors!.map(
        (err) => `${err.keyword}: ${err.instancePath}, ${err.message}`,
      );
      const error = lines.join("\n");

      throw new NanoRPCError(
        NanoRPCErrCode.CallError,
        `Call ${name}:${method} ${error}`,
      );
    }

    if (reply.status !== NanoRPCStatus.OK) {
      throw new NanoRPCError(
        reply.error?.code ?? NanoRPCErrCode.CallError,
        `Call ${name}:${method} ${reply.error?.message ?? "unknown error"}`,
      );
    }

    return reply.result;
  }

  async call<T, P extends Array<unknown>>(
    name: string,
    method: string,
    ...args: P
  ) {
    return await this.apply<T, P>(name, method, args);
  }

  invoke<T, P extends Array<unknown>>(name: string, method: string) {
    return async (...args: P) => await this.apply<T, P>(name, method, args);
  }
}

export const createNanoRPCClient = (redisOrUrl: RedisType | string) =>
  new NanoRPCClient(redisOrUrl);
