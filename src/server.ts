/*
 * server.ts
 *
 * Copyright (c) 2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/nanorpc-redis
 */

import { isPromise } from "node:util/types";
import { EventEmitter } from "node:events";
import { Mutex } from "async-mutex";
import { NanoRPC, NanoRPCError } from "nanorpc-validator";
import { createNanoRPCError, createNanoReply } from "nanorpc-validator";
import { NanoRPCBase, NanoRPCErrCode, NanoRPCStatus } from "./base.js";
import { RedisType, withRedis } from "./db.js";

export class NanoRPCServer extends NanoRPCBase {
  private running: boolean;
  private readonly name: string;
  private readonly events: EventEmitter;
  private readonly methods: { [method: string]: boolean };

  constructor(name: string, redisOrUrl: RedisType | string) {
    super(redisOrUrl);
    this.running = false;
    this.name = name;
    this.events = new EventEmitter();
    this.methods = {};
  }

  on<T, P extends Array<unknown>>(
    method: string,
    func: (...args: P) => T | Promise<T>,
  ) {
    if (method in this.methods) {
      throw new NanoRPCError(
        NanoRPCErrCode.DuplicateMethod,
        `${method} method already registered`,
      );
    }

    this.events.on(method, async (rpc: NanoRPC<object>, mutex?: Mutex) => {
      try {
        const params = (
          Array.isArray(rpc.params)
            ? rpc.params
            : rpc.params
              ? [rpc.params]
              : []
        ) as P;
        const result = func(...params);
        const retval = isPromise(result) ? await result : result;
        const reply = createNanoReply(rpc.id, NanoRPCStatus.OK, retval);

        await this.redis.rPush(
          `NanoRPCs:${this.name}:${method}:${rpc.id}`,
          JSON.stringify(reply),
        );
      } catch (error) {
        const reply =
          error instanceof NanoRPCError
            ? createNanoRPCError(
                rpc.id,
                NanoRPCStatus.Exception,
                error.code,
                error.message,
              )
            : createNanoRPCError(
                rpc.id,
                NanoRPCStatus.Exception,
                NanoRPCErrCode.CallError,
                typeof error === "string"
                  ? error
                  : error instanceof Error
                    ? error.message
                    : `${error}`,
              );

        await this.redis.rPush(
          `NanoRPCs:${this.name}:${method}:${rpc.id}`,
          JSON.stringify(reply),
        );
      }

      if (mutex) {
        mutex.release();
      }
    });

    this.methods[method] = true;
    return this;
  }

  run(queued = false) {
    if (!this.running) {
      this.running = true;

      (async () => {
        const parseNanoRPC = (text: string) => {
          try {
            return JSON.parse(text) as NanoRPC<object>;
          } catch (error) {
            return undefined;
          }
        };
        const mutex = queued ? new Mutex() : undefined;

        await this.connect();

        await withRedis(this.redis, async (redis) => {
          while (this.running) {
            const payload = await redis.blPop(`NanoRPCs:${this.name}`, 0.25);

            if (!payload) {
              continue;
            }

            const rpc = parseNanoRPC(payload.element);

            if (!rpc) {
              continue;
            }

            if (!("method" in rpc) || typeof rpc.method !== "string") {
              continue;
            }

            const validator = this.validators.getValidator(rpc.method);

            if (validator && !validator(rpc)) {
              continue;
            }

            if (!(rpc.method in this.methods)) {
              const reply = createNanoRPCError(
                rpc.id,
                NanoRPCStatus.MissingMethod,
                NanoRPCErrCode.MethodNotFound,
                "Missing Method",
              );

              await redis.rPush(
                `NanoRPCs:${this.name}:${rpc.method}:${rpc.id}`,
                JSON.stringify(reply),
              );

              continue;
            }

            if (mutex) {
              await mutex.acquire();
            }

            this.events.emit(rpc.method, rpc, mutex);

            if (mutex) {
              await mutex.waitForUnlock();
            }
          }
        });
      })();
    }

    return async () => {
      this.running = false;
      await this.close();
    };
  }
}

export const createNanoRPCServer = (
  name: string,
  redisOrUrl: RedisType | string,
) => new NanoRPCServer(name, redisOrUrl);
