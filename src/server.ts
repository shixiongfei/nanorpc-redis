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
import { EventEmitter } from "eventemitter3";
import { Mutex } from "async-mutex";
import { NanoRPC, createNanoReply } from "nanorpc-validator";
import { NanoRPCBase, NanoRPCCode } from "./base.js";
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
      throw new Error(`${method} method already registered`);
    }

    this.events.on(method, async (rpc: NanoRPC<P>, mutex?: Mutex) => {
      try {
        const result = func(...rpc.arguments);
        const retval = isPromise(result) ? await result : result;
        const reply = createNanoReply(rpc.id, NanoRPCCode.OK, "OK", retval);

        await this.redis.rPush(
          `NanoRPCs:${this.name}:${method}:${rpc.id}`,
          JSON.stringify(reply),
        );
      } catch (error) {
        const reply = createNanoReply(
          rpc.id,
          NanoRPCCode.Exception,
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
            return JSON.parse(text) as NanoRPC<unknown[]>;
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
              const reply = createNanoReply(
                rpc.id,
                NanoRPCCode.MissingMethod,
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
