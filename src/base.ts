/*
 * base.ts
 *
 * Copyright (c) 2024 Xiongfei Shi
 *
 * Author: Xiongfei Shi <xiongfei.shi(a)icloud.com>
 * License: Apache-2.0
 *
 * https://github.com/shixiongfei/nanorpc-redis
 */

import { NanoValidator, createNanoValidator } from "nanorpc-validator";
import { RedisType, createRedis } from "./db.js";

export enum NanoRPCStatus {
  OK = 0,
  Exception,
  MissingMethod,
}

export enum NanoRPCErrCode {
  DuplicateMethod = -1,
  MethodNotFound = -2,
  CallError = -3,
}

export class NanoRPCBase {
  public readonly validators: NanoValidator;
  protected readonly redis: RedisType;
  protected readonly maintenance: boolean;

  constructor(redisOrUrl: RedisType | string) {
    this.validators = createNanoValidator();

    if (typeof redisOrUrl === "string") {
      this.redis = createRedis(redisOrUrl);
      this.maintenance = true;
    } else {
      this.redis = redisOrUrl;
      this.maintenance = false;
    }
  }

  async connect() {
    if (this.maintenance && !this.redis.isReady) {
      await this.redis.connect();
    }
  }

  async close() {
    if (this.maintenance && this.redis.isReady) {
      await this.redis.quit();
    }
  }
}
