/**
 * kafkajs/kafka.ts — `KafkaJS.Kafka`: holds the common config and acts as the
 * factory for Producer / Consumer / Admin (FR-2).
 *
 * Semantics cross-checked against confluent-kafka-javascript's
 * `lib/kafkajs/_kafka.js`:
 *  - The common config takes `{ kafkaJS: {...} }` + pass-through librdkafka
 *    properties.
 *  - A kafkaJS key misplaced OUTSIDE the block (e.g.
 *    `new Kafka({ brokers: [...] })`) → throws with a message pointing to the
 *    block.
 *  - `producer()/consumer()/admin()` merge configs: the `kafkaJS` block
 *    shallow-merges one level (specific over common), the rest shallow-merges.
 */

import {
  checkIfKafkaJsKeysPresent,
  CompatibilityErrorMessages,
  mergeRawConfigs,
  type CommonRawConfig,
} from "./config-mapper.ts";
import { KafkaJSError } from "./errors.ts";
import { Producer } from "./producer.ts";
import { Consumer } from "./consumer.ts";
import { Admin } from "./admin.ts";
import type {
  AdminConstructorConfig,
  CommonConstructorConfig,
  ConsumerConstructorConfig,
  ProducerConstructorConfig,
} from "./config-types.ts";

export class Kafka {
  #commonClientConfig: CommonRawConfig;

  constructor(config?: CommonConstructorConfig) {
    this.#commonClientConfig = config ?? {};
    const disallowedKey = checkIfKafkaJsKeysPresent("common", this.#commonClientConfig);
    if (disallowedKey !== null) {
      throw new KafkaJSError(CompatibilityErrorMessages.kafkaJSCommonKey(disallowedKey));
    }
  }

  #merged(config?: CommonRawConfig): CommonRawConfig {
    return mergeRawConfigs(this.#commonClientConfig, config);
  }

  producer(config?: ProducerConstructorConfig): Producer {
    const disallowedKey = checkIfKafkaJsKeysPresent("producer", config ?? {});
    if (disallowedKey !== null) {
      throw new KafkaJSError(
        CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, "producer"),
      );
    }
    return new Producer(this.#merged(config));
  }

  consumer(config?: ConsumerConstructorConfig): Consumer {
    const disallowedKey = checkIfKafkaJsKeysPresent("consumer", config ?? {});
    if (disallowedKey !== null) {
      throw new KafkaJSError(
        CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, "consumer"),
      );
    }
    return new Consumer(this.#merged(config));
  }

  admin(config?: AdminConstructorConfig): Admin {
    const disallowedKey = checkIfKafkaJsKeysPresent("admin", config ?? {});
    if (disallowedKey !== null) {
      throw new KafkaJSError(CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, "admin"));
    }
    return new Admin(this.#merged(config));
  }
}
