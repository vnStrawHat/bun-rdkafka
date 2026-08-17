import { describe, expect, test } from "bun:test";
import { Fifo } from "../../packages/bun-rdkafka/src/core/fifo.ts";

describe("Fifo", () => {
  test("push/shift preserves order and reports length", () => {
    const q = new Fifo<number>();
    expect(q.shift()).toBeUndefined();
    q.push(1);
    q.push(2);
    q.push(3);
    expect(q.length).toBe(3);
    expect(q.shift()).toBe(1);
    expect(q.length).toBe(2);
    expect(q.shift()).toBe(2);
    expect(q.shift()).toBe(3);
    expect(q.shift()).toBeUndefined();
    expect(q.length).toBe(0);
  });

  test("take(n) dequeues at most n items in order", () => {
    const q = new Fifo<number>();
    for (let i = 0; i < 5; i++) q.push(i);
    expect(q.take(2)).toEqual([0, 1]);
    expect(q.take(0)).toEqual([]);
    expect(q.take(10)).toEqual([2, 3, 4]);
    expect(q.length).toBe(0);
    expect(q.take(1)).toEqual([]);
  });

  test("clear empties the queue", () => {
    const q = new Fifo<string>();
    q.push("a");
    q.clear();
    expect(q.length).toBe(0);
    expect(q.shift()).toBeUndefined();
    q.push("b");
    expect(q.shift()).toBe("b");
  });

  test("stays correct across compaction (many interleaved push/shift)", () => {
    const q = new Fifo<number>();
    let next = 0;
    let expected = 0;
    for (let round = 0; round < 50; round++) {
      for (let i = 0; i < 300; i++) q.push(next++);
      for (let i = 0; i < 200; i++) expect(q.shift()).toBe(expected++);
      expect(q.length).toBe(next - expected);
    }
    while (q.length > 0) expect(q.shift()).toBe(expected++);
    expect(expected).toBe(next);
  });
});
