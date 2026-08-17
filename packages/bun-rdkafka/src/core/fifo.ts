/**
 * core/fifo.ts — a minimal FIFO backed by an array with a moving head index.
 *
 * `Array.prototype.shift()`/`splice(0, n)` on a hot queue costs a memmove per
 * call; on the consume path that showed up as the second-largest JS cost after
 * decoding. This queue dequeues in O(1) and compacts the array lazily once the
 * dead prefix is both large and dominant.
 */
export class Fifo<T> {
  private items: (T | undefined)[] = [];
  private head = 0;

  /** Number of queued items. */
  get length(): number {
    return this.items.length - this.head;
  }

  push(item: T): void {
    this.items.push(item);
  }

  /** Dequeues the oldest item, or `undefined` when empty. */
  shift(): T | undefined {
    const items = this.items;
    if (this.head >= items.length) return undefined;
    const item = items[this.head];
    items[this.head++] = undefined;
    this.compact();
    return item;
  }

  /** Dequeues up to `n` items (fewer when the queue is shorter). */
  take(n: number): T[] {
    const items = this.items;
    const end = Math.min(items.length, this.head + Math.max(0, n));
    const out = items.slice(this.head, end) as T[];
    for (let i = this.head; i < end; i++) items[i] = undefined;
    this.head = end;
    this.compact();
    return out;
  }

  clear(): void {
    this.items = [];
    this.head = 0;
  }

  private compact(): void {
    const items = this.items;
    if (this.head === items.length) {
      items.length = 0;
      this.head = 0;
    } else if (this.head >= 1024 && this.head * 2 >= items.length) {
      this.items = items.slice(this.head);
      this.head = 0;
    }
  }
}
