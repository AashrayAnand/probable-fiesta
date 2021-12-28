### Probable Fiesta

A log-structued merge tree based key value store.

The goals of this project are providing a data store for

1. simple key-value dictionary semantics
2. atomic and durable operations
3. serializability
4. high throughput

The approach to solve these problems includes

1. Write-ahead logging, to persist operations as they occur, prior to mutating state.
2. Structured segments without duplicates, with merging of segments from most-to-least recently used to ensure sequential writes
3. primary-secondary architecture, with log shipping for replication, writes served by the primary only, and asynchronous

This key-value store is intended to serve as a system cache, as the schema is not as structured as a relational database or even a hierarchial key-value store, but instead serves high throughput reads and writes of simple key-value pairs