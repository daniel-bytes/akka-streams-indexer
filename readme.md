# Akka Streams Elasticsearch Indexer Example

Example of indexing a constant stream of data to Elasticsearch, using Akka Streams.

### Akka Stream Flow

The following Akka Streams flow is configured in `MessageIndexer.scala`:
```
   |-----------------|
   | 1. Actor Source |
   |-----------------|
             |
             ▼
 |----------------------|
 | 2. Persistent Buffer |  <- squbs PersistentBuffer
 |----------------------|
             |
             ▼
|------------------------|
| 3. Backpressure Toggle | <- custom GraphStage
|------------------------|
             |
             ▼
      |-------------|
      | 4. Throttle |
      |-------------|
             |
             ▼
  |----------------------|
  | 5. Transform Message |
  |----------------------|
             |
             ▼
  |----------------------|
  | 6. Bulk Index to ES  | <- Alpakka ElasticsearchSink
  |----------------------|
```

- `1. Actor Source `
  - An Akka Streams `Source` exposed as an `ActorRef`.  This allows us to send messages to the stream using the standard Akka `!` operator.
- `2. Persistent Buffer`
  - A [`PersistentBuffer`](https://github.com/paypal/squbs/blob/master/docs/persistent-buffer.md) stage, which allows us to store incoming messages in a memory-mapped file.  Any downstream backpressure applied will cause this buffer to fill, and once the stream is resumed all existing messages will be delivered from the buffer.  This allows for a limited form of durability (messages are stored on disk and can survive a process restart, but of course are still open to a machine or disk failure).
- `3. Backpressure Toggle`
  - A custom `GraphStage` implementation that allows the stream to be toggled between an open and closed state.  When closed, backpressure will be applied.  This allows us to buffer incoming messages durably in the preceding `PersistentBuffer` stage while we reindex.
- `4. Throttle`
  - Standard Akka Streams `throttle` stage, used to limit the amount of data send downstream to Elasticsearch over a given time period.
- `5. Transform Message`
  - Convert the `Message` class into an Elasticsearch friendly model
- `6. Bulk Index to ES`
  - Batch indexes messages to Elasticsearch, with retry
  
## Backpressure Toggle

The Backpressure Toggle (via the `Toggle` class) is a custom Flow component used to signal back pressure.  As long as the associated `ToggleState.state` is set to `true`, the stream will flow as normal.  If it is set to `false`, no new elements are pulled.  When used in conjunction with the `PersistentBuffer`, we can enter a state where we are buffering all incoming values for a controlled period of time (in our case, while we are reindexing our data into a new Elasticsearch index).

## Main Demo Application

The demo application in `Main.scala` shows how Akka streams can be used for indexing a stream of non-timeseries data into Elasticsearch, with the ability to pause and reindex without losing data. 

A `Future` is created with an infinite loop yielding data from our in-memory store of `Message` structures.  You can imagine this being a crude approximation of something like a Kafka, RabbitMQ or Zookeeper listener, or maybe a relational database transaction log watcher.  Any time the same message is published, it's `version` field is incremented.

The demo application shows how we can create a new Elasticsearch index, set read and write aliases to the index, and write some data from the stream.  Then, we show how we can pause indexing, reindex into a new index and swap write and read indexes at separate times (so we can catch up with our writes while temporarily serving reads from our old index).