# Consumer poll return empty records after seek to special offset

## kafka
 
in [docker-compose.yaml](docker-compose.yaml), set `log.segment.bytes` to 50kb

```shell
docker-compose up -d
```

## run test class

[TestKafka.java](src/test/java/TestKafka.java)

Each run will recreate the topic named `TEST`, and prepare data like this 
```text
  ┌────────┬───────┬─────────┬───────┐
  │0 1 2   │4   6 7│8 9 10   │12     │
  └──────▲─┴──▲────┴───────▲─┴───▲───┘
  box = segment, num = offset, ▲ = commitTransaction
```

The observed behavior is as follows:
- seek(3) + poll() return empty records
- seek(5) + poll()  return records
- seek(11) + poll() return empty records
