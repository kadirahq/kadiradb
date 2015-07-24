# KadiraDB - metrics

A real time metrics database which uses KadiraDB under the hood.

## Getting Started

``` 
docker run -d \
  -p 19000:19000 \
  -v /tmp/kmdb:/data \
  --cap-add=IPC_LOCK \
  kadirahq/kadiradb-metrics:latest
```

