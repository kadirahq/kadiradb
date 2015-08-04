# KadiraDB

KadiraDB is a time-series database designed for storing real-time metrics.



## Getting Started

The recommended method to run the database server in production is using our Docker container.  The docker image is based on the lightweight [alpine linux](http://www.alpinelinux.org/) distribution therefore the image size is smaller than 15 MB.

### Starting a server

Use this command to start the server. It will start the server and store all data in `/tmp/data` directory which is mounted as a docker volume.

``` shell
❯ docker run -d \
  --name kadiradb \
  -p 19000:19000 \
  -v /tmp/data:/data \
  --cap-add=IPC_LOCK \
  kadirahq/kadiradb:latest
❯ docker logs -f kadiradb
```

New Databases can be created using the database shell, one of our client libraries, or providing initial db configurations as a parameter when starting the server.



## Database Clients

- [Golang](https://github.com/kadirahq/kadiradb-go)
- [NodeJS](https://github.com/kadirahq/kadiradb-node)
