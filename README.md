# KadiraDB Metrics

KadiraDB Metrics is a time-series database designed for storing real-time metrics. 



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
  kadirahq/kadiradb-metrics:latest
❯ docker logs -f kadiradb
```

New Databases can be created using the database shell, one of our client libraries, or providing initial db configurations as a parameter when starting the server. Install the shell from nam and connect to the server.

``` shell
❯ npm i -g kmdb-node
❯ kmdb "kmdb://localhost:19000"
```

The second argument to kdb is optional. It defaults to "kmdb://localhost:19000". If you're working on a Mac or a Windows computer, replace "localhost" with boot2docker vm ip address or docker machine ip address.

### Crating a DB

Let's assume that we need to create **a database to store the temperature of major cities in Sri Lanka with 1-minute resolution and we need to keep data for 30 days**. Run this command in your kadiradb-metrics shell.

``` javascript
❯ open({database: 'temperature', resolution: 60, retention: 2592000, epochTime: 86400, maxROEpochs: 10, maxRWEpochs: 2})
{}
```

*Note: The resolution, retention, epochTime parameters are given in seconds.*

You can verify that the database is crated successfully with the info command.

``` javascript
❯ info()
{ databases: [ { database: 'temperature', resolution: 60, retention: 0 } ] }
```

### Writing Data

From the shell, select the database to write to with the `use` function and write some temperature values with the `put` function.

``` javascript
❯ use('temperature')
❯ var time = Math.floor(Date.now()/1000);
❯ var fields = ['WP', 'Colombo', 'Dematagoda'];
❯ put({"timestamp": time, fields: fields, value: 100, count: 10})
```

### Reading Data

The get method can be used to read data from the database.

``` javascript
❯ var groupBy = [true, true, true];
❯ get({startTime: time-120, endTime: time+60, fields: fields, groupBy: groupBy})
{ groups: [ { fields: [Object], points: [Object] } ] }
❯ pretty(_)
{
  ...
}
```

