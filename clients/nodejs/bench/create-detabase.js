var Client = require('../');

var ADDRESS = 'kmdb://localhost:19000';

var client = new Client(ADDRESS);
client.connect(function (err) {
  if(err) {
    console.error(err);
    process.exit(1);
  }

  start();
});

function start () {
  var params = {
    name:          "test",
		resolution:    60000000000,
		epochDuration: 3600000000000,
		payloadSize:   16,
		segmentLength: 100000,
		maxROEpochs:   10,
		maxRWEpochs:   2,
  };

  client.open(params, function (err, res) {
    if(err) {
      console.error(err);
    }

    process.exit(0);
  });
}
