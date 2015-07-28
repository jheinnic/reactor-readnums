var net = require('net');
var fs = require('fs');
var Chance = require('chance');
var mt = new Chance().mersenne_twister();

var nineDigits = function() {
    var number = Math.floor(mt.genrand_res53() * 1000000000);
    var numStr = number.toString();
    var padding = 9 - numStr.length;
    if (padding > 0) {
        var padStr = '';
        var ii;
        for(ii=0; ii<padding; ii++) {
            padStr = padStr + '0';
        }
        numStr = padStr + numStr;
        // console.log(numStr);
    }

    return numStr;
}

var HOST = '192.168.5.6';
var PORT = 4000;
var msgsPerLoop = 100000;

function launchTimedClient() {
    var totalMsgs = 0;
    var totalTime = 0;
    var client = new net.Socket();
    client.connect(PORT, HOST, function() {
        console.log('CONNECTED TO: ' + HOST + ':' + PORT);
        var ii = 0;
        var start = new Date();
    console.log(start);
    console.log("");
        while(true) {
            var loopStart = new Date();
        var buf = '';
            for(ii=0; ii<msgsPerLoop; ii+=100) {
            for(jj=0; jj<10; jj++) {
            // buf = buf + nineDigits() + "\r\n";
                    client.write(nineDigits());
                    client.write("\n");
        }
        // console.log(buf);
                // client.write(buf);
            }
            var atLoop = new Date();
    
            var delta = atLoop - loopStart;
            var deltaSec = delta / 1000;
            var loopRate = msgsPerLoop / deltaSec;
    
            totalTime = atLoop - start;
            totalMsgs = totalMsgs + msgsPerLoop;
            var totalSec = totalTime / 1000;
            var totalRate = totalMsgs / totalSec;
    
            console.log( atLoop );
            console.log( "Just ran " + msgsPerLoop + " messages in " + deltaSec + " seconds, " + loopRate + " messages per second." );
            console.log( "Cumulatively, " + totalMsgs + " messages in " + totalSec + " seconds, " + totalRate + " messages per second\n" );

        var jj;
        var kk;
        for( var ii=0; ii < 1000000; ii++ ) {
        kk = ii * 2;
        jj = kk - 24;
        kk = kk / jj + ii;
        jj = -kk * -ii / 4284.34;
            }
        }
    });

    return client;
}

function launchUntimedClient() {
    var client = new net.Socket();
    client.connect(PORT, HOST, function() {
        console.log('CONNECTED TO: ' + HOST + ':' + PORT);
        var ii = 0;
        while(true) {
            for(ii=0; ii<msgsPerLoop; ii++) {
                client.write(nineDigits());
                client.write("\n");
            }
        }
    });

    return client;
}

function launchBurstClient(burstSize, delay, repeat) {
    var client = new net.Socket();
    client.connect(PORT, HOST, function() {
        console.log('CONNECTED TO: ' + HOST + ':' + PORT);
        var ii = 0;
    for(jj=0; jj<repeat; jj++) {
            for(ii=0; ii<burstSize; ii++) {
                client.write(nineDigits());
                client.write("\n");
            }
        }
    });

    return client;
}

function getBatch(burstSize) {
    var retVal = '';
    for(ii=0; ii<burstSize; ii++) {
        retVal = retVal + nineDigits() + "\n";
    }
    return retVal;
}

function writeDataFile(filePath, burstSize, goal) {
    var goalSize = goal - 1;

    appendForever = function appendForever(err, data) {
        if(err) {
            console.log(err);
            return;
        } else if( goalSize-- == 0 ) {
            console.log("Goal!");
            return;
        }

        var dataSet = getBatch(burstSize);
        fs.appendFile( filePath, getBatch(burstSize), appendForever);
    }

    fs.appendFile( filePath, getBatch(burstSize), appendForever );
}

function writeDataFileSync(filePaths, burstSize, goal) {
    var goalSize = goal;
    var filePath;
    while (goalSize > 0) {
	for (filePath in filePaths) {
	    goalSize = goalSize - 1;
            fs.appendFileSync(filePath, getBatch(burstSize));
	}
    }
}

module.exports = {
    nd: nineDigits,
    launchTimed: launchTimedClient,
    launchUntimed: launchUntimedClient,
    launchBurst: launchBurstClient,
    writeDataFile: writeDataFile,
    writeDataFileSync: writeDataFileSync
};

