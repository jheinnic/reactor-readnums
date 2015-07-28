var beClient = require('./beClient');
var launchTimed = beClient.launchTimed;
var launchUntimed = beClient.launchUntimed;
var launchBurst = beClient.launchBurst;

var cliOne = launchBurst(500);
// var cliTwo = launchUntimed();
// var cliThree = launchUntimed();
