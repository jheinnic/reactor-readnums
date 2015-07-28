var Chance = require('chance').Chance;
var chance = new Chance();
var mt = chance.mersenne_twister();

function makeRun( startSum, upDownSpread, spreadFactor, lowSumOne, highSumOne, lowStepDelta, highStepDelta, varSteps, totalSteps) {
    var sum = startSum;
    var sequence = [];
    var index = 0;

    var upMax = upDownSpread / 2;
    var downMin = upDownSpread / 2;
    var minSum = lowSumOne;
    var maxSum = highSumOne;
    var randomSpread = upDownSpread;
    while (index++ < totalSteps) {
       var sumSpread = maxSum - minSum;
       // var avgSum = sumSpread / 2;
       var maxToSum = maxSum - sum;
       var minToSum = sum - minSum;
       
       var maxRatio = maxToSum / sumSpread;
       var minRatio = minToSum / sumSpread; 
       var nextUpMax = Math.round(randomSpread * maxToSum / sumSpread);
       var nextDownMin = Math.round(randomSpread * minToSum / sumSpread);
       console.log("\n*** Step #" + index + " ***");
       console.log( minToSum + " / " + sumSpread + " --> " + minRatio + " * " + randomSpread + " == " + (-1 * nextDownMin) );
       console.log( maxToSum + " / " + sumSpread + " --> " + maxRatio + " * " + randomSpread + " == " + nextUpMax );

       var nextUp = mt.genrand_int31() % nextUpMax;
       var nextDown = (mt.genrand_int31() % nextDownMin) * -1;
       var sumDelta = nextUp + nextDown;
       sum = sum + sumDelta;
       sequence.push(nextUp, nextDown);

       console.log( "====================\n" + nextUp + "\n" + nextDown + "\n====================>   " + sumDelta + ", " + sum + "\n        " + minSum + " to " + maxSum + "\n" + "        " + (nextDownMin*-1) + " to " + nextUpMax + " @ " + randomSpread + "\n" );
       if (index <= varSteps) {
           randomSpread = randomSpread * spreadFactor;
	   minSum = minSum + lowStepDelta;
	   maxSum = maxSum + highStepDelta;
       }
    }
    sequence.push(sum);

    return sequence;
}

var steps01 = makeRun(0, 100, 1.00475, -60, 200, 10, 25, 50, 120);
var steps02 = makeRun(0, 90, 1.008, -60, 150, 10, 32, 50, 120);
var steps03 = makeRun(0, 100, 1.00475, -60, 200, 10, 25, 50, 120);
var steps04 = makeRun(0, 90, 1.008, -60, 150, 10, 32, 50, 120);


var steps05 = makeRun(steps01[240], 64, 1, steps01[240]-150, steps01[240]+150, 0, 0, 0, 18);
var steps06 = makeRun(steps02[240], 64, 1, steps02[240]-150, steps02[240]+150, 8, 0, 0, 18);
var steps07 = makeRun(steps03[240], 64, 1, steps03[240]-150, steps03[240]+150, 8, 0, 0, 18);
var steps08 = makeRun(steps04[240], 64, 1, steps04[240]-150, steps04[240]+150, 8, 0, 0, 18);


console.log( 'int[] bootstrap01 = new int[] {' + steps01.join(', ') + '};');
console.log( 'int[] ongoing01 = new int[] {' + steps05.join(', ') + '};\n');
console.log( 'int[] bootstrap02 = new int[] {' + steps02.join(', ') + '};');
console.log( 'int[] ongoing02 = new int[] {' + steps06.join(', ') + '};\n');
console.log( 'int[] bootstrap03 = new int[] {' + steps03.join(', ') + '};');
console.log( 'int[] ongoing03 = new int[] {' + steps07.join(', ') + '};\n');
console.log( 'int[] bootstrap04 = new int[] {' + steps04.join(', ') + '};');
console.log( 'int[] ongoing04 = new int[] {' + steps08.join(', ') + '};\n');
       
       
       
