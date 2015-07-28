#!/usr/bin/perl
#runFromList.pl

my($jobName, $sourceFile, $totalToWrite, $reportInterval, $loopLength) = @ARGV;

print <<EOF;
Launching <${jobName}>
====================
** Sending <${totalToWrite}> messages from <${sourceFile}>
** Checking the clock every <${loopLength}> rows.
** Report <${reportInterval}> seconds after previous clock check report.
EOF

# Opening source data file for a canned example stream.
open(IN, $sourceFile) || die "Cannot read <$sourceFile>";
use IO::Socket::INET;

# Flush after every 16K written
$| = 8192;
my($socket, $client_socket);

# creating object interface of IO::Socket::INET modules which internally 
# creates socket, binds and connects to TCP server on specific port.
$socket = new IO::Socket::INET (
PeerHost => '127.0.0.1',
PeerPort => '4000',
Proto => 'tcp',
) or die "ERROR in Socket Creation : $!\n";

print "TCP Connection Success.\n";

my $lastCounter = $totalToWrite;
my $totalCounter = $totalToWrite;

my $firstClock = time();
my $lastClock = $firstClock;
my $nextClock = $lastClock + $reportInterval;

print "Load generation begins at ${nextClock}\n";
while($totalCounter > 0) {
	$totalCounter = $totalCounter - ($loopLength*3);
	if ($totalCounter < 0) { $totalCounter = 0; }

	my $line = '';
	my $loopCounter = $loopLength;
	while($loopCounter-- > 0) {
		$line = <IN>;
		print $socket $line;
		$line =~ s/(.{3})(.{6})/\2\1/;
		print $socket $line;
		$line =~ s/(.{3})(.{6})/\2\1/;
		print $socket $line;
	}

	my $thisClock = time();
	if (($thisClock >= $nextClock) || ($totalCounter == 0)) {
		my $interval = $thisClock - $lastClock;
		my $duration = $thisClock - $firstClock;
		my $progress = $lastCounter - $totalCounter; 
		my $overall = $totalToWrite - $totalCounter;
		my $rate1, $rate2;
	       
	 	if ($interval > 0) {
			$rate1 = (1.0 * $progress)/$interval;
		} else {
			$rate1 = "NaN";
		}

		if ($duration > 1) {
			$rate2 = (1.0 * $overall)/$duration;
		} else {
			$rate2 = "NaN";
		}

		print "Over previous ${interval} seconds, ${progress} messages were sent.  (current rate = ${rate1})\n";
		print "Since launched ${duration} seconds ago, ${overall} messages have been sent.  (cumulative rate = ${rate2})\n\n";

		$lastCounter = $totalCounter;
		$lastClock   = $thisClock;
		$nextClock   = $thisClock + $reportInterval;
	}
}

$socket->flush();
sleep 2;
$socket->close();

print "\nFinished!  Type <Enter> to exit...";
my $junk = <STDIN>;

