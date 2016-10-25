#!/usr/bin/perl
#sendSome.pl

use IO::Socket::INET;

# Try to write in 16K buffers
$| = 16384;
my($socket, $client_socket);

# creating object interface of IO::Socket::INET modules which internally 
# creates socket, binds and connects to TCP server on specific port.
$socket = new IO::Socket::INET (
PeerHost => '127.0.0.1',
PeerPort => '4000',
Proto => 'tcp',
) or die "ERROR in Socket Creation : $!\n";

$socket->sockopt(SO_SNDBUF, 16384);

print "TCP Connection Success.\n";

$msgCount = 0;
$nextClock = time() + 30;
$thisLineCount = 0;
$nextLineCount = $thisLineCount + 100;
@thisBuffer = ();
while(true) {
	$thisLine = int((rand() * 1000000000) - 0.5);
	while(length($thisLine) < 9) {
		$thisLine = "0" . $thisLine;
	}
	push(@thisBuffer, $thisLine);
	# print $socket $thisLine;
	$thisLineCount++;
	if ($thisLineCount == $nextLineCount) {
		push(@thisBuffer, "");
		$thisBuffer = join("\n", @thisBuffer);
		# print STDOUT "<<$thisBuffer>>";
		print $socket $thisBuffer;
		@thisBuffer = ();
		$nextLineCount = $thisLineCount + 100;

		$thisClock = time();
		if ($thisClock >= $nextClock) {
			print "$thisLineCount \@ $thisClock: $thisLine\n\n";
			$nextClock = $thisClock + 30;
		}
	}
}

$socket->close();

