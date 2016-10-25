#!/usr/bin/perl

# creating object interface of IO::Socket::INET modules which internally 
# creates socket, binds and connects to TCP server on specific port.

$seed = <STDIN>;
$maxLines = <STDIN>;
chomp $seed;
chomp $maxLines;
$seed = int($seed);
$maxLines = int($maxLines);

srand($seed);

$msgCount = 0;
$thisLineCount = 0;
$nextLineCount = $thisLineCount + 100;
@thisBuffer = ();
while($thisLineCount < $maxLines) {
	$thisLine = int(rand(1000000000));
	while(length($thisLine) < 9) {
		$thisLine = "0" . $thisLine;
	}
	push(@thisBuffer, $thisLine);
	$thisLineCount++;
	if ($thisLineCount == $nextLineCount) {
		push(@thisBuffer, "");
		$thisBuffer = join("\n", @thisBuffer);
		print STDOUT "$thisBuffer";
		@thisBuffer = ();
		$nextLineCount = $thisLineCount + 100;
	}
}

