#!/usr/bin/perl
#runFromList.pl

my($jobName, $sourceDir, $fileCount, $numPerFile) = @ARGV;
open(OUT, ">${jobName}.log");

print OUT <<EOF;
Launching <${jobName}>
====================
** Sending <${fileCount}> message files from <${sourceDir}>
** Each file has <${numPerFile}> messages
EOF

print <<EOF;
Launching <${jobName}>
====================
** Sending <${fileCount}> message files from <${sourceDir}>
** Each file has <${numPerFile}> messages
EOF

use IO::Socket::INET;

# Flush after every 16K written
$| = 32768;
my($socket, $client_socket);

# creating object interface of IO::Socket::INET modules which internally 
# creates socket, binds and connects to TCP server on specific port.
$socket = new IO::Socket::INET (
PeerHost => '127.0.0.1',
PeerPort => '4000',
Proto => 'tcp',
) or die "ERROR in Socket Creation : $!\n";

print "\nTCP Connection Success.\n";
print OUT "\nTCP Connection Success.\n";

my $firstClock = time();
my $lastClock = $firstClock;
my $nextClock = time();

my $fileCounter = 0;
my $overall = 0;
my $progress = 0;
my $fileFormat = $sourceDir . "/file%03d.dat";

print "Load generation begins at ${nextClock}\n";
print OUT "Load generation begins at ${nextClock}\n";
while($fileCounter <= $fileCount) {
    # Opening source data file for a canned example stream.
    $sourceFile = sprintf($fileFormat, $fileCounter++);
    open(IN, $sourceFile) || die "Cannot read <$sourceFile>";
    print $socket <IN>;
    close IN;

    my $thisClock = time();
    if (($thisClock >= $nextClock) || ($totalCounter == 0)) {
        my $interval = $thisClock - $lastClock;
        my $duration = $thisClock - $firstClock;
        $overall  = $overall + $numPerFile;
	$progress = $numPerFile;
        my $rate1, $rate2;
           
        if ($interval > 0) {
            $rate1 = (1.0 * $numPerFile)/$interval;
        } else {
            $rate1 = "NaN";
        }

        if ($duration > 1) {
            $rate2 = (1.0 * $overall)/$duration;
        } else {
            $rate2 = "NaN";
        }

        print "Over previous ${interval} seconds, ${progress} messages were sent.  (current rate = ${rate1})\n";
        print OUT "Over previous ${interval} seconds, ${progress} messages were sent.  (current rate = ${rate1})\n";
        print "Since launched ${duration} seconds ago, ${overall} messages have been sent.  (cumulative rate = ${rate2})\n\n";
        print OUT "Since launched ${duration} seconds ago, ${overall} messages have been sent.  (cumulative rate = ${rate2})\n\n";

        $lastClock   = $thisClock;
    }
}

$socket->flush();
sleep 2;
$socket->close();

print OUT "\nFinished!  Type <Enter> to exit...\n";
close OUT;

print "\nFinished!  Type <Enter> to exit...";
my $junk = <STDIN>;

