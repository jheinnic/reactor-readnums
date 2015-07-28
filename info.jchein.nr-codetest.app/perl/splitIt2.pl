open(IN, "dataSet01.dat");

sub closeFiles {
    close OUT1;
    close OUT2;
    close OUT3;
    close OUT4;
    close OUT5;
}

sub openFiles {
    my $fileNum = $_[0];
    my $fileName = sprintf("file%03d.dat", $fileNum);
    open(OUT1, ">dataSet01/${fileName}");
    open(OUT2, ">dataSet02/${fileName}");
    open(OUT3, ">dataSet03/${fileName}");
    open(OUT4, ">dataSet04/${fileName}");
    open(OUT5, ">dataSet05/${fileName}");
}

my $nextRunId = 0;
my $entriesPerFile = 1000000;
my $runLinesLeft = 0;
while($line = <IN>) {
	if ($runLinesLeft == 0) {
		closeFiles();
		openFiles($nextRunId);
		$nextRunId = $nextRunId + 1;
		$runLinesLeft = $entriesPerFile - 1;
	} else {
		$runLinesLeft = $runLinesLeft - 1;
	}

	$out1 = $line;
	$out2 = $line;
	$out3 = $line;
	$out4 = $line;
	$out5 = $line;

	$out2 =~ s/(.)(.{3})(.{5})/\3\2\1/;
	$out3 =~ s/(.{2})(.{7})/\2\1/;
	$out4 =~ s/(.)(.{2})(.{3})(.{3})/\4\3\2\1/;
	$out5 =~ s/(.)(.)(.)(.{3})(.{3})/\3\5\1\4\2/;

	print OUT1 $out1;
	print OUT2 $out2;
	print OUT3 $out3;
	print OUT4 $out4;
	print OUT5 $out5;
}

closeFiles();
close(IN);
