open(IN, "dataSet01.dat");
open(OUT1, ">dataSet02.dat");
open(OUT2, ">dataSet03.dat");
open(OUT3, ">dataSet04.dat");
open(OUT4, ">dataSet05.dat");

while($line = <IN>) {
	$out1 = $line;
	$out2 = $line;
	$out3 = $line;
	$out4 = $line;

	$out1 =~ s/(.)(.{3})(.{5})/\3\2\1/;
	$out2 =~ s/(.{2})(.{7})/\2\1/;
	$out3 =~ s/(.)(.{2})(.{3})(.{3})/\4\3\2\1/;
	$out4 =~ s/(.)(.)(.)(.{3})(.{3})/\3\5\1\4\2/;

	print OUT1 $out1;
	print OUT2 $out2;
	print OUT3 $out3;
	print OUT4 $out4;
}

close OUT1;
close OUT2;
close OUT3;
close OUT4;
