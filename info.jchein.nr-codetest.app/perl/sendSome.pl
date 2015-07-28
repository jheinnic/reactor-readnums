#!/usr/bin/perl
#sendSome.pl

use IO::Socket::INET;

# Flush after every write
$| = 1;
my($socket, $client_socket);

# creating object interface of IO::Socket::INET modules which internally creates 
# socket, binds and connects to the TCP server running on the specific port.
$socket = new IO::Socket::INET (
PeerHost => '192.168.5.6',
PeerPort => '4000',
Proto => 'tcp',
) or die "ERROR in Socket Creation : $!\n";

print "TCP Connection Success.\n";

# read the socket data sent by server.
# $data = <$socket>;
# we can also read from socket through recv()  in IO::Socket::INET
# $socket->recv($data,1024);
# print "Received from Server : $data\n";

# write on the socket to server.
$data = "184720027\n857317233\n119922947\n918274482\n";
print OUT $data;
print $socket $data;
# we can also send the data through IO::Socket::INET module,
# $socket->send($data);

sleep (10);
$socket->close();

