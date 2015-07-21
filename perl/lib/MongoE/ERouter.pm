#!/usr/bin/perl -w

use strict;

package MongoE::ERouter;

my $proxy; # Holds global proxy state

require Exporter;
require AutoLoader;

our @ISA = qw(Exporter AutoLoader);
our @EXPORT_OK = qw(
			init_proxy
			setup_proxy
			route_for_destination
			get_router_iterable
			); # Functions exposed to clients
our @EXPORT = @EXPORT_OK;

##########################
# Note: No functions in this library should read or write to the network,
#	meaning they should not touch STDIN or STDOUT
#	If you're adding anything in here that would do I/O (apart from STDERR),
#	pass it in or return it back.

=pod

=head1 NAME

MongoE::ERouter - Router components for mongoe

=head1 SYNOPSIS

This manages backend connections for mongoe and related setup

=head1 METHODS

=cut

=pod

B<setup_proxy(CFGSTRING)>

Read a config string saying where to get routing information, do the right thing to 
load that

=cut

sub setup_proxy($)
{
my ($cfg) = @_;
my %ret;

my ($mode, $args) = split(':', $cfg, 2);
if($mode eq 'file')
	{%ret = load_proxyinfo_from_file($args);}
else	{die "Unsupported proxy method [$mode]\n";}

$proxy = \%ret;
}


=pod

B<load_proxyinfo_from_file(FILENAME)>

(Private method)
Load a colon-separated file that maps
SCHEMA:HOST:PORT

=cut

sub load_proxyinfo_from_file($)
{	
my ($fn) = @_;
my %ret;
open(my $fh, $fn) || die "Could not open proxy descriptor file [$fn]:$!\n";
while(<$fh>)
	{
	tr/\n\r\f//d; # Remove newlines and stuff
	s/#.*//; # Strip comments
	s/^\s+//;
	s/\s+$//;
	next if(/^$/); # Skip blank lines
	my($schema, $host, $port) = split(':', $_);
	$ret{$schema}{host} = $host;
	$ret{$schema}{port} = $port;
	#print "Learned schema [$schema]\n";
	}
close($fh);
if(! defined($ret{_DEFAULT}))
	{die "You must define a _DEFAULT schema\n";}

return %ret;
}

=pod

B<route_for_destination(SCHEMA)>

Given a schema, return the (Host,Port) pair it maps to

=cut

sub route_for_destination($)
{	# Given a schema, return the (host,port) pair it maps to
my ($schema) = @_;

my ($host, $port); # Return this
if(! defined($$proxy{$schema}))
	{$schema = "_DEFAULT";} # We should always have entries for this

$host = $$proxy{$schema}{host};
$port = $$proxy{$schema}{port};
return ($host, $port);
}


=pod

B<get_router_iterable()>

Packages the entire routing config up into a single data structure
for something to walk it efficiently by endpoint rather than by schema served.
Returns a multidimensional hash with ip and port as the 2 keys, and the value
being a reference to a list of the schemas served by the endpoint. 
TODO: Evaluate Key-value with URL as key, value still as listref or maybe CSV

=cut

sub get_router_iterable
{	
my %ret;
my @schemot = keys %$proxy;
foreach my $schema (@schemot)
	{
	my $host = $$proxy{$schema}{host};
	my $port = $$proxy{$schema}{port};
	push(@{$ret{$host}{$port}}, $schema);
	}
return %ret;
}

1;

__END__

=pod

=head1 TO DO

Many improvements

=head1 BUGS

Probably Many

=head1 AUTHORS

Pat Gunn <pgunn@dachte.org>

=cut
