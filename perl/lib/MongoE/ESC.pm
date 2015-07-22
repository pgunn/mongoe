#!/usr/bin/perl -w

use strict;

package MongoE::ESC;

require Exporter;
require AutoLoader;

our @ISA = qw(Exporter AutoLoader);
our @EXPORT_OK = qw(
			esc_registry_init
			esc_docshape_known
			esc_learn_docshape
			esc_densepackify
			esc_loosepackify
			); # Functions exposed to clients
our @EXPORT = @EXPORT_OK;

our %ESRegistry; # Package-Global, stores shapes

##########################
# Note: No functions in this library should read or write to the network,
#	meaning they should not touch STDIN or STDOUT
#	If you're adding anything in here that would do I/O (apart from STDERR),
#	pass it in or return it back.

=pod

=head1 NAME

MongoE::ESC - Endpoint Shape Compression

=head1 SYNOPSIS

This manages the registry for the ESC feature. With this feature, a wire protocol client and server learn new opcodes that allow for a negotiated alternative to the standard opcodes that lets repeated use of payloads with the same shape to omit the shape (more densely packing the data) and instead include a shape identifier. The server side of a connection unpacks the document, either parsing it directly (if a mongod or mongos - neither presently implemented) or repacking it with the shape in its registry and sending it on (if a mongoe).

First milestone is alternatives to OP_INSERT. 

=head1 METHODS

=cut

=pod

B<esc_registry_init(DEST)>

Initialise or reset the ESC registry for a given destination (key). ESC is separately negotiated with every system a given client may talk to, because otherwise their shapeID schemes would clash

=cut

sub esc_registry_init($)
{
my ($dest) = @_;

$ESRegistry{$dest} = \();
}

=pod

B<esc_docshape_known(DEST, DOC)>

Scans a document, extracts its shape, and looks that shape up in its registry for that destination.
If it's found, returns the docshape (as an integer)
If it's not found, returns undef.

=cut

sub esc_docshape_known($$)
{
my ($dest, $doc) = @_;

}

=pod

B<esc_learn_docshape(DEST, DOC, SHAPEID)>

Scans a document, extracts its shape, and registers it in the ESC Cache for that DEST using the given SHAPEID

=cut

sub esc_learn_docshape($$$)
{	
my ($dest, $doc, $shapeid) = @_;

}

#########
# Things that change how a payload is packed
#
# We call the default WP packing "loosepacked"
# We call the ESC packing "densepacked"

=pod

B<esc_densepackify(SHAPEID, DEST, LOOSEDOC)>

Given a SHAPEID (regular perl number, not packed) and a DEST it belongs to,
and a LOOSEDOC, return a densepacked version of the same doc.

=cut

sub esc_densepackify
{
my ($shapeid, $loosedoc) = @_;

}

=pod

B<esc_loosepackify(DENSEDOC, SRC)>

Given a DENSEDOC and the place it came from (usually - more loosely the server
for the shapeid we'll read from the doc), repack the document with its shape

=cut

sub esc_loosepackify
{
my ($densedoc, $src) = @_;

}

#########################
# Private methods
sub doc_split($); # Prototype for recursive functions
sub doc_split($)
{	# Given a document, return two things:
	# first: a (binary) string composed of the shape parts of the document sans values
	# second: a (binary) string composed of the value parts of the documents sans shape
	# Neither of these have separators beyond those of their type.
	#
	# DO NOT PASS multiple docs to this. Split them first.
	#
	# The binary shape representation includes a document length, corrected for
	# the document size without payloads
my ($indoc) = @_;
use bytes; # Turn unicode off for this show
# XXX Should docshape and rawdense be strings or arrays?
my $docshape = ''; # Header (prepended later), shapes only. Deep (contains subdoc shapes)
my $rawdense = ''; # Packed values
open(my $doc, "<", \$indoc) || die "Failed to open doc for shape split\n";
undef = altread($doc, 4); # Number of bytes. We don't care - we recalculate this
while(my $val = altread($doc, 1))
	{ # Each loop is responsible for advancing the reads up to the next element
	local $/ = "\0";	# Set line-terminator to null values so any call to readline will
				# read a string
	if($val == hex('0x01') ) # Double
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 8); # defined as 8 bytes
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x02') ) # String
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = read_bson_stringtype($doc);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif( ($val == hex('0x03')) || ($val == hex('0x04')) ) # Embedded document or array
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $subdoc_len = altread($doc, 4);
		my $subdoc_len_decoded = unpack('V', $subdoc_len);
		my $subdoc_chopt = altread($doc, $subdoc_len_decoded);
		my $fieldblob = $subdoc_len . $subdoc_chopt; # Unchop it!
		my ($subshape, $subval) = doc_split($fieldblob);
		$docshape .= $val . $fieldname . $subshape;
		$rawdense .= $subval;
		}
	elsif($val == hex('0x05') ) # Binary
		{		# XXX Design decision here:
				# A docshape does not include the length or type of binary data fields; that's part of
				# the packed-values
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldlen = altread($doc, 4);
		my $fieldsubt= altread($doc, 1);
		my $fieldlen_decoded = unpack('V', $fieldlen); # XXX Is this the right unpack string?
		my $fieldsubv= altread($doc, $fieldlen_decoded);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldlen . $fieldsubt . $fieldsubv;
		}
	elsif($val == hex('0x06') ) # Undef (no payload)
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		$docshape .= $val . $fieldname;
		}
	elsif($val == hex('0x07') ) # ObjectId
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 12);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x08') ) # Boolean
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 1);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x09') ) # DateTime
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 8);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x0A') ) # Null
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		$docshape .= $val . $fieldname;
		}
	elsif($val == hex('0x0B') ) # Regex
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldvala = readline($doc);
		my $fieldvalb = readline($doc);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldvala . $fieldvalb;
		}
	elsif($val == hex('0x0C') ) # DBPointer
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $dbpstring = read_bson_stringtype($doc); # FIXME Not sure what this is. Does it belong to docshape or rawdense?
		my $fieldval = altread($doc, 12); # defined as 8 bytes
		$docshape .= $val . $fieldname;
		$rawdense .= $dbpstring . $fieldval; # Guessing dbpstring is a database name?
		}
	elsif($val == hex('0x0D') ) # Javascript
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = read_bson_stringtype($doc);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x0E') ) # (obsolete)
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = read_bson_stringtype($doc);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x0F') ) # Javascript
		{
		# TODO Madness...
		}
	elsif($val == hex('0x10') ) # int32
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 4);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x11') ) # Timestamp
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 8);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0x12') ) # int64
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		my $fieldval = altread($doc, 8);
		$docshape .= $val . $fieldname;
		$rawdense .= $fieldval;
		}
	elsif($val == hex('0xFF') ) # Minkey
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		$docshape .= $val . $fieldname;
		}
	elsif($val == hex('0x7F') ) # Maxkey
		{
		my $fieldname = readline($doc); # Includes the trailing null, useful for re-packing but be careful
		$docshape .= $val . $fieldname;
		}
	}
close($doc);

my $shapelen = length($docshape) + 4; # The 4 is to leave room for the shape descriptor we're about to insert
					# The "use bytes" above ensures unicode doesn't confuse things
my $shapelen_packed = pack('V', $shapelen);
return $shapelen_packed . $docshape, $rawdense;
}

sub altread($$)
{
my ($fh, $len) = @_;
my $val;
my $retcode = read($fh, $val, $len);
if( (! defined $retcode) || ($retcode < $len))
	{return undef;} # We don't care about the distinction here. If it happens we probably will bail
return $val;
}

sub read_bson_stringtype($)
{	# Read one of the bson managed string types.
	# Know: DISCARDS THE NULL, does NOT adjust length
	# Know: NOT the same as the cstring tyle in the spec
my ($fh) = @_;
my $readlen = altread($fh, 4);
my $readlen_decoded = unpack('V', $readlen);
my $readval = altread($fh, $readlen_decoded - 1);
my $should_be_null = altread($fh, 1);
if($should_be_null != "\0")
	{die "Error in unpacking BSON string: no null where expected\n";}
return $readlen_decoded . $readval;
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
