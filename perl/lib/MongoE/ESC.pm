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
