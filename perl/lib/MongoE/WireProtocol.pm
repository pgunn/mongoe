#!/usr/bin/perl -w

use strict;

package MongoE::WireProtocol;

require Exporter;
require AutoLoader;

our @ISA = qw(Exporter AutoLoader);
our @EXPORT_OK = qw(); # Functions exposed to clients
our @EXPORT = @EXPORT_OK;

=pod

=head1 NAME

MongoE::WireProtocol - WireProtocol components of MongoE

=head1 SYNOPSIS

Use this to decode and encode information between the MongoDB Wire Protocol and native formats.

=head1 METHODS

=cut

=pod

Document method here

=cut


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
