#!/usr/bin/perl -w

use strict;

package MongoE::WireProtocol;

require Exporter;
require AutoLoader;

our @ISA = qw(Exporter AutoLoader);
our @EXPORT_OK = qw(
			parse_op
			read_message
			bson_docset_to_docarray
			bson_i_refuse
			generate_protocol_response
			generate_version_response
			bson_response_listDatabases
			); # Functions exposed to clients
our @EXPORT = @EXPORT_OK;

=pod

=head1 NAME

MongoE::WireProtocol - WireProtocol components of MongoE

=head1 SYNOPSIS

Use this to decode and encode information between the MongoDB Wire Protocol and native formats.

=head1 METHODS

=cut

=pod

B<read_message(FILEHANDLE)>

Read a single message from the fh representing the network socket.
Return a hash with the information needed in there. The hash has the
following keys:

=over

=item msglen_decoded

=item d_decoded

=item respto_decoded

=item opcode_decoded

=item raw - binary data from the request

=back

=cut

sub read_message($)
{
my ($fh) = @_;

my %ret;
read($fh, $ret{msglen}, 4); # Size of messages in bytes, including header!
read($fh, $ret{reqid} , 4);
read($fh, $ret{respto}, 4);
read($fh, $ret{opcode}, 4);
$ret{raw} = join('', @ret{"msglen", "reqid", "respto", "opcode"});
$ret{msglen_decoded} = unpack('l', $ret{msglen});
$ret{opcode_decoded} = unpack('l', $ret{opcode});
$ret{reqid_decoded} = unpack('l', $ret{reqid});
$ret{respto_decoded} = unpack('l', $ret{respto});
my $content;

if($ret{msglen_decoded} > 16)
	{read($fh, $content, ($ret{msglen_decoded} - 16));}

parse_op($ret{opcode}, $content, \%ret);

$ret{raw} .= $content;
return %ret;
}

=pod

B<parse_op(OPCODE, REQUEST, OPINFO)>

Given a (still binary) opcode, the full binary request, and a ref to an opinfo hash,
analyse the content of the operation, taking notes in-place in opinfo. Opinfo gains
some of the following fields:

=over

=item	opname

=item	schema

=item	table

=item	bson

=back

The number of fields depends on the operation.

=cut

sub parse_op($$$)
{	# Pass in: (non-decoded) opcode, request
	# Returns: opname, schema, table
	# TODO: opcode is part of opinfo. Refactor arg passing
my ($opcode, $request, $opinfo) = @_;

my $opcode_d = unpack('l', $opcode);
if($opcode_d == 2001) # OP_UPDATE
	{
	my (undef, $coll) = unpack("lZ*", $request); # ZERO, collection, flags, selectorDocument, updateDocument
	my $selectordoc = substr($request, 9+length($coll)); # 2L + strlen + null-termination
		# TODO The selectordoc starts with an int32 that's its (packed) length.
		# We should substr once (no-replace) to pull the updatedocument out,
		# then substr again (with replace) to trim the selectordoc down to its desired parts.
	my ($schema, $table) = split(/\./, $coll);
	@$opinfo{'opname','schema','table'} = ('OP_UPDATE', $schema, $table);
	}
elsif($opcode_d == 2002) # OP_INSERT
	{
	my (undef, $coll) = unpack("lZ*", $request); # Flags, collection, documents
	my $qdoc = substr($request, 5+length($coll)); # 1L + strlen + null-termination
	my @dec_docset = map
				{
				BSON::decode($_, ixhash=>1)
				} bson_docset_to_docarray($qdoc);
	
	my ($schema, $table) = split(/\./, $coll);
	@$opinfo{'opname','schema','table', 'bson'} = ('OP_INSERT', $schema, $table, \@dec_docset );
	}
elsif($opcode_d == 2004) # OP_QUERY
	{
	my (undef, $coll) = unpack("lZ*", $request); # Flags, collection, skip, return, the rest
	my $qdoc = substr($request, 13+length($coll)); # 3L + strlen + null-termination

	my @doc = (BSON::decode($qdoc, ixhash=>1));

	my ($schema, $table) = split(/\./, $coll);
	@$opinfo{'opname','schema','table', 'bson'} = ('OP_QUERY', $schema, $table, \@doc);
	}
elsif($opcode_d == 2005) # OP_GET_MORE
	{
	my (undef, $coll) = unpack("lZ*", $request);
	my ($schema, $table) = split(/\./, $coll);
	@$opinfo{'opname','schema','table'} = ('OP_GET_MORE', $schema, $table);
	}
elsif($opcode_d == 2006) # OP_DELETE
	{
	my (undef, $coll) = unpack("lZ*", $request);
	my $qdoc = substr($request, 9+length($coll)); # 2L + strlen + null-termination
	my @doc = (BSON::decode($qdoc, ixhash=>1));
	my ($schema, $table) = split(/\./, $coll);
	@$opinfo{'opname','schema','table', 'bson'} = ('OP_DELETE', $schema, $table, \@doc );
	}
elsif($opcode_d == 2007) # OP_KILL_CURSORS
	{
	@$opinfo{'opname'} = ('OP_KILL_CURSORS');
	}
elsif($opcode_d == 1) # OP_REPLY
	{
	my(undef, $cursorid, $startfrom, $numret) = unpack("lqll", $request); # first field is response flags
	my $qdoc = substr($request, 20); # 5L
	my @dec_docset = map
				{
				BSON::decode($_, ixhash=>1)
				} bson_docset_to_docarray($qdoc, $numret);
	@$opinfo{'opname', 'bson'} = ('OP_REPLY', 
								\@dec_docset
								);
	#@$opinfo{'opname'} = ('OP_REPLY'); # First try with no BSON decode
	}
else
	{
	@$opinfo{'opname'} = ('OP_UNKNOWN');
	}
}

=pod

B<bson_docset_to_docarray(DATA, NUMDOCS?)>

Given BSON data that can contain multiple documents, and a specifier
how many how many documents should be in there, return an array of
those documents.

=cut

sub bson_docset_to_docarray($;$)
{	
my ($data, $numdocs) = @_;
my @ret;
while(length($data))
	{
	open(my $dfh, "<", \$data) || die "Failed to disassemble data: $!\n";
	read($dfh, my $msglen, 4) || die "Misparse in data disassembly: Partial document read\n";
	close($dfh);
	my $msglen_decoded = unpack('l', $msglen);
	my $thisdoc = substr($data, 0, $msglen_decoded, ''); # Destructive substr
	push(@ret, $thisdoc);
	if(defined $numdocs)
		{$numdocs--;}
	}
if( (defined $numdocs) && ($numdocs != 0))
	{die "Protocol error in unpacking BSON docset\n";}
#print STDERR "PARSED: I saw " . scalar(@ret) . " DOCUMENTS\n";
return @ret;
}

#########
# Functions that generate BSON rather than consume it

=pod

B<bson_i_refuse(REQUESTID)>

Generate an OP_REPLY with a payload amounting to refusing an operation. Pass in
a requestid so the payload will be structured as a reply.

=cut

sub bson_i_refuse
{
my ($reqid) = @_;
my $payload = BSON::encode({'$err' => "Forbidden"});
my $respflags = 0;
vec($respflags, 1,1) = 1; # QueryFailure is true

my $mlength = length($payload) + 36; # 28 from the int32 + 8 from the int64
my $ret = pack('lllllqll', 		# Header:
		$mlength,			# int32 requestID
		int( rand( 2**31 - 1 ) ),	# int32 requestID FIXME Comment incorrect. Code probably ok.
		$reqid,				# int32 "what we're replying to"
		1,				# OP_REPLY
						# --- Begin Body ---
		$respflags,			# int32 responseFlags, bit 1 high
		0,				# int64 CursorID
		0,				# int32 startingFrom
		1)				# int32 numReturned
.		$payload;
return $ret;
}

=pod

B<generate_protocol_response(REQUESTID, FEATURES, MONGOE_VERSION)>

Generate an OP_REPLY with a payload of protocol negotiation specifics. Pass in
a requestid so the payload will be structured as a reply.

=cut

sub generate_protocol_response
{
my ($reqid, $feature_r, $mongoe_version) = @_;

my $bson_true = BSON::Bool->true;
my $bson_now = BSON::Time->new;
my $payload = BSON::encode(	{
				'ismaster' => $bson_true,
				'maxBsonObjectSize' => 16777216,
				'maxMessageSizeBytes' => 48000000,
				'maxWriteBatchSize' => 1000,
				'localTime' => $bson_now,
				'maxWireVersion' => 3,
				'minWireVersion' => 0,
				'mongoeVersion'  => $mongoe_version,
				'mongoeFeatures' => $feature_r,
				'ok' => 1
				});
my $mlength = length($payload) + 36; # 28 from the int32, 8 from the int64

my $ret = pack('lllllqll', 		# Header:
		$mlength,			# int32 requestID
		int( rand( 2**31 - 1 ) ),	# int32 requestID FIXME comment incorrect
		$reqid,				# int32 "what we're replying to"
		1,				# OP_REPLY
						# --- Begin Body ---
		0,				# int32 responseFlags, all bits low
		0,				# int64 CursorID
		0,				# int32 startingFrom
		1)				# int32 numReturned
.		$payload;

return $ret;
}

=pod

B<generate_version_response(REQUESTID, FEATURES)>

Generate an OP_REPLY with a payload of protocol negotiation specifics. Pass in
a requestid so the payload will be structured as a reply.

=cut

sub generate_version_response
{
my ($reqid) = @_;

my $bson_true = BSON::Bool->true;
my $bson_false = BSON::Bool->false;
my $bson_now = BSON::Time->new;
my @versionarray = [3,0,99];
my $payload = BSON::encode(	{
				'version' => "3.0.99",
				'sysInfo' => 'sn5176 sn5176 9.0.2.2 sin.0 CRAY Y-MP',
				'loaderFlags' => 'use strict',
				'compilerFlags' => '-w',
				'versionArray' => \@versionarray,
				'javascriptEngine' => 'V8',
				'bits' => 64,
				'debug' => $bson_false,
				'maxBsonObjectSize' => 16777216,
				'ok' => 1
				});
my $mlength = length($payload) + 36; # 28 from the int32, 8 from the int64

my $ret = pack('lllllqll', 		# Header:
		$mlength,			# int32 requestID
		int( rand( 2**31 - 1 ) ),	# int32 requestID FIXME comment incorrect
		$reqid,				# int32 "what we're replying to"
		1,				# OP_REPLY
						# --- Begin Body ---
		0,				# int32 responseFlags, all bits low
		0,				# int64 CursorID
		0,				# int32 startingFrom
		1)				# int32 numReturned
.		$payload;

return $ret;
}

=pod

B<bson_response_listDatabases(REQUESTID, DBINFO)>

Generate an OP_REPLY with a payload that's a valid response to listDatabases.
DBInfo should be a hash with its values being distinct databases mongoe knows
about.

=cut

sub bson_response_listDatabases
{
my ($reqid, %dbinfo) = @_;
print STDERR "Pre\n";
print STDERR Dumper(\%dbinfo);
my $totalsize = schinfo_totalsize(%dbinfo);
my @databases = values(%dbinfo);
my $payload = BSON::encode({'ok' => 1, databases => \@databases, totalSize => $totalsize});
my $mlength = length($payload) + 36; # 28 from the int32, 8 from the int64

my $ret = pack('lllllqll', 		# Header:
		$mlength,			# int32 requestID
		int( rand( 2**31 - 1 ) ),	# int32 requestID FIXME comment is incorrect. Code probably fine.
		$reqid,				# int32 "what we're replying to"
		1,				# OP_REPLY
						# --- Begin Body ---
		0,				# int32 responseFlags, all bits low
		0,				# int64 CursorID
		0,				# int32 startingFrom
		1)				# int32 numReturned
.		$payload;

return $ret;
}

sub schinfo_totalsize
{
my %schemot = @_;
my $size = 0;
foreach my $schema (keys %schemot)
	{
	$size += $schemot{$schema}{sizeOnDisk};
	}
return $size;
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
