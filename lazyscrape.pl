#!/usr/bin/perl -w

# Tool to let you scrape the bits of the 1.x driver you need
# to get a working install of mongoe. May break without notice
# due to external changes, USE AT YOUR OWN RISK

# Usage: Run where this script is.
use LWP::Simple;

my $module = 'MongoDB';
my @files = qw{Error.pm _Link.pm _Protocol.pm _Types.pm};
my $basepath = q{https://raw.githubusercontent.com/mongodb/mongo-perl-driver/master/lib/MongoDB};

mkdir($module) || die "Failed to make module dir: $!\n";
chdir($module) || die "Failed to enter module dir: $!\n";
foreach my $file (@files)
	{
	print "Fetching file [$file]\n";
	mirror("$basepath/$file", $file) || die "Failed to fetch file: $!\n";
	}
print "Done\n";
