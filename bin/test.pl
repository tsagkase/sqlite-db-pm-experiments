#! /usr/bin/perl -w

package Tsagkase::BukuWrapper;

use strict;

use lib::abs('../lib');

use Log::Log4perl qw(get_logger :levels);
our $log = get_logger(__PACKAGE__);

# use JSON;
# use Date::Parse;
# use File::Slurp;
# use JSON::XS qw(decode_json);

use Data::Dumper;

sub new {
	my ($class, $db) = @_;
	
	my $self = {db => $db};
	bless $self, $class;
}

sub test {
	my ($self, $id) = @_;
	my $sql = $self->cmd_to_test_count;
	$log->debug("Trying: $sql ... with args: '$id'");
	$self->{db}->lookup($sql, $id);
}

sub cmd_to_test_count {
	"SELECT COUNT(*) FROM bookmarks WHERE id < ?";
}

1;

package Tsagkase::GritWrapper;

use strict;

use lib::abs('../lib');

use Log::Log4perl qw(get_logger :levels);
our $log = get_logger(__PACKAGE__);

# use JSON;
# use Date::Parse;
# use File::Slurp;
# use JSON::XS qw(decode_json);

use Data::Dumper;

sub new {
	my ($class, $db) = @_;
	
	my $self = {db => $db};
	bless $self, $class;
}

sub ls {
	my ($self, $id) = @_;
	my $sql = $self->cmd_to_test_count;
	$log->debug("Trying:\n$sql");
	$self->{db}->lookup($sql);
}

sub cmd_to_test_count {
	q(
with recursive
descendants(root, descendant, completed) as (
select node_id, node_id, 0 from nodes where not exists (select 1 from links where node_id=dest_id)
union
select descendants.root, node_id, case when node_completed is null then 0 else 1 end from nodes JOIN links ON dest_id = node_id JOIN descendants ON descendants.descendant = origin_id 
)
select * from (select round(avg(completed), 1) as pct_completed, root, node_name from descendants, nodes ON root=node_id where root <> descendant group by root) where pct_completed<1 order by root;
	);
}

1;


package main;

use strict;

use Data::Dumper;
use Log::Log4perl qw(get_logger :levels);

$0=~s/\.pl//;   # set executable name to avoid Perl executable reflected on Oracle session connection name.
                # trick works for Perl version >=5.8.7 and Linux kernel versions >=2.6.13 (see https://www.perlmonks.org/?node_id=500895)

use Tsagkase::DB;

our $log4perl_conf = "./conf/buku-utils-log4perl.conf";
Log::Log4perl::init($log4perl_conf);
our $log = get_logger("Tsagkase");

my $dbfile = $ARGV[0] || '/tmp/grb.db';
$log->info("Connecting to database $dbfile");
my $db = new Tsagkase::DB($dbfile);
my $grit = new Tsagkase::GritWrapper($db);
my @res = $grit->ls();
$log->debug(Dumper(@res));

=comment
$log->info("Checking for buku bookmarks");
my $buku = new Tsagkase::BukuWrapper($db);

my $res = $buku->test($ARGV[1] || 0);
$log->info("Results count: $res");
=cut



$db->disconnect;

sub get_log_filename {
	return "./log/buku-utils.log";
}

1;

