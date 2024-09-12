#! /usr/bin/perl -w

package Tsagkase::DB;
use strict;

use Log::Log4perl qw(get_logger :levels);
our $log = get_logger(__PACKAGE__);

use DBI qw(:sql_types);
use DBD::SQLite;
use DBD::SQLite::Constants qw/:file_open/;

use Data::Dumper;

#use Tsagkase::Util;
#our @ISA = qw(DBI::db);

sub new {
    my ($class, $dbfile) = @_;
    my $self = {};
    bless $self, $class;

    $log->info("Connecting to SQLite database: $dbfile");
 	my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile", undef, undef,
	{
		AutoCommit => 0
		, RaiseError => 0
		, LongReadLen => 10_000_000, LongTruncOk => 1	# TODO: are these meaningful here?
		# , sqlite_open_flags => SQLITE_OPEN_READONLY
		# , sqlite_see_if_its_a_number => 1
	});
	if (! defined $dbh) {
		$log->fatal("FATAL: SQL Error: Could not connect to database: $dbfile");
		$self->app_abort;
	}
	$dbh->{RaiseError} = 1;
	# TODO: datetime format?
	# $dbh->do("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
	$dbh->do("PRAGMA foreign_keys = ON");	# supported since 3.6.19 (Oct 14, 2009 and DBD::SQLite 1.26_05)
	# $dbh->do("PRAGMA synchronous = OFF");	# Will prevent SQLite from doing fsync's when writing (slows down non-transactional writes significantly) at the expense of some peace of mind
	# $dbh->do("PRAGMA cache_size = 800000");	# Will allocate 800M for DB cache; the default is 2M. Sweet spot probably lies somewhere in between.

    $self->{h} = $dbh;

	# TODO: set timezone?
	# $self->{timezone} = $self->timezone;
    
    return $self;
}

sub h {shift->{h}}

sub disconnect {
	my $self = shift;
	eval {
		$self->h->disconnect or do {
			# It looks like this doesn't get invoked
			my $err = $self->h->errstr;
			$log->error("Failed to disconnect $err");
		};
		1;
	};
	if ($@) {
		# catches error not caught above
		$log->error("Failed to disconnect $@");
		$self->app_abort;
	}
	$log->debug("Disconnected!");
}

sub commit {
	my $self = shift;
	eval {
		$self->h->commit or do {
			# It looks like this doesn't get invoked
			my $err = $self->h->errstr;
			$log->error("Failed to commit: $err");
		};
		1;
	};
	if ($@) {
		# catches error not caught above
		$log->error("Failed to commit: $@");
		$self->app_abort;
	}
	$log->debug("Committed!");
}

sub lookup {
	my ($self, $cmd, @bindVars) = @_;
	my $res;
	eval {$res = $self->{h}->selectall_arrayref($cmd, undef, @bindVars)};
	if ($@) {
		$self->sql_error($@);
	}
	if (wantarray) {
		return () unless @$res;
		if (scalar(@$res) == 1) {
			return @{$res->[0]};
		} elsif (scalar(@{$res->[0]}) == 1) {
			return map($_->[0], @$res);
		} else {
			return @$res;
		}
	} else {
		return @$res ? $res->[0][0] : undef;
	}
}

sub lookup_array {
	my ($self, $cmd, @bindVars) = @_;
	my $res;
	eval {$res = $self->{h}->selectall_arrayref($cmd, undef, @bindVars)};
	if ($@) {
		$self->sql_error($@);
	}
	return @$res;
}

sub lookup_hash {
	my ($self, $cmd, @bindVars) = @_;
	my $sth = $self->h->prepare($cmd) or do {
		$log->error("SQL prepare failed: " . $self->h->errstr);
		die $self->h->errstr;
	};
	eval {
		$sth->execute(@bindVars) or do {
			# It looks like this doesn't get invoked
			$log->error("SQL execute failed: " . $sth->errstr);
			die $sth->errstr;
		};
		1;
	};
	if ($@) {
		# catches are not caught above
		$self->sql_error($@);
	}
	my @a = ();
	while (my $hRow = $sth->fetchrow_hashref("NAME_uc")) {
		push @a, $hRow;
	}
	return wantarray ? @a :$a[0];
}

sub getrow {
	my ($self, $cmd, @bindVars) = @_;
	my $res;
	eval {$res = $self->h->selectrow_arrayref($cmd, undef, @bindVars)};	
	if ($@) {
		$self->sql_error($@);
	}
	return @$res;
}

sub getcol {
	my ($self, $cmd, @bindVars) = @_;
	my $res;
	eval {$res = $self->h->selectcol_arrayref($cmd, undef, @bindVars)};	
	if ($@) {
		$self->sql_error($@);
	}
	return @$res;
}

sub getcol_relaxed {
	my ($self, $cmd, @bindVars) = @_;
	my $res;
	eval {$res = $self->h->selectcol_arrayref($cmd, undef, @bindVars)};	
	if ($@) {
		return \$@;  ### Return scalar ref
	}
	return $res;
}

sub InsertRow {
	my ($self, $table, $row, $dup_no_abort) = @_;
#	print Dumper($table);
	my %H_BIND_ARGS = (
		BLOB => SQL_BLOB,
		INT => SQL_INTEGER
	);
	my @colArr = ();
	my @valArr = ();
	my @bindArr = ();
#	while (my ($col, $val) = each %$row) {
	for (my $i = 0; $i < scalar(@$row); $i += 2) {
		my ($col, $val) = ($row->[$i], $row->[$i + 1]);
		my ($colName, $len, $colType) = split /\//, $col;
		push @colArr, '"' . uc($colName) . '"';
		if ($colType and $colType eq "FUNCTION") {
			push @valArr, $val;
			## Nothing to put in @bindArr
		} else {
			push @valArr, "?";
			push @bindArr, [ $len ? &MySubstr($val, $len) : $val, $colType || undef ];
		}
	} 
	my $cmd = "INSERT INTO $table (" . join(",", @colArr) . ") VALUES(" .
		join(",", @valArr) . ")";
	my $sth;
	eval {$sth = $self->{h}->prepare($cmd)};
	if ($@) {
		$self->sql_error($@);
	}
	my $cnt = 0;
	foreach my $bindVal (@bindArr) {
		my ($val, $colType) = @$bindVal;
		if ($colType) {
			$sth->bind_param(++$cnt, $val, $H_BIND_ARGS{$colType});
		} else {
			$sth->bind_param(++$cnt, $val);
		}
	}
	my $res;
	eval {$res = $sth->execute};
	if ($@) {
		if ($dup_no_abort and $sth->err == 1) { ## $sth->err == 1: ORA-00001
			$log->error($@);
		} else {
			$self->sql_error($@);  ## Log error and abort
		}
	}
	return $res;
}

sub Update {
	my ($self, $cmd, @bind_vals) = @_;
#	$bind_vals = [] unless defined $bind_vals;
	my %H_BIND_ARGS = (
		BLOB => SQL_BLOB,
		INT => SQL_INTEGER
	);
	my $sth;
	eval {$sth = $self->{h}->prepare($cmd)};
	if ($@) {
		$self->sql_error($@);
	}
	my $cnt = 0;
	foreach my $bind_val (@bind_vals) {
		if (ref($bind_val) && ref($bind_val) eq "ARRAY") {
			my ($val, $bind_arg) = @$bind_val;
			$sth->bind_param(++$cnt, $val, $H_BIND_ARGS{$bind_arg});
		} else {
			$sth->bind_param(++$cnt, $bind_val);
		}
	}
	my $rows_affected;
	eval {$rows_affected = $sth->execute};
	if ($@) {
		$self->sql_error($@);
	}
	return $rows_affected;
}

sub Update2 {
#### Update with structure similar to InsertRow
	my ($self, $table, $data, $where, @where_bind_val) = @_;
	my %H_BIND_ARGS = (
		BLOB => SQL_BLOB,
		# NCLOB => { ora_csform => SQLCS_NCHAR, ora_type => ORA_CLOB }
		INT => SQL_INTEGER
	);
	my @setArr = ();
	my @bindArr = ();
#	while (my ($col, $val) = each %$row) {
	for (my $i = 0; $i < scalar(@$data); $i += 2) {
		my ($col, $val) = ($data->[$i], $data->[$i + 1]);
		my ($colName, $len, $colType) = split /\//, $col;
		$colName = '"' . uc($colName) . '"';
		if ($colType and $colType eq "FUNCTION") {
			push @setArr, "$colName=$val";
			## Nothing to put in @bindArr
		} else {
			push @setArr, "$colName=?";
			push @bindArr, [ $len ? &MySubstr($val, $len) : $val, $colType || undef ];
		}
	}
	push @bindArr, @where_bind_val if @where_bind_val;
	my $cmd = sprintf('UPDATE %s SET %s %s', $table, join(",", @setArr), $where); 
	my $sth;
	eval {$sth = $self->{h}->prepare($cmd)};
	if ($@) {
		$self->sql_error($@);
	}
	my $cnt = 0;
	foreach my $bindVal (@bindArr) {
		if (! ref $bindVal) {
			$sth->bind_param(++$cnt, $bindVal);
			next;
		}
		my ($val, $colType) = @$bindVal;
		if ($colType) {
			$sth->bind_param(++$cnt, $val, $H_BIND_ARGS{$colType});
		} else {
			$sth->bind_param(++$cnt, $val);
		}
	}
	eval {$sth->execute};
	if ($@) {
		$self->sql_error($@);  ## Log error and abort
	}
}

sub Do {
	my ($self, $cmd, @BindArr) = @_;
	my $rows_affected;
	eval {$rows_affected = $self->h->do($cmd, undef, @BindArr)};
	if ($@) {
		$self->sql_error($@);  ## Log error and abort
	}
	return $rows_affected;
}

sub sql_error {
	my ($self, $err_str) = @_;
	$self->h->rollback;
	$log->fatal("FATAL: SQL Error: $err_str");
	$self->h->rollback;
	for (my $i = 0; my @r = caller($i); $i++) { $log->info("$r[1]:$r[2] $r[3]"); }
	
	$self->app_abort;
}

sub app_abort {
	my $self = shift;
	$log->info("Application abnormal exit");
	sleep 60;
	exit 1;
}

# TODO: properly implement for sqlite3
sub timezone {
	my $self = shift;
	my $cmd = "select TO_CHAR(systimestamp,'TZH'), TO_CHAR(systimestamp,'TZM') FROM dual";
	my @tz_data = $self->lookup_array($cmd);
	my ($tz_hour, $tz_min) = @{$tz_data[0]};
	my $tz = {hour => $tz_hour, minute => $tz_min};
	$self->{timezone} = $tz;
	return $tz;
}

=comment
sub timezone {
	my $self = shift;
	return $self->{timezone} if exists $self->{timezone};
	my $cmd = "SELECT sessiontimezone FROM dual";
	my $tz_string = $self->lookup($cmd);
	my ($tz_hour, $tz_min) = split /:/, $tz_string;
	my $tz = {hour => $tz_hour, minute => $tz_min};
	$self->{timezone} = $tz;
	$self->{tz_string} = $tz_string;
	$log->info("Timezone is $tz_string");
	return $tz;
}
=cut

sub human_date {
	my ($self, $nls_date) = @_;
#### $nls_date is in NLS_DATE_FORMAT
	my $cmd = "select to_char(to_date(?), 'Dy, DD/Mon/YYYY HH24:MI:SS') from dual";
	my $human_date = $self->lookup($cmd, $nls_date);
	my $tz_string = join(":", $self->timezone->{hour}, $self->timezone->{minute});
	return "$human_date (UTC: " . $tz_string . ")"; 
}

# TODO: there is no dual table
sub epoch {
	my $self = shift;
	my $date = shift;
	my $cmd;
	if ($date) {
		$cmd = "SELECT (TO_DATE(?) - TO_DATE('1970-01-01', 'YYYY-MM-DD')) * 86400 FROM dual";
		return $self->lookup($cmd, $date);
	} else {
		$cmd = "SELECT (SYSDATE - TO_DATE('1970-01-01', 'YYYY-MM-DD')) * 86400 FROM dual";
		return $self->lookup($cmd);
	}
}

sub MySubstr {
	my ($str, $len) = @_;
	return undef unless defined($str);
	return substr($str, 0, $len);
}



1;
