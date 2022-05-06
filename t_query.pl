#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Path;
use List::Util;

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

open(VARADA_LOG, ">varada_tpcds.log") or die "can not open varada.log";
open(HIVE_LOG, ">hive_tpcds.log") or die "can not open hive.log";

print "****************************************parquet******************************************************";

print "***************************************Hive Warm**************************************************\n";
chdir $SCRIPT_PATH;
chdir 't1';
my @queries = glob '*.sql';
for my $query ( @queries ) {

	print "Warming Query : $query\n";
	my $warmStart = time();
	my $cmd="(/home/ec2-user/bigdata/trino/trino-server-370/bin/trino --server localhost:8080 --catalog hive -f ./$query)";
	my @warnoutput=`$cmd`;

	my $warmEnd = time();
	my $warmTime = $warmEnd - $warmStart ;
	print "Warmed Query : $query In $warmTime secs\n";
	print HIVE_LOG "$query,0 : $warmTime\n";

} # end for

close HIVE_LOG;

## warm
print "***************************************Varada Warm**************************************************\n";
chdir '../';
chdir 't2';
my @queries = glob '*.sql';
for my $query ( @queries ) {
	print "Warming Query : $query\n";
	my $warmStart = time();
	my $cmd="(/home/ec2-user/bigdata/trino-server-370/bin/trino --server localhost:8080 --catalog varada -f ./$query)";
	my @warnoutput=`$cmd`;

	my $warmEnd = time();
	my $warmTime = $warmEnd - $warmStart ;
	print "Warmed Query : $query In $warmTime secs\n";
	print VARADA_LOG "$query,0 : $warmTime\n";

    sleep 10;
} # end for

print "***************************************Varada Turn One**************************************************\n";
for my $query ( @queries ) {

	print "Turn one : $query\n";
	my $warmStart = time();
	my $cmd="(/home/ec2-user/bigdata/trino-server-370/bin/trino --server localhost:8080 --catalog varada -f ./$query)";
	my @warnoutput=`$cmd`;

	my $warmEnd = time();
	my $warmTime = $warmEnd - $warmStart ;
	print "Warmed Query : $query In $warmTime secs\n";
	print VARADA_LOG "$query,1 : $warmTime\n";

    sleep 10;
} # end for
#
print "***************************************Varada Turn Two**************************************************\n";
for my $query ( @queries ) {

	print "Warming Query : $query\n";
	my $warmStart = time();
	my $cmd="(/home/ec2-user/bigdata/trino-server-370/bin/trino --server localhost:8080 --catalog varada -f ./$query)";
	my @warnoutput=`$cmd`;

	my $warmEnd = time();
	my $warmTime = $warmEnd - $warmStart ;
	print "Warmed Query : $query In $warmTime secs\n";
	print VARADA_LOG "$query,2 : $warmTime\n";

    sleep 10;
} # end for

close VARADA_LOG;