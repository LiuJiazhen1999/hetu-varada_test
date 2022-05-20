#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Path;
use List::Util;

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

open(TRINO_TPCH_QUERY_LOG, ">trino_tpch_query.log") or die "can not open trino_tpch_query.log";

chdir $SCRIPT_PATH;
chdir 'tpch_sql';
my @queries = glob '*.sql';
for my $query ( @queries ) {

	print "parquet query : $query start \n";
	my $queryStart = time();
	my $cmd="(/home/ec2-user/bigdata/trino-server-370/bin/trino --server localhost:8080 --catalog hive -f ./$query)";
	my @warnoutput=`$cmd`;

	my $queryEnd = time();
	my $queryTime = $queryEnd - $queryStart;
	print "parquet Query : $query In $queryTime secs\n";
	print TRINO_TPCH_QUERY_LOG "parquet $query : $queryTime\n";
}