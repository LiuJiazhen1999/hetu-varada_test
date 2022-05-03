#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Path;
use List::Util;

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

open(HETU_PARQUET_QUERY_LOG, ">hetu_parquet_query.log") or die "can not open hetu_parquet_query.log";
open(HETU_ORC_QUERY_LOG, ">hetu_orc_query.log") or die "can not open hetu_orc_query.log";

chdir $SCRIPT_PATH;
chdir 'tpcds_parquet_sql';
my @queries = glob '*.sql';
for my $query ( @queries ) {

	print "parquet query : $query start \n";
	my $queryStart = time();
	my $cmd="(java -jar /home/ec2-user/bigdata/hetu-server-1.6.0/bin/hetu-cli-1.6.0-executable.jar --server localhost:8080 --catalog hive -f ./$query)";
	my @warnoutput=`$cmd`;

	my $queryEnd = time();
	my $queryTime = $queryEnd - $queryStart;
	print "parquet Query : $query In $queryTime secs\n";
	print HETU_PARQUET_QUERY_LOG "parquet $query : $queryTime\n";
}

chdir $SCRIPT_PATH;
chdir 'tpcds_orc_sql';
for my $query ( @queries ) {

	print "orc query : $query start \n";
	my $queryStart = time();
	my $cmd="(java -jar /home/ec2-user/bigdata/hetu-server-1.6.0/bin/hetu-cli-1.6.0-executable.jar --server localhost:8080 --catalog hive -f ./$query)";
	my @warnoutput=`$cmd`;

	my $queryEnd = time();
	my $queryTime = $queryEnd - $queryStart;
	print "orc Query : $query In $queryTime secs\n";
	print HETU_ORC_QUERY_LOG "orc $query : $queryTime\n";
}