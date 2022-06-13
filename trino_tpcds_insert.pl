#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Path;
use List::Util;

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

open(INSERT_LOG, ">insert.log") or die "can not open insert.log";

chdir $SCRIPT_PATH;
chdir 'tpcds_insert_sql';
my @queries = glob '*.sql';
for my $query ( @queries ) {

	print "inset query : $query\n";
	my $insetStart = time();
	my $cmd="(/home/ec2-user/bigdata/trino-server-370/bin/trino --server localhost:8080 --catalog hive --schema tpcds_parquet_100 -f ./$query)";
	my @warnoutput=`$cmd`;

	my $insertEnd = time();
	my $insertTime = $insertEnd - $insetStart ;
	print "insert Query : $query In $insertTime secs\n";
	print INSERT_LOG "$query : $insertTime\n";
}