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
	my $cmd="(java -jar /proj/ccjs-PG0/indexproject/hetu-server-1.6.0/bin/hetu-cli-1.6.0-executable.jar --server localhost:8080 --catalog hive --schema tpcds_orc_1000 -f ./$query)";
	my @warnoutput=`$cmd`;

	my $insertEnd = time();
	my $insertTime = $insertEnd - $insetStart ;
	print "insert Query : $query In $insertTime secs\n";
	print INSERT_LOG "$query : $insertTime\n";
}