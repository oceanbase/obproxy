#!/usr/bin/env perl
# Copyright (c) 2021 OceanBase
# SPDX-License-Identifier: Apache-2.0

use strict;
use warnings;
use Data::Dumper;
my $error_count=0;
my %map1;
open my $fh, '<', "ob_errno.def";
while(<$fh>)
{
    my $error_name;
    if (/^DEFINE_ERROR\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^)]*)/) {
	++$error_count;
	$error_name = $1;
    } elsif (/^DEFINE_ERROR_EXT\(([^,]+),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*("[^"]*")/) {
	++$error_count;
	$error_name = $1;
    }

    if (defined $error_name) {
	my $count = `find .. \\( \\( -name "*.cpp" -o -name "*.h" \\) -a ! -name "*ob_errno.*" \\) -exec grep $error_name {} + |wc -l`;
	chomp $count;
	$map1{$1} = $count;
    }
}

print "total error code: $error_count\n";
my @pairs = map {[$_, $map1{$_}]} keys %map1;
my @sorted = sort {$a->[1] <=> $b->[1]} @pairs;
#print Dumper(@sorted);
print "$_->[0] $_->[1]\n" foreach @sorted;
