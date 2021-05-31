#!/usr/bin/env perl
# Copyright (c) 2021 OceanBase
# OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
# You can use this software according to the terms and conditions of the Mulan PubL v2.
# You may obtain a copy of Mulan PubL v2 at:
#          http://license.coscl.org.cn/MulanPubL-2.0
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PubL v2 for more details.

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
