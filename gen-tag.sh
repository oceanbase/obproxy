#!/bin/bash
version="4.2.3.0"
if [ "$1" = "odp-test" ]
then
	echo `git log -1 --pretty=format:${version}-%h`
else
	echo $version
fi
