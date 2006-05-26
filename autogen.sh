#!/bin/sh

svn log | ./gnuify-changelog.pl > ChangeLog
aclocal
automake
autoconf
./configure
