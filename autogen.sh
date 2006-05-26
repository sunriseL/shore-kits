#!/bin/sh

svn log | ./gnuify-changelog.pl > ChangeLog
aclocal -I m4
automake
autoconf
