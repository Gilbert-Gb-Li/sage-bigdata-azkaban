#!/bin/sh

mysql -h10.201.10.22 -P3306 -uyaojianju -p3gxAFpyRTC -Dyaojianju --local-infile=1 -e 'call yaojianju.syncAll();'