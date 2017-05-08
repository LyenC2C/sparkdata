#!/usr/bin/env bash
zuotian=$(date -d '0 days ago' +%Y%m%d)

wget ftp://ftp.blhdk.wolongdata.com/$zuotian*.csv --ftp-user=qx --ftp-password=NzS7d2oWe47LPVXx --no-passive-ftp
hfs -put ./$zuotian*.csv /user/wrt/qianxing/