#!/usr/bin/env bash



hive -e ' desc table ' 1 >log

select *
from