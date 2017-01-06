#!/bin/bash

hadoop fs -mkdir /commit/iteminfo/archivet_base_weibo_career
hadoop fs -mv /commit/iteminfo/20*/* /commit/iteminfo/archive/$today'_arc'/