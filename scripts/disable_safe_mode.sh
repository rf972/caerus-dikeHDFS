#!/usr/bin/env bash

bin/hadoop dfsadmin -safemode leave
echo "Done disabling safe mode"
