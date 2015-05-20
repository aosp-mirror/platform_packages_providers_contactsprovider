#!/bin/bash
#
# Copyright (C) 2015 The Android Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

db=/data/data/com.android.providers.contacts/databases/contacts2.db

# Use ls to make sure the file already exists.
# Otherwise sqlite3 would create an empty file owned by root.

# Sed inserts a newline after each ( and ,
adb shell "(ls $db >/dev/null)&& sqlite3 $db \"select name, sql from sqlite_master where type in('table','index') order by name\"" |
    sed -e 's/\([(,]\)/\1\n  /g'
echo "> sqlite_stat1"
adb shell "(ls $db >/dev/null)&& sqlite3 $db \"select * from sqlite_stat1 order by tbl, idx, stat\""


