#!/bin/bash
set -x
adb root && adb wait-for-device

adb shell setprop log.tag.ContactsProvider VERBOSE && adb shell killall android.process.acore

adb shell setprop log.tag.SQLiteSlowQueries VERBOSE
CONTACTS_UID=`adb shell pm list packages -U | sed -ne 's/^package:com\.android\.providers\.contacts  *uid://p'`
adb shell setprop db.log.slow_query_threshold.$CONTACTS_UID 0
adb shell setprop db.log.detailed 1 # to show more details in the "slow query" log

# adb shell setprop log.tag.SQLiteQueryBuilder VERBOSE # Redundant if slowlog
# enabled
adb shell setprop log.tag.SQLiteConnection VERBOSE


