#!/bin/bash
#
# Copyright (C) 2016 The Android Open Source Project
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

cd $ANDROID_BUILD_TOP

. build/envsetup.sh

mmm -j32 packages/providers/ContactsProvider
adb install -t -r -g $ANDROID_PRODUCT_OUT/system/priv-app/ContactsProvider/ContactsProvider.apk
adb install -t -r -g $ANDROID_PRODUCT_OUT/data/app/ContactsProviderTests/ContactsProviderTests.apk
adb install -t -r -g $ANDROID_PRODUCT_OUT/data/app/ContactsProviderTests2/ContactsProviderTests2.apk

runtest() {
    log=/tmp/$$.log
    adb shell am instrument -w "${@}" |& tee $log
    if grep -q FAILURES $log || ! grep -P -q 'OK \([1-9]' $log ; then
        return 1
    else
        return 0
    fi

}

runtest com.android.providers.contacts.tests
runtest com.android.providers.contacts.tests2