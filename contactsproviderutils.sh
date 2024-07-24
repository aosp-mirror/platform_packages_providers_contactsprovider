# Shell utility functions for contactsprovider developers.
# sudo apt-get install rlwrap to have a more fully featured sqlite CLI
# sudo apt-get install sqlitebrowser to explore the database with GUI

CONTACTS_DB="contacts2.db"

function contacts-pull () {
    adb root && adb wait-for-device
    dir=$(get-dir $1)
    package=$(get-package)

    if [ -f "$dir/$CONTACTS_DB" ]; then
      rm "$dir/$CONTACTS_DB"
    fi
    if [ -f "$dir/$CONTACTS_DB-wal" ]; then
      rm "$dir/$CONTACTS_DB-wal"
    fi
    adb pull /data/user/0/$package/databases/$CONTACTS_DB $dir/$CONTACTS_DB
    adb pull /data/user/0/$package/databases/$CONTACTS_DB-wal $dir/$CONTACTS_DB-wal
}

function get-dir (){
    if [ -z "$1" ]
    then
        dir=$(pwd)
    else
        dir=$1
    fi
    echo "$dir"
}

function sqlite3-pull () {
    dir="$(get-dir $1)"
    contacts-pull $dir
    rlwrap sqlite3 $dir/$CONTACTS_DB
}

function sqlitebrowser-pull () {
    dir="$(get-dir $1)"
    contacts-pull $dir
    sqlitebrowser $dir/$CONTACTS_DB
}

function sqlite3-push () {
    adb root && adb wait-for-device
    if [ -z "$1" ]
    then
        dir=$(pwd)
    else
        dir=$1
    fi
    package=$(get-package)

    adb push $dir/$CONTACTS_DB /data/user/0/$package/databases/$CONTACTS_DB
    adb push $dir/$CONTACTS_DB-wal /data/user/0/$package/databases/$CONTACTS_DB-wal

    sqlite3-trigger-upgrade
}

function sqlite3-trigger-upgrade () {
    package=$(get-package)

    # Doesn't actually upgrade the db because db version is hardcoded in code
    # It however triggers upgrade path
    check_string="/data/user/0/$package/databases/$CONTACTS_DB \"pragma user_version\""
    version=$(adb shell sqlite3 $check_string)
    echo "Old version: $version"

    version=$((version+1))
    upgrade_string="/data/user/0/$package/databases/$CONTACTS_DB \"pragma user_version=$version\""
    adb shell sqlite3 $upgrade_string

    version=$(adb shell sqlite3 $check_string)
    echo "New version: $version"

    adb shell am force-stop $package
}

function get-package() {
    echo "com.android.providers.contacts"
}

set +x  # disable debugging
