LOCAL_PATH:= $(call my-dir)
include $(CLEAR_VARS)

LOCAL_MODULE_TAGS := user

# Only compile source java files in this apk.
LOCAL_SRC_FILES := $(call all-java-files-under, src)

LOCAL_SRC_FILES += ../src/com/android/providers/contacts2/ContactsContract.java

LOCAL_PACKAGE_NAME := Contacts2

include $(BUILD_PACKAGE)
