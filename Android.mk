LOCAL_PATH:= $(call my-dir)
include $(CLEAR_VARS)

LOCAL_MODULE_TAGS := user

LOCAL_SRC_FILES := $(call all-subdir-java-files)

LOCAL_JAVA_LIBRARIES := ext

LOCAL_PACKAGE_NAME := ContactsProvider
LOCAL_CERTIFICATE := shared

include $(BUILD_PACKAGE)
