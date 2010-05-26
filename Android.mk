LOCAL_PATH:= $(call my-dir)
include $(CLEAR_VARS)

LOCAL_MODULE_TAGS := optional

# Only compile source java files in this apk.
LOCAL_SRC_FILES := $(call all-java-files-under, src)
LOCAL_SRC_FILES += \
        src/com/android/providers/contacts/EventLogTags.logtags

LOCAL_JAVA_LIBRARIES := ext

LOCAL_STATIC_JAVA_LIBRARIES += android-common

LOCAL_PACKAGE_NAME := ContactsProvider
LOCAL_CERTIFICATE := shared

include $(BUILD_PACKAGE)

# Use the following include to make our test apk.
include $(call all-makefiles-under,$(LOCAL_PATH))
