package com.amar.nsestockscanner

import android.webkit.JavascriptInterface

class WebAppBridge(
    private val activity: MainActivity,
    private val notifications: NotificationHelper
) {
    @JavascriptInterface
    fun post(title: String, body: String, tag: String) {
        notifications.post(title, body, tag)
    }

    @JavascriptInterface
    fun permission(): String = notifications.permission()

    @JavascriptInterface
    fun requestPermission() {
        activity.runOnUiThread { activity.requestAndroidPermission() }
    }
}
