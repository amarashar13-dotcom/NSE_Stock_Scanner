package com.amar.nsestockscanner

import android.Manifest
import android.annotation.SuppressLint
import android.app.AlertDialog
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.view.View
import android.webkit.JsResult
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.EditText
import android.widget.ProgressBar
import androidx.activity.ComponentActivity
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.content.ContextCompat

class MainActivity : ComponentActivity() {

    companion object {
        const val DEFAULT_URL = "https://nse-stock-scanner-dbbc.onrender.com"
        private const val PREFS = "nse"
        private const val KEY_URL = "url"
    }

    private lateinit var webView: WebView
    private lateinit var progressBar: ProgressBar
    private lateinit var notificationHelper: NotificationHelper

    private val permissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
            resolvePermission(granted)
        }

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        webView = findViewById(R.id.webView)
        progressBar = findViewById(R.id.progressBar)
        notificationHelper = NotificationHelper(this)
        notificationHelper.createChannel()

        val s = webView.settings
        s.javaScriptEnabled = true
        s.domStorageEnabled = true
        s.databaseEnabled = true
        s.loadWithOverviewMode = true
        s.useWideViewPort = true
        s.javaScriptCanOpenWindowsAutomatically = true
        s.mediaPlaybackRequiresUserGesture = false
        s.mixedContentMode = WebSettings.MIXED_CONTENT_NEVER_ALLOW
        val ua = s.userAgentString
        if (ua.contains("; wv")) s.userAgentString = ua.replace("; wv", "")

        webView.addJavascriptInterface(WebAppBridge(this, notificationHelper), "AndroidNotify")

        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean =
                handleUrl(view, request.url.toString())

            @Suppress("DEPRECATION")
            override fun shouldOverrideUrlLoading(view: WebView, url: String): Boolean =
                handleUrl(view, url)

            override fun onPageFinished(view: WebView, url: String) {
                super.onPageFinished(view, url)
                injectNotificationShim()
            }
        }

        webView.webChromeClient = object : WebChromeClient() {
            override fun onProgressChanged(view: WebView, newProgress: Int) {
                progressBar.progress = newProgress
                progressBar.visibility = if (newProgress >= 100) View.GONE else View.VISIBLE
            }

            override fun onJsAlert(view: WebView?, url: String?, message: String?, result: JsResult?): Boolean =
                super.onJsAlert(view, url, message, result)
        }

        findViewById<View>(R.id.settingsBtn).setOnClickListener { showUrlDialog() }
        webView.loadUrl(loadUrl())
    }

    private fun handleUrl(view: WebView, url: String): Boolean {
        if (!url.startsWith("http://") && !url.startsWith("https://")) return false
        val currentHost = Uri.parse(view.url ?: DEFAULT_URL).host
        val targetHost = Uri.parse(url).host
        if (currentHost != null && targetHost != null && currentHost == targetHost) return false
        return try {
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
            true
        } catch (_: Exception) {
            true
        }
    }

    private fun loadUrl(): String =
        getSharedPreferences(PREFS, Context.MODE_PRIVATE).getString(KEY_URL, DEFAULT_URL) ?: DEFAULT_URL

    private fun showUrlDialog() {
        val input = EditText(this).apply {
            setText(loadUrl())
            setSingleLine(true)
            hint = "https://..."
        }
        AlertDialog.Builder(this)
            .setTitle("App URL")
            .setMessage("Backend must be reachable over HTTPS from the phone.")
            .setView(input)
            .setPositiveButton("Load") { _, _ ->
                val u = input.text.toString().trim()
                if (u.isNotEmpty()) {
                    getSharedPreferences(PREFS, Context.MODE_PRIVATE)
                        .edit().putString(KEY_URL, u).apply()
                    webView.loadUrl(if (u.startsWith("http://") || u.startsWith("https://")) u else "https://" + u)
                }
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    @Deprecated("Deprecated in Java")
    override fun onBackPressed() {
        if (webView.canGoBack()) webView.goBack() else super.onBackPressed()
    }

    fun requestAndroidPermission() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            resolvePermission(true)
            return
        }
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) ==
            PackageManager.PERMISSION_GRANTED
        ) {
            resolvePermission(true)
            return
        }
        permissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
    }

    private fun resolvePermission(granted: Boolean) {
        val state = if (granted) "granted" else "denied"
        runOnUiThread {
            try {
                webView.evaluateJavascript("window.__nseResolvePerm('$state')", null)
            } catch (_: Exception) {
            }
        }
    }

    private fun injectNotificationShim() {
        val shim = """(function(){
  if (window.__nseShim) return;
  window.__nseShim = true;
  window.__nsePermCbs = [];
  window.__nseResolvePerm = function(perm){
    var cbs = window.__nsePermCbs || [];
    window.__nsePermCbs = [];
    cbs.forEach(function(cb){ try { cb(perm); } catch(e){} });
  };
  function AndroidNotif(title, opts){
    opts = opts || {};
    try { AndroidNotify.post(String(title||''), String(opts.body||''), String(opts.tag||'')); } catch(e){}
  }
  Object.defineProperty(window, 'Notification', { value: AndroidNotif, configurable: true, writable: true });
  window.Notification.requestPermission = function(cb){
    var resolve;
    var p = new Promise(function(res){ resolve = res; });
    (window.__nsePermCbs = window.__nsePermCbs || []).push(function(perm){
      if (typeof cb === 'function') { try { cb(perm); } catch(e){} }
      resolve(perm);
    });
    try { AndroidNotify.requestPermission(); } catch(e){ resolve('default'); }
    return p;
  };
  Object.defineProperty(window.Notification, 'permission', {
    get: function(){ try { return AndroidNotify.permission(); } catch(e){ return 'default'; } },
    configurable: true
  });
})();"""
        try {
            webView.evaluateJavascript(shim, null)
        } catch (_: Exception) {
        }
    }
}
