package com.qgb.clientmqtt

import android.Manifest
import android.content.ClipData
import android.content.ClipboardManager as AndroidClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.BitmapFactory
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.provider.Settings
import android.util.Base64
import android.net.Uri
import java.io.File
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.clickable
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.pager.HorizontalPager
import androidx.compose.foundation.pager.rememberPagerState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.BugReport
import androidx.compose.material.icons.outlined.CameraAlt
import androidx.compose.material.icons.outlined.Extension
import androidx.compose.material.icons.outlined.Folder
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.Menu
import androidx.compose.material.icons.outlined.NetworkWifi
import androidx.compose.material.icons.outlined.Refresh
import androidx.compose.material.icons.outlined.Settings
import androidx.compose.material.icons.outlined.Terminal
import androidx.compose.material.icons.outlined.Tune
import androidx.compose.material.icons.twotone.Security
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.ModalDrawerSheet
import androidx.compose.material3.ModalNavigationDrawer
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.Switch
import androidx.compose.material3.rememberDrawerState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.snapshotFlow
import androidx.compose.runtime.setValue
import androidx.compose.ui.graphics.ImageBitmap
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat
import com.chaquo.python.Python
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.json.JSONObject

private data class RemoteFile(
    val path: String,
    val kind: String,
    val size: Long,
    val modified: Double
)

private data class FeatureDescriptor(
    val name: String,
    val title: String,
    val actions: List<String>,
    val icon: String? = null,
    val moduleFile: String = "",
    val source: String = ""
)

private fun featureIcon(feature: FeatureDescriptor) = when (feature.icon?.lowercase()) {
    "folder", "files", "file", "directory" -> Icons.Outlined.Folder
    "camera", "photo", "image" -> Icons.Outlined.CameraAlt
    "wifi", "network", "wireless" -> Icons.Outlined.NetworkWifi
    "terminal", "shell", "console" -> Icons.Outlined.Terminal
    "bug", "debug" -> Icons.Outlined.BugReport
    "info", "about" -> Icons.Outlined.Info
    "settings", "config", "tune" -> Icons.Outlined.Settings
    else -> when (feature.name) {
        "files" -> Icons.Outlined.Folder
        "camera" -> Icons.Outlined.CameraAlt
        "wifi" -> Icons.Outlined.NetworkWifi
        else -> Icons.Outlined.Extension
    }
}

private data class TargetDescriptor(
    val id: String,
    val name: String,
    val requestTopic: String,
    val remoteRoot: String
)

@OptIn(ExperimentalFoundationApi::class, ExperimentalMaterial3Api::class)
class MainActivity : ComponentActivity() {
    private val cameraPermission = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            cameraPermission.launch(Manifest.permission.CAMERA)
        }
        setContent {
            ClientMqttScreen()
        }
    }
}

@OptIn(ExperimentalFoundationApi::class, ExperimentalMaterial3Api::class)
@Composable
private fun ClientMqttScreen() {
    var features by remember { mutableStateOf(listOf<FeatureDescriptor>()) }
    var targets by remember { mutableStateOf(listOf<TargetDescriptor>()) }
    val pagerState = rememberPagerState(pageCount = { features.size })
    val pagerScope = rememberCoroutineScope()
    val drawerState = rememberDrawerState(initialValue = androidx.compose.material3.DrawerValue.Closed)
    var settings by remember { mutableStateOf(false) }
    var targetSettings by remember { mutableStateOf(false) }
    var diagnostics by remember { mutableStateOf(false) }
    var editingDeviceId by remember { mutableStateOf<String?>(null) }
    var permissions by remember { mutableStateOf(false) }
    var reloadTarget by remember { mutableStateOf<FeatureDescriptor?>(null) }
    var selectedDevice by remember { mutableStateOf("Target") }
    var selectedDeviceId by remember { mutableStateOf("") }
    var selectedTopic by remember { mutableStateOf("sys/device/request") }
    var targetRemoteRoot by remember { mutableStateOf("/data/data") }
    var onlineStatus by remember { mutableStateOf("checking") }
    var onlineProbeEnabled by remember { mutableStateOf(true) }
    var onlineProbeInterval by remember { mutableStateOf(30) }
    val snackbarHostState = remember { SnackbarHostState() }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()

    fun parseTargets(raw: String) = buildList {
        val array = org.json.JSONArray(raw)
        for (index in 0 until array.length()) {
            val item = array.optJSONObject(index) ?: continue
            val topic = item.optString("request_topic")
            if (topic.isNotBlank()) add(TargetDescriptor(
                item.optString("id", topic),
                item.optString("name", topic),
                topic,
                item.optString("remote_root", "/data/data")
            ))
        }
    }

    suspend fun refreshTargets() {
        val raw = withContext(Dispatchers.IO) { service.callAttr("device_catalog").toString() }
        targets = parseTargets(raw)
    }

    suspend fun refreshFeatures() {
        val raw = withContext(Dispatchers.IO) { service.callAttr("feature_catalog").toString() }
        val array = org.json.JSONArray(raw)
        features = buildList {
            for (index in 0 until array.length()) {
                val item = array.optJSONObject(index) ?: continue
                add(FeatureDescriptor(
                    name = item.optString("name"),
                    title = item.optString("title", item.optString("name")),
                    actions = buildList {
                        val actions = item.optJSONArray("actions") ?: org.json.JSONArray()
                        for (actionIndex in 0 until actions.length()) add(actions.optString(actionIndex))
                    },
                    icon = if (item.isNull("icon")) null else item.optString("icon").ifBlank { null },
                    moduleFile = item.optString("module_file"),
                    source = item.optString("source"),
                ))
            }
        }
    }

    // 首次进入：以 Python 端持久化的 selected_device_id 为准恢复上次目标，
    // 不再每次重启都跳回列表第一个 topic。
    LaunchedEffect(Unit) {
        runCatching {
            val activeConfig = withContext(Dispatchers.IO) {
                JSONObject(service.callAttr("device_settings").toString())
            }
            targets = parseTargets(withContext(Dispatchers.IO) {
                service.callAttr("device_catalog").toString()
            })
            val activeId = activeConfig.optString("id")
            val activeTopic = activeConfig.optString("request_topic")
            val target = targets.firstOrNull { it.id == activeId }
                ?: targets.firstOrNull { it.requestTopic == activeTopic }
            if (target != null) {
                selectedDeviceId = target.id
                selectedTopic = target.requestTopic
                selectedDevice = target.name
                targetRemoteRoot = target.remoteRoot
                if (target.id != activeId) {
                    withContext(Dispatchers.IO) { service.callAttr("select_device", target.id) }
                }
            }
        }
        while (true) {
            runCatching { refreshFeatures() }
            delay(3_000)
        }
    }

    LaunchedEffect(Unit) {
        while (true) {
            runCatching {
                val raw = withContext(Dispatchers.IO) { service.callAttr("device_catalog").toString() }
                val updated = parseTargets(raw)
                targets = updated
                // 保持用户当前选择；选中项消失时不擅自跳到第一个目标。
                val active = updated.firstOrNull { it.id == selectedDeviceId }
                    ?: updated.firstOrNull { selectedTopic.isNotBlank() && it.requestTopic == selectedTopic }
                if (active != null) {
                    if (active.id != selectedDeviceId) {
                        withContext(Dispatchers.IO) { service.callAttr("select_device", active.id) }
                    }
                    selectedDeviceId = active.id
                    selectedTopic = active.requestTopic
                    selectedDevice = active.name
                    targetRemoteRoot = active.remoteRoot
                }
            }
            delay(1_000)
        }
    }

    LaunchedEffect(Unit) {
        while (true) {
            runCatching {
                val config = withContext(Dispatchers.IO) {
                    JSONObject(service.callAttr("general_settings").toString())
                }
                onlineProbeEnabled = config.optBoolean("online_probe_enabled", true)
                onlineProbeInterval = config.optInt("online_probe_interval", 30).coerceIn(5, 3600)
            }
            delay(1_000)
        }
    }

    LaunchedEffect(selectedTopic) {
        targetRemoteRoot = withContext(Dispatchers.IO) {
            runCatching {
                JSONObject(service.callAttr("device_settings", selectedTopic).toString())
                    .optString("remote_root", "/data/data")
            }.getOrDefault("/data/data")
        }
    }

    LaunchedEffect(selectedDeviceId, onlineProbeEnabled, onlineProbeInterval) {
        while (true) {
            val health = withContext(Dispatchers.IO) {
                runCatching { JSONObject(service.callAttr("target_health", selectedDeviceId).toString()) }
                    .getOrElse { JSONObject() }
            }
            val lastProbeAt = health.optLong("last_probe_at_ms", 0)
            val inFlight = health.optInt("inflight_count", 0)
            val lastProbeOk = health.optBoolean("last_probe_ok", false)
            onlineStatus = when {
                inFlight > 0 -> "checking"
                lastProbeAt == 0L && !onlineProbeEnabled -> "probe disabled"
                lastProbeAt == 0L -> "checking"
                lastProbeOk && !onlineProbeEnabled -> "online · probe disabled"
                !lastProbeOk && !onlineProbeEnabled -> "offline · probe disabled"
                lastProbeOk -> "online"
                else -> "offline"
            }

            val probeDue = lastProbeAt == 0L || System.currentTimeMillis() - lastProbeAt >= onlineProbeInterval * 1_000L
            if (onlineProbeEnabled && inFlight == 0 && probeDue && selectedDeviceId.isNotBlank()) {
                withContext(Dispatchers.IO) {
                    runCatching { service.callAttr("online", selectedDeviceId) }
                }
            }
            delay(1_000)
        }
    }

    LaunchedEffect(features.size) {
        if (features.isNotEmpty() && pagerState.currentPage >= features.size) {
            pagerState.animateScrollToPage(features.lastIndex)
        }
    }

    BackHandler(enabled = permissions || targetSettings || settings || diagnostics) {
        when {
            permissions -> permissions = false
            targetSettings -> targetSettings = false
            settings -> settings = false
            diagnostics -> diagnostics = false
        }
    }

    ModalNavigationDrawer(
        drawerState = drawerState,
        gesturesEnabled = !settings && !targetSettings && !permissions,
        drawerContent = {
            ModalDrawerSheet {
                Row(modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp)) {
                    Text("Scripts", style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
                    Text("Long-press to reload", style = MaterialTheme.typography.labelSmall)
                }
                features.forEachIndexed { index, feature ->
                    NavigationDrawerItem(
                        label = {
                            Text(
                                feature.title,
                                modifier = Modifier.combinedClickable(
                                    onClick = {},
                                    onLongClick = { reloadTarget = feature }
                                )
                            )
                        },
                        selected = pagerState.currentPage == index && !settings && !targetSettings,
                        onClick = {
                            pagerScope.launch {
                                pagerState.animateScrollToPage(index)
                                drawerState.close()
                            }
                        },
                        icon = {
                            Box(
                                modifier = Modifier.combinedClickable(
                                    onClick = {},
                                    onLongClick = { reloadTarget = feature }
                                )
                            ) {
                                Icon(featureIcon(feature), contentDescription = feature.title)
                            }
                        },
                        modifier = Modifier.padding(horizontal = 12.dp)
                    )
                }
                HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                Text("Targets", style = MaterialTheme.typography.titleMedium, modifier = Modifier.padding(16.dp))
                targets.forEach { target ->
                    NavigationDrawerItem(
                        label = { Text(target.requestTopic) },
                        selected = selectedTopic == target.requestTopic,
                        onClick = {
                            service.callAttr("select_device", target.id)
                            selectedDeviceId = target.id
                            selectedTopic = target.requestTopic
                            selectedDevice = target.name
                            targetRemoteRoot = target.remoteRoot
                            scope.launch { drawerState.close() }
                        },
                        modifier = Modifier.padding(horizontal = 12.dp)
                    )
                }
                NavigationDrawerItem(
                    label = { Text("Add target") },
                    selected = false,
                    onClick = {
                        editingDeviceId = null
                        targetSettings = true
                        scope.launch { drawerState.close() }
                    },
                    modifier = Modifier.padding(horizontal = 12.dp)
                )
            }
        }
    ) {
        Scaffold(
            topBar = {
                TopAppBar(
                    title = {
                        Column {
                            Text(selectedDevice)
                            Text(onlineStatus, style = MaterialTheme.typography.labelSmall)
                        }
                    },
                    actions = {
                        if (!settings && !targetSettings && !permissions) {
                            TextButton(onClick = { diagnostics = true }) {
                                Text("RPC logs")
                            }
                            IconButton(onClick = { scope.launch { drawerState.open() } }) {
                                Icon(Icons.Outlined.Menu, contentDescription = "Scripts and targets")
                            }
                            IconButton(onClick = {
                                editingDeviceId = selectedDeviceId
                                targetSettings = true
                            }) {
                                Icon(Icons.Outlined.Tune, contentDescription = "Target settings")
                            }
                        }
                        if (!settings && !targetSettings && !permissions) {
                            IconButton(onClick = { settings = true }) {
                                Icon(Icons.Outlined.Settings, contentDescription = "App settings")
                            }
                        }
                    }
                )
            },
            snackbarHost = { SnackbarHost(snackbarHostState) },
            bottomBar = {
                if (!settings && !targetSettings && !permissions && !diagnostics && features.isNotEmpty()) {
                    val selectedPage = pagerState.currentPage.coerceIn(0, features.lastIndex)
                    NavigationBar {
                        features.forEachIndexed { index, feature ->
                            NavigationBarItem(
                                selected = selectedPage == index,
                                onClick = { pagerScope.launch { pagerState.animateScrollToPage(index) } },
                                icon = {
                                    Box(
                                        modifier = Modifier.combinedClickable(
                                            onClick = {},
                                            onLongClick = { reloadTarget = feature }
                                        )
                                    ) {
                                        Icon(featureIcon(feature), contentDescription = feature.title)
                                    }
                                },
                                label = { Text(feature.title, maxLines = 1, style = MaterialTheme.typography.labelSmall) }
                            )
                        }
                    }
                }
            }
        ) { padding ->
            if (diagnostics) {
                DiagnosticsPage(
                    modifier = Modifier.padding(padding),
                    selectedTopic = selectedTopic,
                    onlineStatus = onlineStatus,
                    onBack = { diagnostics = false }
                )
            } else if (permissions) {
                PermissionPage(onBack = { permissions = false })
            } else if (targetSettings) {
                TargetSettingsPage(
                    modifier = Modifier.padding(padding),
                    existingDeviceId = editingDeviceId,
                    onBack = { targetSettings = false },
                    onTargetCreated = { id ->
                        selectedDeviceId = id
                    }
                )
            } else if (settings) {
                SettingsPage(
                    modifier = Modifier.padding(padding),
                    onBack = { settings = false },
                    onPermissions = { permissions = true }
                )
            } else {
                Column(modifier = Modifier.fillMaxSize().padding(padding)) {
                    if (features.isEmpty()) {
                        Text("No feature scripts found", modifier = Modifier.padding(16.dp))
                    } else {
                        HorizontalPager(
                            state = pagerState,
                            modifier = Modifier.fillMaxSize().weight(1f)
                        ) { page ->
                            when (features[page].name) {
                                "files" -> FilesPage(targetRemoteRoot)
                                "camera" -> CameraPage()
                                "wifi" -> WifiPage()
                                else -> DynamicFeaturePage(features[page])
                            }
                        }
                    }
                }
            }
        }
    }

    reloadTarget?.let { feature ->
        AlertDialog(
            onDismissRequest = { reloadTarget = null },
            icon = { Icon(Icons.Outlined.Refresh, contentDescription = null) },
            title = { Text("Reload feature module") },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Text("${feature.title} (feature_${feature.name}.py)")
                    Text(
                        "Loaded from: " + (feature.moduleFile.ifBlank { "not loaded yet" }),
                        style = MaterialTheme.typography.bodySmall
                    )
                    Text(
                        "Drops sys.modules cache and pyc, then re-imports from py_updates.",
                        style = MaterialTheme.typography.bodySmall
                    )
                }
            },
            confirmButton = {
                TextButton(onClick = {
                    val target = feature
                    reloadTarget = null
                    scope.launch {
                        val message = runCatching {
                            val raw = withContext(Dispatchers.IO) {
                                service.callAttr("reload_feature", target.name).toString()
                            }
                            refreshFeatures()
                            val result = JSONObject(raw)
                            if (result.optBoolean("ok")) {
                                "Reloaded ${target.title}"
                            } else {
                                "Reload failed: ${result.optString("error", "unknown error")}"
                            }
                        }.getOrElse { "Reload failed: ${it.message}" }
                        snackbarHostState.showSnackbar(message)
                    }
                }) { Text("Reload") }
            },
            dismissButton = {
                TextButton(onClick = { reloadTarget = null }) { Text("Cancel") }
            }
        )
    }
}

@Composable
private fun FilesPage(initialRoot: String) {
    var root by remember(initialRoot) { mutableStateOf(initialRoot) }
    var limit by remember { mutableStateOf("100") }
    var status by remember { mutableStateOf("Ready") }
    var entries by remember { mutableStateOf(listOf<RemoteFile>()) }
    var nextOffset by remember { mutableStateOf(0) }
    var hasMore by remember { mutableStateOf(false) }
    var loading by remember { mutableStateOf(false) }
    var preview by remember { mutableStateOf<ImageBitmap?>(null) }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()
    val listState = rememberLazyListState()
    val rootBoundary = initialRoot.trimEnd('/').ifEmpty { "/" }

    fun loadPage(start: Int, scanRoot: String = root) {
        if (loading) return
        loading = true
        status = "Scanning $scanRoot at $start..."
        val pageSize = limit.toIntOrNull()?.coerceIn(1, 10000) ?: 100
        scope.launch {
            val raw = withContext(Dispatchers.IO) {
                service.callAttr("call_feature", "files", "scan", scanRoot, start, pageSize).toString()
            }
            try {
                val result = JSONObject(raw)
                if (!result.optBoolean("ok", true)) {
                    if (start == 0) entries = emptyList()
                    hasMore = false
                    status = result.optString("error", "Feature action failed")
                    return@launch
                }
                val page = result.optJSONArray("items") ?: org.json.JSONArray()
                val newEntries = buildList {
                    for (index in 0 until page.length()) {
                        val item = page.optJSONObject(index) ?: continue
                        add(RemoteFile(
                            item.optString("path"),
                            item.optString("kind", "file"),
                            item.optLong("size"),
                            item.optDouble("modified")
                        ))
                    }
                }
                entries = if (start == 0) newEntries else entries + newEntries
                hasMore = result.optBoolean("has_more")
                nextOffset = result.optInt("next_offset", start + newEntries.size)
                status = "Loaded ${entries.size} files${if (hasMore) ", more below" else ""}"
            } catch (error: Exception) {
                status = "Invalid scan response: ${error.message}"
            } finally {
                loading = false
            }
        }
    }

    LaunchedEffect(listState, hasMore, loading, entries.size) {
        snapshotFlow { listState.layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: -1 }
            .collect { last ->
                if (hasMore && !loading && entries.isNotEmpty() && last >= entries.lastIndex) {
                    loadPage(nextOffset)
                }
            }
    }
    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
            Button(enabled = root != rootBoundary, onClick = {
                val parent = root.trimEnd('/').substringBeforeLast('/', rootBoundary).ifEmpty { "/" }
                val parentRoot = if (parent.length < rootBoundary.length || !parent.startsWith(rootBoundary)) {
                    rootBoundary
                } else {
                    parent
                }
                root = parentRoot
                entries = emptyList()
                nextOffset = 0
                hasMore = false
                scope.launch { listState.scrollToItem(0) }
                loadPage(0, parentRoot)
            }) { Text("Up") }
            Text(root, modifier = Modifier.weight(1f).align(androidx.compose.ui.Alignment.CenterVertically), maxLines = 2)
            Button(onClick = {
                entries = emptyList()
                nextOffset = 0
                hasMore = false
                loadPage(0, root)
            }) { Text("Refresh") }
        }
        OutlinedTextField(
            limit,
            { limit = it.filter(Char::isDigit) },
            Modifier.fillMaxWidth(),
            singleLine = true,
            label = { Text("Page size") }
        )
        Text(status, style = MaterialTheme.typography.labelMedium)
        LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(4.dp)) {
            items(entries) { entry ->
                Row(
                    Modifier.fillMaxWidth().clickable {
                        if (entry.kind == "directory") {
                            val childRoot = root.trimEnd('/') + "/" + entry.path
                            root = childRoot
                            entries = emptyList()
                            nextOffset = 0
                            hasMore = false
                            status = "Opening $childRoot..."
                            scope.launch { listState.scrollToItem(0) }
                            loadPage(0, childRoot)
                        } else {
                            val currentRoot = root
                            status = "Downloading ${entry.path}..."
                            scope.launch {
                                try {
                                    val transfer = withContext(Dispatchers.IO) {
                                        service.callAttr("call_feature", "files", "upload", currentRoot.trimEnd('/') + "/" + entry.path).toString()
                                    }
                                    val transferJson = JSONObject(transfer)
                                    if (!transferJson.optBoolean("ok", true)) error(transferJson.optString("error", "upload failed"))
                                    val url = transferJson.optString("url")
                                    if (url.isBlank()) error(transferJson.optString("error", "upload returned no URL"))
                                    val saved = withContext(Dispatchers.IO) {
                                        JSONObject(service.callAttr("download_remote_to_file", url, entry.path.substringAfterLast('/')).toString())
                                    }
                                    if (!saved.optBoolean("ok")) error(saved.optString("error", "download failed"))
                                    if (entry.path.lowercase().matches(Regex(".*\\.(jpg|jpeg|png|webp|gif)$"))) {
                                        val encoded = withContext(Dispatchers.IO) {
                                            service.callAttr("download_transfer_base64", url).toString()
                                        }
                                        val bytes = Base64.decode(encoded, Base64.DEFAULT)
                                        preview = BitmapFactory.decodeByteArray(bytes, 0, bytes.size)?.asImageBitmap()
                                    }
                                    status = "Saved to app script directory downloads/"
                                } catch (error: Exception) {
                                    status = "Download failed: ${error.message}"
                                }
                            }
                        }
                    }.padding(vertical = 8.dp),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Column(Modifier.weight(1f)) {
                        Text(entry.path, style = MaterialTheme.typography.bodyMedium)
                        Text(
                            if (entry.kind == "directory") "Folder" else "${entry.size} B · ${entry.modified}",
                            style = MaterialTheme.typography.bodySmall
                        )
                    }
                    Text(if (entry.kind == "directory") "Open" else "Download", style = MaterialTheme.typography.labelSmall)
                }
            }
            if (loading) item { Text("Loading...") }
        }
        preview?.let { bitmap ->
            Image(bitmap = bitmap, contentDescription = "Remote image", modifier = Modifier.fillMaxWidth())
        }
    }
}

@Composable
private fun CameraPage() {
    var facing by remember { mutableStateOf(0) }
    var cameraMenuExpanded by remember { mutableStateOf(false) }
    var status by remember { mutableStateOf("Ready") }
    var preview by remember { mutableStateOf<ImageBitmap?>(null) }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()
    Column(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Box {
                Button(onClick = { cameraMenuExpanded = true }) {
                    Text(if (facing == 0) "Back camera" else "Front camera")
                }
                DropdownMenu(
                    expanded = cameraMenuExpanded,
                    onDismissRequest = { cameraMenuExpanded = false }
                ) {
                    DropdownMenuItem(
                        text = { Text("Back camera") },
                        onClick = { facing = 0; cameraMenuExpanded = false }
                    )
                    DropdownMenuItem(
                        text = { Text("Front camera") },
                        onClick = { facing = 1; cameraMenuExpanded = false }
                    )
                }
            }
            Button(onClick = {
                status = "Capturing..."
                preview = null
                scope.launch {
                    try {
                        val raw = withContext(Dispatchers.IO) {
                            service.callAttr("call_feature", "camera", "capture", facing).toString()
                        }
                        val result = JSONObject(raw)
                        if (!result.optBoolean("ok")) {
                            status = raw
                            return@launch
                        }
                        val url = result.optString("url")
                        if (url.isNotBlank()) {
                            val encoded = withContext(Dispatchers.IO) {
                                service.callAttr("download_transfer_base64", url).toString()
                            }
                            val bytes = Base64.decode(encoded, Base64.DEFAULT)
                            preview = BitmapFactory.decodeByteArray(bytes, 0, bytes.size)?.asImageBitmap()
                        }
                        status = raw
                    } catch (error: Exception) {
                        status = "Capture failed: ${error.message}"
                    }
                }
            }) { Text("Capture") }
        }
        preview?.let { bitmap ->
            Image(bitmap = bitmap, contentDescription = "Captured photo", modifier = Modifier.fillMaxWidth())
        }
        if (status.trimStart().startsWith("{")) {
            FeatureOutput(status, modifier = Modifier.fillMaxWidth().weight(1f))
        } else {
            Text(status)
        }
        Text("JPEG stays in memory on the target and is transferred outside MQTT.", style = MaterialTheme.typography.bodySmall)
    }
}

@Composable
private fun WifiPage() {
    var status by remember { mutableStateOf("Waiting for target RPC") }
    var copyStatus by remember { mutableStateOf("") }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()
    val context = androidx.compose.ui.platform.LocalContext.current
    val clipboard = context.getSystemService(Context.CLIPBOARD_SERVICE) as AndroidClipboardManager
    Column(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(onClick = {
                status = "Querying..."
                scope.launch {
                    try {
                        val raw = withContext(Dispatchers.IO) {
                            service.callAttr("call_feature", "wifi", "info").toString()
                        }
                        status = runCatching { JSONObject(raw).toString(2) }.getOrDefault(raw)
                        copyStatus = ""
                    } catch (error: Exception) {
                        status = "Wi-Fi query failed: ${error.message}"
                    }
                }
            }) { Text("Refresh Wi-Fi") }
            TextButton(onClick = {
                clipboard.setPrimaryClip(ClipData.newPlainText("Wi-Fi result", status))
                copyStatus = "Copied"
            }) { Text("Copy") }
        }
        if (status.trimStart().startsWith("{")) {
            FeatureOutput(status, modifier = Modifier.fillMaxWidth().weight(1f))
        } else {
            SelectionContainer { Text(status, style = MaterialTheme.typography.bodyMedium) }
        }
        if (copyStatus.isNotBlank()) Text(copyStatus, style = MaterialTheme.typography.labelSmall)
    }
}

@Composable
private fun DiagnosticsPage(
    modifier: Modifier = Modifier,
    selectedTopic: String,
    onlineStatus: String,
    onBack: () -> Unit
) {
    var logs by remember { mutableStateOf(listOf<String>()) }
    var status by remember { mutableStateOf("") }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()
    val listState = rememberLazyListState()
    val context = androidx.compose.ui.platform.LocalContext.current
    val clipboard = context.getSystemService(Context.CLIPBOARD_SERVICE) as AndroidClipboardManager

    LaunchedEffect(Unit) {
        while (true) {
            runCatching {
                val raw = withContext(Dispatchers.IO) { service.callAttr("rpc_logs").toString() }
                val array = org.json.JSONArray(raw)
                val updated = buildList {
                    for (index in 0 until array.length()) add(array.optString(index))
                }
                if (updated != logs) logs = updated
            }.onFailure { status = "Unable to read RPC diagnostics: ${it.message}" }
            delay(500)
        }
    }

    LaunchedEffect(logs.size) {
        if (logs.isNotEmpty()) listState.animateScrollToItem(logs.lastIndex)
    }

    Column(modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Column {
                Text("RPC diagnostics", style = MaterialTheme.typography.titleLarge)
                Text("$selectedTopic · $onlineStatus", style = MaterialTheme.typography.bodySmall)
            }
            Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                TextButton(onClick = {
                    val report = "topic=$selectedTopic status=$onlineStatus\n" + logs.joinToString("\n")
                    clipboard.setPrimaryClip(ClipData.newPlainText("RPC diagnostics", report))
                    status = "RPC diagnostics copied"
                }) { Text("Copy") }
                TextButton(onClick = {
                    scope.launch {
                        runCatching {
                            withContext(Dispatchers.IO) { service.callAttr("clear_rpc_logs") }
                            logs = emptyList()
                            status = ""
                        }.onFailure { status = "Unable to clear logs: ${it.message}" }
                    }
                }) { Text("Clear") }
                TextButton(onClick = onBack) { Text("Done") }
            }
        }
        Text("Keys, Aliyun credentials, and RPC source code are excluded from this log.", style = MaterialTheme.typography.bodySmall)
        if (status.isNotBlank()) Text(status, style = MaterialTheme.typography.bodySmall)
        LazyColumn(
            state = listState,
            modifier = Modifier.fillMaxSize(),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            if (logs.isEmpty()) {
                item { Text("No RPC activity yet", style = MaterialTheme.typography.bodyMedium) }
            } else {
                items(logs) { entry ->
                    SelectionContainer {
                        Text(entry, style = MaterialTheme.typography.bodySmall)
                    }
                    HorizontalDivider()
                }
            }
        }
    }
}

@Composable
private fun FeatureOutput(raw: String, modifier: Modifier = Modifier) {
    // raw 是 Python 端 json.dumps 出来的原始字符串；这里只按字段取出
    // stdout/stderr/error 单独原样显示，不再用 Java 重排/美化 print 内容。
    val parsed = remember(raw) { runCatching { JSONObject(raw) }.getOrNull() }
    fun fieldText(key: String): String? = parsed
        ?.takeIf { it.has(key) && !it.isNull(key) }
        ?.optString(key)
        ?.takeIf { it.isNotEmpty() }
    val stdout = fieldText("_stdout")
    val stderr = fieldText("_stderr")
    val remoteError = fieldText("_remote_error") ?: fieldText("error")
    Column(
        modifier.verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        if (stdout != null) {
            Text("Python stdout", style = MaterialTheme.typography.labelLarge)
            SelectionContainer {
                Text(stdout, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
            }
        }
        if (stderr != null) {
            Text("Python stderr", style = MaterialTheme.typography.labelLarge)
            SelectionContainer {
                Text(stderr, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
            }
        }
        if (remoteError != null) {
            Text("Target error", style = MaterialTheme.typography.labelLarge)
            SelectionContainer {
                Text(remoteError, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
            }
        }
        Text("Raw response (Python)", style = MaterialTheme.typography.labelLarge)
        SelectionContainer {
            Text(raw, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
        }
    }
}

@Composable
private fun DynamicFeaturePage(feature: FeatureDescriptor) {
    var result by remember(feature.name) { mutableStateOf("Ready") }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()
    Column(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Text(feature.title, style = MaterialTheme.typography.headlineSmall)
        Text("feature_${feature.name}.py · actions=${feature.actions.joinToString()}", style = MaterialTheme.typography.bodySmall)
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            feature.actions.forEach { action ->
                Button(onClick = {
                    result = "Running $action..."
                    scope.launch {
                        result = withContext(Dispatchers.IO) {
                            runCatching { service.callAttr("call_feature", feature.name, action).toString() }
                                .getOrElse { "Error: ${it.message}" }
                        }
                    }
                }) { Text(action) }
            }
        }
        FeatureOutput(result, modifier = Modifier.fillMaxWidth().weight(1f))
    }
}

@Composable
private fun TargetSettingsPage(
    modifier: Modifier = Modifier,
    existingDeviceId: String?,
    onBack: () -> Unit,
    onTargetCreated: (String) -> Unit
) {
    var deviceId by remember(existingDeviceId) { mutableStateOf(existingDeviceId.orEmpty()) }
    var topic by remember(existingDeviceId) {
        mutableStateOf(if (existingDeviceId == null) "sys/device/request" else "")
    }
    var remoteRoot by remember(existingDeviceId) { mutableStateOf("/data/data") }
    var privateKey by remember(existingDeviceId) { mutableStateOf("") }
    var timeout by remember(existingDeviceId) { mutableStateOf("10") }
    var allowNoServerKey by remember(existingDeviceId) { mutableStateOf(true) }
    var normalizingPrivateKey by remember { mutableStateOf(false) }
    var privateKeyStatus by remember { mutableStateOf("") }
    var loaded by remember(existingDeviceId) { mutableStateOf(existingDeviceId == null) }
    var lastLocalEdit by remember(existingDeviceId) { mutableStateOf(0L) }
    var status by remember { mutableStateOf("") }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()

    LaunchedEffect(deviceId, existingDeviceId) {
        val targetId = deviceId.ifBlank { existingDeviceId ?: return@LaunchedEffect }
        while (true) {
            runCatching {
                val config = withContext(Dispatchers.IO) {
                    JSONObject(service.callAttr("device_settings", targetId).toString())
                }
                if (deviceId.isBlank()) deviceId = config.optString("id", targetId)
                if (System.currentTimeMillis() - lastLocalEdit >= 1_200L) {
                    topic = config.optString("request_topic", "")
                    remoteRoot = config.optString("remote_root", "/data/data")
                    privateKey = config.optString("private_key", "")
                    timeout = config.optString("timeout_draft", config.optString("timeout", "10"))
                    allowNoServerKey = config.optBoolean("allow_no_server_pubkey_response", true)
                    loaded = true
                }
            }.onFailure { status = "Unable to sync target settings: ${it.message}" }
            delay(1_000)
        }
    }

    LaunchedEffect(deviceId, topic, remoteRoot, privateKey, timeout, allowNoServerKey, loaded) {
        if (!loaded || topic.isBlank()) return@LaunchedEffect
        delay(300)
        val timeoutValue = timeout.toDoubleOrNull()?.takeIf { it > 0 }
        val values = JSONObject()
            .put("request_topic", topic.trim())
            .put("remote_root", remoteRoot)
            .put("private_key", privateKey)
            .put("allow_no_server_pubkey_response", allowNoServerKey)
        if (timeoutValue != null) values.put("timeout", timeoutValue)
        else values.put("timeout_draft", timeout)

        status = "Saving..."
        runCatching {
            val saved = withContext(Dispatchers.IO) {
                JSONObject(service.callAttr("update_device_settings", deviceId, values.toString()).toString())
            }
            val savedId = saved.optString("id")
            if (deviceId.isBlank() && savedId.isNotBlank()) {
                deviceId = savedId
                onTargetCreated(savedId)
            }
            status = when {
                timeoutValue == null -> "Saved; timeout must be positive"
                else -> "Saved to client_mqtt.json"
            }
        }.onFailure { status = "Save failed: ${it.message}" }
    }

    Column(
        modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Text(if (existingDeviceId == null) "Add target" else "Target settings", style = MaterialTheme.typography.headlineSmall)
            Button(onClick = onBack) { Text("Done") }
        }
        OutlinedTextField(
            topic,
            { topic = it; lastLocalEdit = System.currentTimeMillis() },
            Modifier.fillMaxWidth(),
            singleLine = true,
            label = { Text("Request topic") }
        )
        OutlinedTextField(
            remoteRoot,
            { remoteRoot = it; lastLocalEdit = System.currentTimeMillis() },
            Modifier.fillMaxWidth(),
            singleLine = true,
            label = { Text("Allowed remote root") }
        )
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            OutlinedTextField(
                privateKey,
                { privateKey = it; lastLocalEdit = System.currentTimeMillis() },
                Modifier.weight(1f),
                minLines = 1,
                maxLines = 4,
                label = { Text("Client private key") }
            )
            Button(
                enabled = !normalizingPrivateKey && privateKey.isNotBlank(),
                onClick = {
                    normalizingPrivateKey = true
                    privateKeyStatus = "Normalizing key..."
                    scope.launch {
                        try {
                            val normalized = withContext(Dispatchers.IO) {
                                service.callAttr("standardize_private_key", privateKey).toString()
                            }
                            privateKey = normalized
                            lastLocalEdit = System.currentTimeMillis()
                            privateKeyStatus = "Normalized to PEM; saving to client_mqtt.json"
                        } catch (error: Exception) {
                            privateKeyStatus = "Key normalization failed: ${error.message}"
                        } finally {
                            normalizingPrivateKey = false
                        }
                    }
                }
            ) {
                Text(if (normalizingPrivateKey) "Working..." else "Standardize")
            }
        }
        Text(
            if (privateKey.isBlank()) "Private key: not configured"
            else "Private key: configured; value is never copied to RPC logs",
            style = MaterialTheme.typography.bodySmall
        )
        if (privateKeyStatus.isNotBlank()) Text(privateKeyStatus, style = MaterialTheme.typography.bodySmall)
        OutlinedTextField(
            timeout,
            { timeout = it.filter { char -> char.isDigit() || char == '.' }; lastLocalEdit = System.currentTimeMillis() },
            Modifier.fillMaxWidth(),
            singleLine = true,
            label = { Text("Timeout in seconds") }
        )
        Row(
            Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
        ) {
            Text("Allow response without server signature")
            Switch(
                checked = allowNoServerKey,
                onCheckedChange = { allowNoServerKey = it; lastLocalEdit = System.currentTimeMillis() }
            )
        }
        Text(status, style = MaterialTheme.typography.bodySmall)
    }
}

@Composable
private fun SettingsPage(
    modifier: Modifier = Modifier,
    onBack: () -> Unit,
    onPermissions: () -> Unit
) {
    val context = androidx.compose.ui.platform.LocalContext.current
    var useExternal by remember {
        mutableStateOf(context.getSharedPreferences("client_mqtt", Context.MODE_PRIVATE).getBoolean("use_external_scripts", false))
    }
    var status by remember { mutableStateOf("") }
    var aliyunJson by remember { mutableStateOf("{}") }
    var aliyunLoaded by remember { mutableStateOf(false) }
    var aliyunLastEdit by remember { mutableStateOf(0L) }
    var aliyunStatus by remember { mutableStateOf("") }
    var onlineProbeEnabled by remember { mutableStateOf(true) }
    var onlineProbeInterval by remember { mutableStateOf("30") }
    var probeSettingsLoaded by remember { mutableStateOf(false) }
    var probeSettingsLastEdit by remember { mutableStateOf(0L) }
    var probeSettingsStatus by remember { mutableStateOf("") }
    var downloadStatus by remember { mutableStateOf("") }
    var downloadLogs by remember { mutableStateOf(listOf<String>()) }
    var downloading by remember { mutableStateOf(false) }
    var scriptRevision by remember { mutableStateOf(0) }
    var builtinFeatureFiles by remember { mutableStateOf(listOf<String>()) }
    val externalAllowed = Build.VERSION.SDK_INT < Build.VERSION_CODES.R || Environment.isExternalStorageManager()
    val scriptRoot = if (useExternal && externalAllowed) "/sdcard/apm/client_mqtt" else "${context.filesDir}/client_mqtt"
    val featureDirectory = File(scriptRoot, "py_updates")
    val missingFeatures = remember(scriptRoot, scriptRevision, builtinFeatureFiles) {
        builtinFeatureFiles.filterNot { File(featureDirectory, it).isFile }
    }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()

    LaunchedEffect(Unit) {
        runCatching {
            val raw = withContext(Dispatchers.IO) { service.callAttr("builtin_feature_files").toString() }
            val array = org.json.JSONArray(raw)
            builtinFeatureFiles = buildList {
                for (index in 0 until array.length()) add(array.optString(index))
            }
        }
    }

    suspend fun refreshDownloadLogs() {
        val raw = withContext(Dispatchers.IO) { service.callAttr("operation_logs").toString() }
        val array = org.json.JSONArray(raw)
        downloadLogs = buildList {
            for (index in 0 until array.length()) add(array.optString(index))
        }
    }

    LaunchedEffect(downloading) {
        while (downloading) {
            runCatching { refreshDownloadLogs() }
            delay(400)
        }
    }

    LaunchedEffect(Unit) {
        while (true) {
            runCatching {
                val config = withContext(Dispatchers.IO) {
                    JSONObject(service.callAttr("aliyun_settings").toString())
                }
                if (System.currentTimeMillis() - aliyunLastEdit >= 1_200L) {
                    aliyunJson = if (config.has("aliyun_json_draft")) {
                        config.optString("aliyun_json_draft")
                    } else {
                        val incoming = config.optJSONObject("aliyun") ?: JSONObject()
                        val current = runCatching { JSONObject(aliyunJson).toString() }.getOrNull()
                        if (current == incoming.toString()) aliyunJson else incoming.toString(2)
                    }
                    aliyunLoaded = true
                }
            }.onFailure { aliyunStatus = "Unable to sync shared Aliyun settings: ${it.message}" }
            delay(1_000)
        }
    }

    LaunchedEffect(aliyunJson, aliyunLoaded) {
        if (!aliyunLoaded) return@LaunchedEffect
        delay(300)
        val parsed = runCatching { JSONObject(aliyunJson) }.getOrNull()
        val values = JSONObject()
        if (parsed == null) values.put("aliyun_json_draft", aliyunJson)
        else values.put("aliyun", parsed)
        runCatching {
            withContext(Dispatchers.IO) {
                service.callAttr("update_aliyun_settings", values.toString())
            }
            aliyunStatus = if (parsed == null) {
                "Saved draft; invalid JSON keeps the last valid settings active"
            } else {
                "Shared Aliyun settings saved"
            }
        }.onFailure { aliyunStatus = "Aliyun settings save failed: ${it.message}" }
    }

    LaunchedEffect(Unit) {
        while (true) {
            runCatching {
                val config = withContext(Dispatchers.IO) {
                    JSONObject(service.callAttr("general_settings").toString())
                }
                if (System.currentTimeMillis() - probeSettingsLastEdit >= 1_200L) {
                    onlineProbeEnabled = config.optBoolean("online_probe_enabled", true)
                    onlineProbeInterval = config.optInt("online_probe_interval", 30).toString()
                    probeSettingsLoaded = true
                }
            }.onFailure { probeSettingsStatus = "Unable to sync probe settings: ${it.message}" }
            delay(1_000)
        }
    }

    LaunchedEffect(onlineProbeEnabled, onlineProbeInterval, probeSettingsLoaded) {
        if (!probeSettingsLoaded) return@LaunchedEffect
        val interval = onlineProbeInterval.toIntOrNull() ?: return@LaunchedEffect
        delay(300)
        runCatching {
            withContext(Dispatchers.IO) {
                service.callAttr(
                    "update_general_settings",
                    JSONObject()
                        .put("online_probe_enabled", onlineProbeEnabled)
                        .put("online_probe_interval", interval.coerceIn(5, 3600))
                        .toString()
                )
            }
            probeSettingsStatus = "Online probe settings saved"
        }.onFailure { probeSettingsStatus = "Probe settings save failed: ${it.message}" }
    }

    Column(
        modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Text("App settings", style = MaterialTheme.typography.headlineSmall)
            Button(onClick = onBack) { Text("Done") }
        }
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Column(Modifier.weight(1f)) {
                Text("Script directory", style = MaterialTheme.typography.labelLarge)
                Text(scriptRoot, style = MaterialTheme.typography.bodySmall)
                Text(if (externalAllowed) "External storage is authorized" else "Using internal storage; authorize all files to use /sdcard/apm/client_mqtt/", style = MaterialTheme.typography.bodySmall)
            }
            Switch(checked = useExternal, onCheckedChange = {
                useExternal = it
                context.getSharedPreferences("client_mqtt", Context.MODE_PRIVATE).edit().putBoolean("use_external_scripts", it).apply()
                status = "Restart the app to apply script directory"
            })
        }
        HorizontalDivider()
        Text("Shared Aliyun configuration", style = MaterialTheme.typography.titleMedium)
        OutlinedTextField(
            aliyunJson,
            { aliyunJson = it; aliyunLastEdit = System.currentTimeMillis() },
            Modifier.fillMaxWidth(),
            minLines = 6,
            label = { Text("Aliyun configuration JSON") }
        )
        Text(aliyunStatus, style = MaterialTheme.typography.bodySmall)
        HorizontalDivider()
        Text("Online status probe", style = MaterialTheme.typography.titleMedium)
        Row(
            Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
        ) {
            Text("Enable periodic probe")
            Switch(
                checked = onlineProbeEnabled,
                onCheckedChange = {
                    onlineProbeEnabled = it
                    probeSettingsLastEdit = System.currentTimeMillis()
                }
            )
        }
        OutlinedTextField(
            onlineProbeInterval,
            {
                onlineProbeInterval = it.filter(Char::isDigit)
                probeSettingsLastEdit = System.currentTimeMillis()
            },
            Modifier.fillMaxWidth(),
            singleLine = true,
            label = { Text("Probe interval in seconds") }
        )
        Text("After any successful RPC, the next probe waits for this interval. Minimum 5 seconds.", style = MaterialTheme.typography.bodySmall)
        Text(probeSettingsStatus, style = MaterialTheme.typography.bodySmall)
        if (useExternal && externalAllowed) {
            HorizontalDivider()
            Text("External feature scripts", style = MaterialTheme.typography.titleMedium)
            Text(
                when {
                    builtinFeatureFiles.isEmpty() -> "Checking built-in feature files..."
                    missingFeatures.isEmpty() -> "All ${builtinFeatureFiles.size} built-in feature files are present"
                    else -> "${missingFeatures.size} of ${builtinFeatureFiles.size} feature files are missing: ${missingFeatures.joinToString()}"
                },
                style = MaterialTheme.typography.bodySmall
            )
            Button(enabled = !downloading, onClick = {
                downloading = true
                downloadStatus = "Starting Python downloader..."
                downloadLogs = emptyList()
                scope.launch {
                    try {
                        val response = withContext(Dispatchers.IO) {
                            service.callAttr("install_builtin_features", scriptRoot, 4, 15).toString()
                        }
                        val result = JSONObject(response)
                        val ready = result.optJSONArray("results")?.length() ?: 0
                        downloadStatus = if (result.optBoolean("ok")) {
                            "$ready feature scripts ready. Restart if you just changed the script directory."
                        } else {
                            "Some scripts failed. Check the download log and retry."
                        }
                    } catch (error: Exception) {
                        downloadStatus = "Download failed: ${error.message}"
                    } finally {
                        runCatching { refreshDownloadLogs() }
                        downloading = false
                        scriptRevision++
                    }
                }
            }) {
                Text(if (downloading) "Downloading scripts..." else "Download missing feature scripts")
            }
            if (downloadStatus.isNotBlank()) Text(downloadStatus, style = MaterialTheme.typography.bodySmall)
            Text("Download log", style = MaterialTheme.typography.labelLarge)
            Column(
                Modifier
                    .fillMaxWidth()
                    .heightIn(min = 48.dp, max = 180.dp)
                    .background(MaterialTheme.colorScheme.surfaceVariant)
                    .verticalScroll(rememberScrollState())
                    .padding(8.dp),
                verticalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                if (downloadLogs.isEmpty()) {
                    Text("No download activity", style = MaterialTheme.typography.bodySmall)
                } else {
                    downloadLogs.forEach { line -> Text(line, style = MaterialTheme.typography.bodySmall) }
                }
            }
        }
        if (status.isNotBlank()) Text(status, style = MaterialTheme.typography.bodySmall)
        Button(onClick = onPermissions) {
            Icon(Icons.TwoTone.Security, contentDescription = null)
            Text("All Android permissions")
        }
    }
}

@Composable
private fun PermissionPage(onBack: () -> Unit) {
    val context = androidx.compose.ui.platform.LocalContext.current
    var refresh by remember { mutableStateOf(0) }
    var status by remember { mutableStateOf("") }
    val candidates = remember {
        listOf(
            Manifest.permission.CAMERA,
            Manifest.permission.RECORD_AUDIO,
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION,
            Manifest.permission.READ_CONTACTS,
            Manifest.permission.WRITE_CONTACTS,
            Manifest.permission.READ_CALENDAR,
            Manifest.permission.WRITE_CALENDAR,
            Manifest.permission.READ_PHONE_STATE,
            Manifest.permission.CALL_PHONE,
            Manifest.permission.SEND_SMS,
            Manifest.permission.RECEIVE_SMS,
            Manifest.permission.BLUETOOTH_CONNECT,
            Manifest.permission.BLUETOOTH_SCAN,
            Manifest.permission.BLUETOOTH_ADVERTISE,
            Manifest.permission.POST_NOTIFICATIONS,
            Manifest.permission.READ_MEDIA_IMAGES,
            Manifest.permission.READ_MEDIA_VIDEO,
            Manifest.permission.READ_MEDIA_AUDIO,
            Manifest.permission.READ_EXTERNAL_STORAGE,
            Manifest.permission.WRITE_EXTERNAL_STORAGE
        ).filter { permission ->
            when {
                permission.startsWith("android.permission.BLUETOOTH_") -> Build.VERSION.SDK_INT >= Build.VERSION_CODES.S
                permission.startsWith("android.permission.READ_MEDIA_") -> Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
                permission == Manifest.permission.POST_NOTIFICATIONS -> Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
                permission == Manifest.permission.READ_EXTERNAL_STORAGE -> Build.VERSION.SDK_INT <= Build.VERSION_CODES.S_V2
                permission == Manifest.permission.WRITE_EXTERNAL_STORAGE -> Build.VERSION.SDK_INT <= Build.VERSION_CODES.Q
                else -> true
            }
        }
    }
    val permissionLauncher = rememberLauncherForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { result ->
        refresh++
        status = "授权完成：${result.count { it.value }} / ${result.size}"
    }
    val settingsLauncher = rememberLauncherForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { refresh++ }
    val allFilesGranted = Build.VERSION.SDK_INT < Build.VERSION_CODES.R || Environment.isExternalStorageManager()
    Column(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Text("All permissions", style = MaterialTheme.typography.headlineSmall)
            Button(onClick = onBack) { Text("Back") }
        }
        Button(onClick = { permissionLauncher.launch(candidates.toTypedArray()) }) {
            Icon(Icons.TwoTone.Security, contentDescription = null)
            Text("Request all runtime permissions")
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            Button(onClick = {
                settingsLauncher.launch(Intent(Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION, Uri.parse("package:${context.packageName}")))
            }) {
                Text(if (allFilesGranted) "All files access granted" else "Authorize all files access")
            }
        }
        Text("Script storage defaults to internal app storage. After all-files access is granted, enable external scripts in the previous page to use /sdcard/apm/client_mqtt/.", style = MaterialTheme.typography.bodySmall)
        Text(status, style = MaterialTheme.typography.labelMedium)
        LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
            items(candidates) { permission ->
                val granted = ContextCompat.checkSelfPermission(context, permission) == PackageManager.PERMISSION_GRANTED
                Text("${if (granted) "OK" else "--"}  ${permission.removePrefix("android.permission.")}")
            }
        }
    }
}
