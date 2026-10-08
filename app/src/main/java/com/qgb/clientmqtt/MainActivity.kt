package com.qgb.clientmqtt

import android.Manifest
import android.content.ClipData
import android.content.ClipboardManager as AndroidClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.os.SystemClock
import android.widget.Toast
import android.provider.Settings
import android.net.Uri
import java.io.File
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.background
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Box
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.LocalIndication
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
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
import androidx.compose.material.icons.outlined.Call
import androidx.compose.material.icons.outlined.CameraAlt
import androidx.compose.material.icons.outlined.Extension
import androidx.compose.material.icons.outlined.Folder
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.Menu
import androidx.compose.material.icons.outlined.NetworkWifi
import androidx.compose.material.icons.outlined.PlayArrow
import androidx.compose.material.icons.outlined.Refresh
import androidx.compose.material.icons.outlined.Settings
import androidx.compose.material.icons.outlined.Terminal
import androidx.compose.material.icons.outlined.Tune
import androidx.compose.material.icons.twotone.Security
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Checkbox
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.ModalDrawerSheet
import androidx.compose.material3.ModalNavigationDrawer
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationDrawerItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
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
import androidx.compose.runtime.key
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.snapshotFlow
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.Alignment
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.viewinterop.AndroidView
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat
import com.chaquo.python.Python
import com.chaquo.python.PyObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.json.JSONObject

private data class FeatureDescriptor(
    val name: String,
    val title: String,
    val actions: List<String>,
    val icon: String? = null,
    val moduleFile: String = "",
    val source: String = "",
    // ui=python 时界面由 feature 脚本经 Chaquopy 自绘（PythonViewPage 宿主），
    // 其余走 Compose 通用动作页。
    val ui: String = "compose",
    // 脚本导入/加载失败时 Python 端给的错误（设置页列表标红提示）。
    val error: String? = null
)

// 设置页"目标端 feature 列表"的一行：由远程扫描 RPC 返回。
private data class RemoteFeatureEntry(
    val name: String,
    val title: String,
    val actions: List<String>,
    val version: String,
    // 生效版本来源：builtin（AssetFinder 内置）/ py_updates（热更遮蔽）。
    val source: String,
    val dir: String,
    // 同名文件同时存在于内置目录和 py_updates。
    val shadowed: Boolean,
    val error: String? = null
)

private fun featureIcon(feature: FeatureDescriptor) = when (feature.icon?.lowercase()) {
    "folder", "files", "file", "directory" -> Icons.Outlined.Folder
    "camera", "photo", "image" -> Icons.Outlined.CameraAlt
    "wifi", "network", "wireless" -> Icons.Outlined.NetworkWifi
    "audio", "sound", "speaker", "play" -> Icons.Outlined.PlayArrow
    "terminal", "shell", "console", "code", "repl", "python" -> Icons.Outlined.Terminal
    "bug", "debug" -> Icons.Outlined.BugReport
    "phone", "dial", "dialer", "call", "telephone" -> Icons.Outlined.Call
    "info", "about" -> Icons.Outlined.Info
    "settings", "config", "tune" -> Icons.Outlined.Settings
    else -> when (feature.name) {
        "files" -> Icons.Outlined.Folder
        "camera" -> Icons.Outlined.CameraAlt
        "wifi" -> Icons.Outlined.NetworkWifi
        "dialer" -> Icons.Outlined.Call
        "repl" -> Icons.Outlined.Terminal
        else -> Icons.Outlined.Extension
    }
}

private data class TargetDescriptor(
    val id: String,
    val name: String,
    val requestTopic: String,
    val remoteRoot: String,
    // null = 全部 feature 对该目标生效（默认全选）；非空 = 白名单。
    val enabledFeatures: List<String>? = null
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
    var selectedDevice by remember { mutableStateOf("Target") }
    var selectedDeviceId by remember { mutableStateOf("") }
    var selectedTopic by remember { mutableStateOf("sys/device/request") }
    // 当前目标生效的 feature：目标 enabled_features 为 null 时全选（默认），
    // 否则按白名单过滤。底栏/侧栏/pager 一律只用过滤后的列表，避免索引错位。
    val visibleFeatures = remember(features, targets, selectedDeviceId) {
        val whitelist = targets.firstOrNull { it.id == selectedDeviceId }?.enabledFeatures
        if (whitelist == null) features else features.filter { it.name in whitelist }
    }
    val pagerState = rememberPagerState(pageCount = { visibleFeatures.size })
    val pagerScope = rememberCoroutineScope()
    val drawerState = rememberDrawerState(initialValue = androidx.compose.material3.DrawerValue.Closed)
    var settings by remember { mutableStateOf(false) }
    var targetSettings by remember { mutableStateOf(false) }
    var diagnostics by remember { mutableStateOf(false) }
    var editingDeviceId by remember { mutableStateOf<String?>(null) }
    var permissions by remember { mutableStateOf(false) }
    var reloadTarget by remember { mutableStateOf<FeatureDescriptor?>(null) }
    // 冷启动恢复闸门：Python 端 selected_device_id 恢复完成前，
    // 轮询协程不得用 Kotlin 占位 topic 匹配并覆盖持久化选择。
    var restoreDone by remember { mutableStateOf(false) }
    // 每个设备分别记住最后选中的 feature：记录已完成恢复的设备 id，
    // 恢复完成前禁止回写，避免冷启动的占位页覆盖持久化选择。
    var featureRestoredFor by remember { mutableStateOf("") }
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
            if (topic.isBlank()) continue
            val rawName = item.optString("name", topic)
            // 老版本内置默认名 "Target" 对任何 topic 都误导，显示层回退 topic 末段，
            // 与 client_service.update_device_settings 的命名规则保持一致。
            val name = rawName.ifBlank { topic }.let { if (it == "Target") topic.substringAfterLast('/') else it }
            val enabledArray = if (item.isNull("enabled_features")) null else item.optJSONArray("enabled_features")
            val enabledFeatures = enabledArray?.let { arr ->
                buildList {
                    for (featureIndex in 0 until arr.length()) {
                        add(arr.optString(featureIndex))
                    }
                }
            }
            add(TargetDescriptor(
                item.optString("id", topic),
                name,
                topic,
                item.optString("remote_root", "/data/data"),
                enabledFeatures
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
                    ui = item.optString("ui", "compose").ifBlank { "compose" },
                    error = item.optString("error").ifBlank { null },
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
                if (target.id != activeId) {
                    withContext(Dispatchers.IO) { service.callAttr("select_device", target.id) }
                }
            }
        }
        restoreDone = true
        while (true) {
            runCatching { refreshFeatures() }
            delay(3_000)
        }
    }

    LaunchedEffect(Unit) {
        while (true) {
            // 恢复完成前只等待，避免占位 topic 抢占并写掉 Python 端持久化选择。
            if (!restoreDone) {
                delay(100)
                continue
            }
            runCatching {
                val raw = withContext(Dispatchers.IO) { service.callAttr("device_catalog").toString() }
                val updated = parseTargets(raw)
                targets = updated
                // 仅按已恢复的稳定 id 保持当前选择；选中项消失时不擅自跳到别的目标。
                val active = selectedDeviceId
                    .takeIf { it.isNotBlank() }
                    ?.let { id -> updated.firstOrNull { it.id == id } }
                if (active != null) {
                    selectedTopic = active.requestTopic
                    selectedDevice = active.name
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

    LaunchedEffect(selectedDeviceId, onlineProbeEnabled, onlineProbeInterval) {
        while (true) {
            val health = withContext(Dispatchers.IO) {
                runCatching { JSONObject(service.callAttr("target_health", selectedDeviceId).toString()) }
                    .getOrElse { JSONObject() }
            }
            val lastProbeAt = health.optLong("last_probe_at_ms", 0)
            val inFlight = health.optInt("inflight_count", 0)
            val lastProbeOk = health.optBoolean("last_probe_ok", false)
            // 在飞只作为后缀修饰，绝不覆盖已有结论：探针超时/异常结束后
            // 必须落回 offline，而不是被计数钉死在 checking。
            val suffix = if (inFlight > 0) " · checking" else ""
            onlineStatus = when {
                lastProbeAt == 0L && !onlineProbeEnabled -> "probe disabled"
                lastProbeAt == 0L -> "checking"
                lastProbeOk && !onlineProbeEnabled -> "online · probe disabled"
                !lastProbeOk && !onlineProbeEnabled -> "offline · probe disabled"
                lastProbeOk -> "online$suffix"
                else -> "offline$suffix"
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

    LaunchedEffect(visibleFeatures.size) {
        if (visibleFeatures.isNotEmpty() && pagerState.currentPage >= visibleFeatures.size) {
            pagerState.scrollToPage(visibleFeatures.lastIndex)
        }
    }

    // 恢复当前设备上次选中的 feature tab（每设备只做一次；
    // 特性表 3s 轮询会反复替换列表，靠 featureRestoredFor 挡掉重入）。
    // 只在该目标当前生效的 feature 里找，被禁用的记忆 tab 不恢复。
    LaunchedEffect(selectedDeviceId, visibleFeatures) {
        val deviceId = selectedDeviceId
        if (deviceId.isBlank() || visibleFeatures.isEmpty() || featureRestoredFor == deviceId) return@LaunchedEffect
        val saved = withContext(Dispatchers.IO) {
            runCatching { service.callAttr("selected_feature", deviceId).toString() }.getOrDefault("")
        }
        val index = visibleFeatures.indexOfFirst { it.name == saved }
        if (index >= 0 && pagerState.currentPage != index) {
            pagerState.scrollToPage(index)
        }
        featureRestoredFor = deviceId
    }

    // 用户切页（点 tab 或滑动）后回写该设备的最后选中 feature。
    LaunchedEffect(pagerState, selectedDeviceId, visibleFeatures.isNotEmpty()) {
        snapshotFlow { pagerState.currentPage }.collect { page ->
            val deviceId = selectedDeviceId
            val feature = visibleFeatures.getOrNull(page) ?: return@collect
            if (deviceId.isBlank() || featureRestoredFor != deviceId) return@collect
            withContext(Dispatchers.IO) {
                runCatching { service.callAttr("select_feature", feature.name, deviceId) }
            }
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

    // 无覆盖页时的全局返回：先交给当前 Python 自绘 feature（Files 在子目录时回上一层）；
    // feature 不消费则"连按两次退出"（2 秒窗口 + Toast 提示），不再一闪退到桌面。
    val backContext = LocalContext.current
    var lastFeatureBackAt by remember { mutableLongStateOf(0L) }
    val currentFeatureName = visibleFeatures.getOrNull(pagerState.currentPage)?.name
    LaunchedEffect(currentFeatureName) { lastFeatureBackAt = 0L }
    BackHandler(enabled = !permissions && !targetSettings && !settings && !diagnostics) {
        val consumed = currentFeatureName?.let { name ->
            // 纯查询/轻量 View 操作，在主线程快速返回；feature 异常时按未消费处理。
            runCatching { service.callAttr("handle_feature_back", name).toBoolean() }
                .getOrDefault(false)
        } ?: false
        if (consumed) return@BackHandler
        val now = SystemClock.uptimeMillis()
        if (now - lastFeatureBackAt in 1..2000L) {
            (backContext as? android.app.Activity)?.finish()
        } else {
            lastFeatureBackAt = now
            Toast.makeText(backContext, "再按一次退出", Toast.LENGTH_SHORT).show()
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
                // feature 多了之后 Scripts 区必须限范围：占满抽屉剩余高度并在区内
                // 滚动，Targets 区始终固定可见、不被挤出屏幕。
                Column(
                    modifier = Modifier
                        .weight(1f)
                        .fillMaxWidth()
                        .verticalScroll(rememberScrollState())
                ) {
                visibleFeatures.forEachIndexed { index, feature ->
                    val featureSelected = pagerState.currentPage == index && !settings && !targetSettings
                    // 自绘条目：combinedClickable 同一处理器内确定地分发短按/长按，
                    // 不要在带内部 clickable 的 NavigationDrawerItem 上叠加 pointerInput，
                    // 实测内部手势竞争会导致长按不回调。
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(56.dp)
                            .padding(horizontal = 12.dp)
                            .clip(RoundedCornerShape(28.dp))
                            .background(
                                if (featureSelected) MaterialTheme.colorScheme.secondaryContainer
                                else Color.Transparent
                            )
                            .combinedClickable(
                                interactionSource = remember { MutableInteractionSource() },
                                indication = LocalIndication.current,
                                onClick = {
                                    pagerScope.launch {
                                        pagerState.animateScrollToPage(index)
                                        drawerState.close()
                                    }
                                },
                                onLongClick = { reloadTarget = feature }
                            )
                            .padding(horizontal = 16.dp),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        Icon(
                            featureIcon(feature),
                            contentDescription = feature.title,
                            tint = if (featureSelected) MaterialTheme.colorScheme.onSecondaryContainer
                            else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Text(
                            feature.title,
                            color = if (featureSelected) MaterialTheme.colorScheme.onSecondaryContainer
                            else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                }
                HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
                Text("Targets", style = MaterialTheme.typography.titleMedium, modifier = Modifier.padding(16.dp))
                targets.forEach { target ->
                    NavigationDrawerItem(
                        label = { Text(target.requestTopic) },
                        selected = selectedTopic == target.requestTopic,
                        onClick = {
                            // 先乐观更新 UI，Python 选择/落盘放到 IO，避免 Chaquopy 调用卡主线程。
                            selectedDeviceId = target.id
                            selectedTopic = target.requestTopic
                            selectedDevice = target.name
                            val chosenId = target.id
                            scope.launch {
                                withContext(Dispatchers.IO) {
                                    runCatching { service.callAttr("select_device", chosenId) }
                                }
                                drawerState.close()
                            }
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
                            // 名称只作别名，topic 必须同屏可见：
                            // 避免"选了 sys/device/request 却只显示 Target"的困惑。
                            Text("$selectedTopic · $onlineStatus", style = MaterialTheme.typography.labelSmall)
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
                if (!settings && !targetSettings && !permissions && !diagnostics && visibleFeatures.isNotEmpty()) {
                    val selectedPage = pagerState.currentPage.coerceIn(0, visibleFeatures.lastIndex)
                    NavigationBar {
                        // 每项固定 72dp：一屏平铺约 6 个；feature 更多时整条横向滚动，
                        // 不再用 weight 平分把大量图标压成看不清的细条。
                        Row(
                            modifier = Modifier
                                .fillMaxWidth()
                                .horizontalScroll(rememberScrollState())
                        ) {
                        visibleFeatures.forEachIndexed { index, feature ->
                            val itemSelected = selectedPage == index
                            // 自绘底部条目：短按切页、长按重载由同一个 combinedClickable
                            // 确定分发；NavigationBarItem 内部 clickable 会吞掉叠加的长按手势。
                            Row(
                                modifier = Modifier
                                    .width(72.dp)
                                    .height(80.dp)
                                    .combinedClickable(
                                        interactionSource = remember { MutableInteractionSource() },
                                        indication = LocalIndication.current,
                                        onClick = {
                                            pagerScope.launch { pagerState.animateScrollToPage(index) }
                                        },
                                        onLongClick = { reloadTarget = feature }
                                    ),
                                horizontalArrangement = Arrangement.Center,
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                                    Box(
                                        modifier = Modifier
                                            .size(width = 64.dp, height = 32.dp)
                                            .clip(CircleShape)
                                            .background(
                                                if (itemSelected) MaterialTheme.colorScheme.secondaryContainer
                                                else Color.Transparent
                                            ),
                                        contentAlignment = Alignment.Center
                                    ) {
                                        Icon(
                                            featureIcon(feature),
                                            contentDescription = feature.title,
                                            tint = if (itemSelected) MaterialTheme.colorScheme.onSecondaryContainer
                                            else MaterialTheme.colorScheme.onSurfaceVariant
                                        )
                                    }
                                    Text(
                                        feature.title,
                                        maxLines = 1,
                                        style = MaterialTheme.typography.labelSmall,
                                        color = if (itemSelected) MaterialTheme.colorScheme.onSecondaryContainer
                                        else MaterialTheme.colorScheme.onSurfaceVariant
                                    )
                                }
                            }
                        }
                        }
                    }
                }
            }
        ) { padding ->
            // feature pager 常驻组合：RPC 日志/权限/目标设置/应用设置都只是盖在
            // 上面的全屏覆盖页。切到它们时 pager 不离开组合，页面协程不会取消，
            // 进行中的网络请求继续跑，照片/输出等状态也都保留，返回时原样还在。
            Box(Modifier.fillMaxSize()) {
                Column(modifier = Modifier.fillMaxSize().padding(padding)) {
                    when {
                        features.isEmpty() ->
                            Text("No feature scripts found", modifier = Modifier.padding(16.dp))
                        // 该目标把 feature 全关了：不崩不空转，引导去目标设置里勾选。
                        visibleFeatures.isEmpty() -> Column(
                            modifier = Modifier.fillMaxSize().padding(24.dp),
                            verticalArrangement = Arrangement.spacedBy(12.dp)
                        ) {
                            Text(
                                "No feature enabled for this target",
                                style = MaterialTheme.typography.titleMedium
                            )
                            Text(
                                "Open target settings and choose which features apply to $selectedTopic.",
                                style = MaterialTheme.typography.bodyMedium
                            )
                            OutlinedButton(onClick = {
                                editingDeviceId = selectedDeviceId
                                targetSettings = true
                            }) { Text("Enable features") }
                        }
                        else ->
                            // 所有页常驻组合（不只是相邻页）：横向切 feature 时不丢任何
                            // 页面状态；page 内容按 feature 名 key 住，目录刷新后身份不变。
                            HorizontalPager(
                                state = pagerState,
                                modifier = Modifier.fillMaxSize().weight(1f),
                                beyondViewportPageCount = visibleFeatures.size
                            ) { page ->
                                key(visibleFeatures[page].name) {
                                    // 所有 feature 界面都由 feature_*.py 自绘（ui=python），
                                    // APK 端只保留这一个通用宿主；新增/改版界面只热更脚本。
                                    PythonViewPage(visibleFeatures[page], service)
                                }
                            }
                    }
                }
                // 覆盖页：不透明背景全盖住底层 pager，但不销毁它。
                when {
                    diagnostics -> Box(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
                        DiagnosticsPage(
                            modifier = Modifier.padding(padding),
                            selectedTopic = selectedTopic,
                            onlineStatus = onlineStatus,
                            onBack = { diagnostics = false }
                        )
                    }
                    permissions -> Box(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
                        PermissionPage(onBack = { permissions = false })
                    }
                    targetSettings -> Box(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
                        TargetSettingsPage(
                            modifier = Modifier.padding(padding),
                            existingDeviceId = editingDeviceId,
                            onBack = {
                                targetSettings = false
                                // 勾选改动落盘后立刻重拉目录，不等 1s 轮询。
                                scope.launch { runCatching { refreshTargets() } }
                            },
                            onTargetCreated = { id ->
                                selectedDeviceId = id
                            }
                        )
                    }
                    settings -> Box(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.background)) {
                        SettingsPage(
                            modifier = Modifier.padding(padding),
                            selectedTopic = selectedTopic,
                            onBack = { settings = false },
                            onPermissions = { permissions = true },
                            // 设置页"刷新列表"/下载完成后立刻重扫 catalog，
                            // 不必等外层 3s 轮询，新增 feature 当场出现在底栏。
                            onRefreshFeatureList = {
                                scope.launch { runCatching { refreshFeatures() } }
                            }
                        )
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

/**
 * Python 自绘 feature 的通用宿主：feature 脚本提供 build_view(context) 返回
 * android.view.View，APK 只负责把它挂进 Compose。新增自绘 feature 无需改 APK，
 * 脚本经 py_updates / 内置目录热更即可（配合目录页长按 Reload 重载）。
 */
@Composable
private fun PythonViewPage(feature: FeatureDescriptor, service: PyObject) {
    AndroidView(
        modifier = Modifier.fillMaxSize(),
        factory = { context ->
            runCatching {
                service.callAttr("build_feature_view", feature.name, context)
                    .toJava(android.view.View::class.java)
            }.getOrElse { error ->
                android.widget.TextView(context).apply {
                    text = "Python UI failed to build:\n${error.javaClass.simpleName}: ${error.message}"
                    setPadding(48, 48, 48, 48)
                }
            }
        }
    )
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
        if (status.isNotBlank()) SelectionContainer { Text(status, style = MaterialTheme.typography.bodySmall) }
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
    // feature 勾选：null=全部生效（默认全选）；非空=该目标的 feature 白名单。
    val featureCatalog = remember { mutableStateOf(listOf<FeatureDescriptor>()) }
    var enabledFeatures by remember(existingDeviceId) { mutableStateOf<List<String>?>(null) }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()

    LaunchedEffect(Unit) {
        runCatching {
            val raw = withContext(Dispatchers.IO) { service.callAttr("feature_catalog").toString() }
            val array = org.json.JSONArray(raw)
            featureCatalog.value = buildList {
                for (index in 0 until array.length()) {
                    val item = array.optJSONObject(index) ?: continue
                    add(FeatureDescriptor(
                        name = item.optString("name"),
                        title = item.optString("title", item.optString("name")),
                        actions = emptyList(),
                        ui = item.optString("ui", "compose").ifBlank { "compose" }
                    ))
                }
            }
        }
    }

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
                    val enabledArray = if (config.isNull("enabled_features")) null
                        else config.optJSONArray("enabled_features")
                    enabledFeatures = enabledArray?.let { arr ->
                        buildList {
                            for (featureIndex in 0 until arr.length()) add(arr.optString(featureIndex))
                        }
                    }
                    loaded = true
                }
            }.onFailure { status = "Unable to sync target settings: ${it.message}" }
            delay(1_000)
        }
    }

    LaunchedEffect(deviceId, topic, remoteRoot, privateKey, timeout, allowNoServerKey, enabledFeatures, loaded) {
        if (!loaded || topic.isBlank()) return@LaunchedEffect
        delay(300)
        val timeoutValue = timeout.toDoubleOrNull()?.takeIf { it > 0 }
        val values = JSONObject()
            .put("request_topic", topic.trim())
            .put("remote_root", remoteRoot)
            .put("private_key", privateKey)
            .put("allow_no_server_pubkey_response", allowNoServerKey)
        // null 序列化成 JSON null：Python 端解释为"缺省=全选"。
        if (enabledFeatures == null) values.put("enabled_features", JSONObject.NULL)
        else values.put("enabled_features", org.json.JSONArray(enabledFeatures))
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
        HorizontalDivider(modifier = Modifier.padding(vertical = 8.dp))
        // 每个目标独立的 feature 生效列表：默认全选（enabled_features=null），
        // 勾掉任意一项才落白名单；重新勾齐回到 null（新装 feature 自动生效）。
        val catalog = featureCatalog.value
        val allFeaturesEnabled = enabledFeatures == null
        Row(
            Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Column(Modifier.weight(1f)) {
                Text("Enabled features", style = MaterialTheme.typography.titleSmall)
                Text(
                    "Choose which features apply to this target. Default: all.",
                    style = MaterialTheme.typography.bodySmall
                )
            }
            Checkbox(
                checked = allFeaturesEnabled,
                onCheckedChange = { checked ->
                    enabledFeatures = if (checked) null else emptyList()
                    lastLocalEdit = System.currentTimeMillis()
                }
            )
        }
        if (!allFeaturesEnabled) {
            catalog.forEach { descriptor ->
                val checked = enabledFeatures?.contains(descriptor.name) == true
                Row(
                    Modifier
                        .fillMaxWidth()
                        .clip(RoundedCornerShape(8.dp))
                        .combinedClickable(
                            interactionSource = remember { MutableInteractionSource() },
                            indication = LocalIndication.current,
                            onClick = {
                                val current = enabledFeatures ?: catalog.map { it.name }
                                val next = if (checked) current - descriptor.name
                                else (current + descriptor.name).distinct()
                                // 全部勾齐时回归 null（默认全选语义），新装 feature 自动可见。
                                enabledFeatures = if (catalog.all { it.name in next }) null else next
                                lastLocalEdit = System.currentTimeMillis()
                            }
                        )
                        .padding(horizontal = 4.dp, vertical = 2.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Checkbox(
                        checked = checked,
                        onCheckedChange = null
                    )
                    Text(descriptor.title)
                }
            }
        }
        if (status.isNotBlank()) SelectionContainer {
            Text(status, style = MaterialTheme.typography.bodySmall)
        }
    }
}

@Composable
private fun SettingsPage(
    modifier: Modifier = Modifier,
    selectedTopic: String,
    onBack: () -> Unit,
    onPermissions: () -> Unit,
    onRefreshFeatureList: () -> Unit
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
    // 实际扫描到的 feature 全量列表（内置 + py_updates，含加载失败条目）。
    var featureList by remember { mutableStateOf(listOf<FeatureDescriptor>()) }
    var featureListBusy by remember { mutableStateOf(false) }
    // 远程目标扫描出的 feature 列表（Refresh 按钮打的是目标端，不是本机）。
    var remoteFeatures by remember { mutableStateOf(listOf<RemoteFeatureEntry>()) }
    var remoteBusy by remember { mutableStateOf(false) }
    var remoteStatus by remember { mutableStateOf("") }
    var remoteOk by remember { mutableStateOf(false) }
    // feature 下载根 URL：Python 端有默认值（ghfast 代理 GitHub main），
    // 加载完成前输入框显示占位，绝不能把空串回写覆盖默认配置。
    var featureUrlRoot by remember { mutableStateOf("") }
    var featureUrlLoaded by remember { mutableStateOf(false) }
    var featureUrlLastEdit by remember { mutableStateOf(0L) }
    var featureUrlStatus by remember { mutableStateOf("") }
    var newFeatureName by remember { mutableStateOf("") }
    val externalAllowed = Build.VERSION.SDK_INT < Build.VERSION_CODES.R || Environment.isExternalStorageManager()
    val scriptRoot = if (useExternal && externalAllowed) "/sdcard/apm/client_mqtt" else "${context.filesDir}/client_mqtt"
    val featureDirectory = File(scriptRoot, "py_updates")
    val missingFeatures = remember(scriptRoot, scriptRevision, builtinFeatureFiles) {
        builtinFeatureFiles.filterNot { File(featureDirectory, it).isFile }
    }
    val service = remember { Python.getInstance().getModule("client_service") }
    val scope = rememberCoroutineScope()

    // Aliyun JSON 统一解析入口：容忍首尾空白与粘贴文件时常见的 BOM。
    // 所有解析点必须走这里，保证"解析失败"语义在轮询回填、自动保存、错误提示三处一致。
    fun parseAliyunJson(text: String): Result<JSONObject> =
        runCatching { JSONObject(text.trim().removePrefix("\uFEFF")) }

    LaunchedEffect(Unit) {
        runCatching {
            val raw = withContext(Dispatchers.IO) { service.callAttr("builtin_feature_files").toString() }
            val array = org.json.JSONArray(raw)
            builtinFeatureFiles = buildList {
                for (index in 0 until array.length()) add(array.optString(index))
            }
        }
    }

    fun parseFeatureCatalog(raw: String) = buildList {
        val array = org.json.JSONArray(raw)
        for (index in 0 until array.length()) {
            val item = array.optJSONObject(index) ?: continue
            val actions = item.optJSONArray("actions") ?: org.json.JSONArray()
            add(FeatureDescriptor(
                name = item.optString("name"),
                title = item.optString("title", item.optString("name")),
                actions = buildList {
                    for (actionIndex in 0 until actions.length()) add(actions.optString(actionIndex))
                },
                icon = if (item.isNull("icon")) null else item.optString("icon").ifBlank { null },
                moduleFile = item.optString("module_file"),
                source = item.optString("source"),
                ui = item.optString("ui", "compose").ifBlank { "compose" },
                error = item.optString("error").ifBlank { null },
            ))
        }
    }

    // 重扫 py_updates（Python 端同时把新 feature 收养进目标白名单），
    // 返回最新全量列表；rescan 本身就返回 catalog，不必再发一次请求。
    suspend fun rescanFeatureList() {
        featureListBusy = true
        try {
            val raw = withContext(Dispatchers.IO) { service.callAttr("rescan_features").toString() }
            featureList = parseFeatureCatalog(raw)
            scriptRevision++
        } finally {
            featureListBusy = false
        }
    }

    LaunchedEffect(Unit) {
        runCatching {
            val raw = withContext(Dispatchers.IO) { service.callAttr("feature_catalog").toString() }
            featureList = parseFeatureCatalog(raw)
        }
    }

    // 扫描【远程目标】的 feature 文件（内置 AssetFinder + 目标端 py_updates）。
    // 本机列表自动发现、无需手刷；这里的 Refresh 专门问目标端要清单。
    fun parseRemoteCatalog(raw: String): List<RemoteFeatureEntry> {
        val payload = JSONObject(raw)
        val array = payload.optJSONArray("features") ?: return emptyList()
        return buildList {
            for (index in 0 until array.length()) {
                val item = array.optJSONObject(index) ?: continue
                val name = item.optString("name")
                if (name.isBlank()) continue
                val sources = item.optJSONArray("sources")
                val active = item.optJSONObject("active") ?: JSONObject()
                val title = active.optString("title").ifBlank {
                    name.replaceFirstChar { it.uppercase() }
                }
                val actionsArray = active.optJSONArray("actions") ?: org.json.JSONArray()
                add(RemoteFeatureEntry(
                    name = name,
                    title = title,
                    actions = buildList {
                        for (actionIndex in 0 until actionsArray.length()) {
                            add(actionsArray.optString(actionIndex))
                        }
                    },
                    version = active.opt("version")?.toString().orEmpty(),
                    source = active.optString("source").ifBlank { "unknown" },
                    dir = active.optString("dir"),
                    shadowed = (sources?.length() ?: 0) > 1,
                    error = active.optString("error").ifBlank { null }
                ))
            }
        }
    }

    suspend fun refreshRemoteFeatures() {
        remoteBusy = true
        try {
            val raw = withContext(Dispatchers.IO) {
                service.callAttr("remote_feature_catalog").toString()
            }
            val payload = JSONObject(raw)
            remoteOk = payload.optBoolean("ok")
            if (remoteOk) {
                remoteFeatures = parseRemoteCatalog(raw)
                val dirs = payload.optJSONArray("candidate_dirs")
                val elapsed = payload.opt("elapsed_ms")
                remoteStatus = buildString {
                    append("${remoteFeatures.size} features · ${dirs?.length() ?: 0} dirs")
                    if (elapsed != null) append(" · ${elapsed}ms")
                }
            } else {
                remoteFeatures = emptyList()
                remoteStatus = payload.optString("error").ifBlank { "scan failed" }
            }
        } catch (error: Exception) {
            remoteOk = false
            remoteFeatures = emptyList()
            remoteStatus = "scan failed: ${error.message}"
        } finally {
            remoteBusy = false
        }
    }

    // 进入设置页自动扫一次远程；之后手动 Refresh 才再扫（避免频繁 RPC）。
    LaunchedEffect(Unit) { runCatching { refreshRemoteFeatures() } }

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
                    // 远端始终下发 aliyun_json_draft 键，无草稿时为 null。
                    // 不能用 has()+optString：旧版 Android 上 null 会被塌缩成字面量
                    // "null"（新版塌缩成 ""），在停顿 1.2s 后冲掉输入框内容。
                    val rawDraft = if (config.isNull("aliyun_json_draft")) null
                        else config.optString("aliyun_json_draft")
                    // 历史 Bug 落盘过 ""/"null" 脏草稿，按无草稿处理；随后有效的 aliyun
                    // 回填会触发一次正常保存，Python 端顺手把脏草稿 pop 掉。
                    val draft = rawDraft?.takeIf { it.isNotBlank() && it.trim() != "null" }
                    if (!draft.isNullOrEmpty()) {
                        if (aliyunJson != draft) aliyunJson = draft
                    } else {
                        val incoming = config.optJSONObject("aliyun") ?: JSONObject()
                        val current = parseAliyunJson(aliyunJson).getOrNull()
                        val localIsGarbage = aliyunJson.isBlank() || aliyunJson.trim() == "null"
                        // 用户真正编辑中的非法文本绝不能被远端覆盖（只有空/"null"
                        // 这类本 Bug 产物才允许回填有效配置）。
                        when {
                            current != null && current.toString() != incoming.toString() ->
                                aliyunJson = incoming.toString(2)
                            current == null && localIsGarbage ->
                                aliyunJson = incoming.toString(2)
                        }
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
        val parseResult = parseAliyunJson(aliyunJson)
        val parsed = parseResult.getOrNull()
        val values = JSONObject()
        if (parsed == null) values.put("aliyun_json_draft", aliyunJson)
        else values.put("aliyun", parsed)
        runCatching {
            withContext(Dispatchers.IO) {
                service.callAttr("update_aliyun_settings", values.toString())
            }
            // 解析失败只是提示：文本原样保留为草稿，上次有效配置继续生效，绝不清空输入框。
            aliyunStatus = if (parsed == null) {
                "Invalid JSON (${parseResult.exceptionOrNull()?.message}); text kept as draft, last valid settings stay active"
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

    // feature 下载根 URL：1s 轮询回填（停顿 1.2s 后），编辑 300ms 防抖后保存。
    LaunchedEffect(Unit) {
        while (true) {
            runCatching {
                val config = withContext(Dispatchers.IO) {
                    JSONObject(service.callAttr("feature_download_settings").toString())
                }
                if (System.currentTimeMillis() - featureUrlLastEdit >= 1_200L) {
                    val incoming = config.optString("feature_url_root")
                    if (incoming.isNotBlank() && incoming != featureUrlRoot) {
                        featureUrlRoot = incoming
                    }
                    featureUrlLoaded = true
                }
            }.onFailure { featureUrlStatus = "Unable to load feature URL root: ${it.message}" }
            delay(1_000)
        }
    }

    LaunchedEffect(featureUrlRoot, featureUrlLoaded) {
        if (!featureUrlLoaded || featureUrlRoot.isBlank()) return@LaunchedEffect
        delay(300)
        runCatching {
            withContext(Dispatchers.IO) {
                service.callAttr(
                    "update_feature_download_settings",
                    JSONObject().put("feature_url_root", featureUrlRoot).toString()
                )
            }
            featureUrlStatus = "Feature URL root saved"
        }.onFailure { featureUrlStatus = "Feature URL root invalid: ${it.message}" }
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
        val aliyunParseError = remember(aliyunJson) {
            parseAliyunJson(aliyunJson).exceptionOrNull()?.message
        }
        OutlinedTextField(
            value = aliyunJson,
            onValueChange = { aliyunJson = it; aliyunLastEdit = System.currentTimeMillis() },
            modifier = Modifier.fillMaxWidth(),
            minLines = 6,
            isError = aliyunParseError != null,
            supportingText = if (aliyunParseError != null) {
                {
                    Text(
                        "Invalid JSON: $aliyunParseError — text kept editable, last valid settings stay active",
                        color = MaterialTheme.colorScheme.error
                    )
                }
            } else null,
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
        HorizontalDivider()
        Row(
            Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
        ) {
            Column(Modifier.weight(1f)) {
                Text("Features on target", style = MaterialTheme.typography.titleMedium)
                Text(
                    "Scans the SELECTED TARGET's AssetFinder and py_updates over RPC: $selectedTopic",
                    style = MaterialTheme.typography.bodySmall
                )
            }
            OutlinedButton(
                enabled = !remoteBusy,
                onClick = { scope.launch { runCatching { refreshRemoteFeatures() } } }
            ) {
                Icon(Icons.Outlined.Refresh, contentDescription = null)
                Text(if (remoteBusy) "Scanning..." else "Refresh target list")
            }
        }
        if (remoteStatus.isNotBlank()) {
            SelectionContainer {
                Text(
                    remoteStatus,
                    style = MaterialTheme.typography.bodySmall,
                    color = if (remoteOk) MaterialTheme.colorScheme.onSurfaceVariant
                    else MaterialTheme.colorScheme.error
                )
            }
        }
        Column(
            Modifier
                .fillMaxWidth()
                .heightIn(min = 48.dp, max = 240.dp)
                .background(MaterialTheme.colorScheme.surfaceVariant)
                .verticalScroll(rememberScrollState())
                .padding(8.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            if (remoteFeatures.isEmpty()) {
                Text(
                    if (remoteBusy) "Scanning target..."
                    else "Not scanned yet. Tap \"Refresh target list\".",
                    style = MaterialTheme.typography.bodySmall
                )
            }
            remoteFeatures.forEach { entry ->
                Column {
                    Text(
                        buildString {
                            append("${entry.title}  (${entry.name})")
                            if (entry.shadowed) append("  · override")
                        },
                        style = MaterialTheme.typography.bodyMedium
                    )
                    Text(
                        buildString {
                            append(if (entry.source == "py_updates") "py_updates" else "built-in")
                            if (entry.version.isNotBlank()) append("  · v${entry.version}")
                            if (entry.actions.isNotEmpty()) append("  · ${entry.actions.joinToString()}")
                        },
                        style = MaterialTheme.typography.bodySmall
                    )
                    SelectionContainer {
                        Text(entry.dir, style = MaterialTheme.typography.bodySmall)
                    }
                    entry.error?.let { message ->
                        SelectionContainer {
                            Text(
                                "error: $message",
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.error
                            )
                        }
                    }
                    HorizontalDivider()
                }
            }
        }
        if (useExternal && externalAllowed) {
            HorizontalDivider()
            Text("External feature scripts", style = MaterialTheme.typography.titleMedium)
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
            ) {
                Column(Modifier.weight(1f)) {
                    Text(
                        when {
                            builtinFeatureFiles.isEmpty() -> "Checking built-in feature files..."
                            missingFeatures.isEmpty() -> "All ${builtinFeatureFiles.size} built-in feature files are present"
                            else -> "${missingFeatures.size} of ${builtinFeatureFiles.size} feature files are missing: ${missingFeatures.joinToString()}"
                        },
                        style = MaterialTheme.typography.bodySmall
                    )
                    Text(
                        "Put feature_*.py into py_updates manually, then tap refresh to discover it.",
                        style = MaterialTheme.typography.bodySmall
                    )
                }
                OutlinedButton(
                    enabled = !downloading && !featureListBusy,
                    onClick = {
                        // 立即让 Python 重扫【本机】py_updates（同时收养新 feature 进
                        // 目标白名单），然后刷新本页列表和外层底栏。
                        // 远程目标的 feature 列表用上面的 "Refresh target list"。
                        scope.launch {
                            runCatching { rescanFeatureList() }
                            onRefreshFeatureList()
                        }
                    }
                ) {
                    Icon(Icons.Outlined.Refresh, contentDescription = null)
                    Text(if (featureListBusy) "Scanning..." else "Rescan this device")
                }
            }
            // 实际扫描到的 feature 全量列表（文件名即 feature 名）：
            // 来源、加载错误、Reload、py_updates 覆盖删除都在这里操作。
            Text("Features on this device", style = MaterialTheme.typography.labelLarge)
            Column(
                Modifier
                    .fillMaxWidth()
                    .heightIn(min = 48.dp, max = 260.dp)
                    .background(MaterialTheme.colorScheme.surfaceVariant)
                    .verticalScroll(rememberScrollState())
                    .padding(8.dp),
                verticalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                if (featureList.isEmpty()) {
                    Text(
                        "No features discovered yet. Tap \"Refresh feature list\".",
                        style = MaterialTheme.typography.bodySmall
                    )
                }
                featureList.forEach { descriptor ->
                    Row(
                        Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Column(Modifier.weight(1f)) {
                            Text(
                                "${descriptor.title}  (${descriptor.name})",
                                style = MaterialTheme.typography.bodyMedium
                            )
                            Text(
                                when (descriptor.source) {
                                    "py_updates" -> "py_updates override"
                                    "builtin" -> "built-in"
                                    else -> descriptor.source.ifBlank { "not loaded" }
                                },
                                style = MaterialTheme.typography.bodySmall
                            )
                            descriptor.error?.let { message ->
                                SelectionContainer {
                                    Text(
                                        "error: $message",
                                        style = MaterialTheme.typography.bodySmall,
                                        color = MaterialTheme.colorScheme.error
                                    )
                                }
                            }
                        }
                        if (descriptor.source == "py_updates") {
                            OutlinedButton(
                                enabled = !downloading,
                                onClick = {
                                    downloading = true
                                    downloadStatus = "Removing feature_${descriptor.name}.py ..."
                                    scope.launch {
                                        try {
                                            val response = withContext(Dispatchers.IO) {
                                                service.callAttr(
                                                    "delete_runtime_feature",
                                                    scriptRoot,
                                                    descriptor.name
                                                ).toString()
                                            }
                                            val result = JSONObject(response)
                                            downloadStatus = if (result.optBoolean("ok")) {
                                                if (result.optBoolean("falls_back_to_builtin")) {
                                                    "feature_${descriptor.name}.py removed; built-in version is active again."
                                                } else {
                                                    "feature_${descriptor.name}.py removed."
                                                }
                                            } else {
                                                "Remove failed: ${result.optString("error", "unknown error")}"
                                            }
                                        } catch (error: Exception) {
                                            downloadStatus = "Remove failed: ${error.message}"
                                        } finally {
                                            downloading = false
                                            runCatching { rescanFeatureList() }
                                            onRefreshFeatureList()
                                        }
                                    }
                                }
                            ) { Text("Delete") }
                        }
                        OutlinedButton(
                            enabled = !downloading,
                            onClick = {
                                downloading = true
                                downloadStatus = "Reloading feature_${descriptor.name}.py ..."
                                scope.launch {
                                    try {
                                        val response = withContext(Dispatchers.IO) {
                                            service.callAttr("reload_feature", descriptor.name).toString()
                                        }
                                        val result = JSONObject(response)
                                        downloadStatus = if (result.optBoolean("ok")) {
                                            "${descriptor.title} reloaded."
                                        } else {
                                            "Reload failed: ${result.optString("error", "unknown error")}"
                                        }
                                    } catch (error: Exception) {
                                        downloadStatus = "Reload failed: ${error.message}"
                                    } finally {
                                        downloading = false
                                        runCatching { rescanFeatureList() }
                                        onRefreshFeatureList()
                                    }
                                }
                            }
                        ) { Text("Reload") }
                    }
                }
            }
            OutlinedTextField(
                value = featureUrlRoot,
                onValueChange = {
                    featureUrlRoot = it
                    featureUrlLastEdit = System.currentTimeMillis()
                },
                modifier = Modifier.fillMaxWidth(),
                singleLine = true,
                label = { Text("Feature download URL root") },
                placeholder = {
                    Text("https://host/path/app/src/main/python/")
                },
                supportingText = {
                    Text("New and built-in feature_*.py files are fetched from <root>feature_<name>.py; built-ins then fall back to GitHub.")
                }
            )
            Text(featureUrlStatus, style = MaterialTheme.typography.bodySmall)
            // force=false 只补缺失；force=true 无条件重下全部并清模块/pyc 缓存，
            // 下一次 call_feature 立刻用新脚本，无需重启。
            fun startBuiltinDownload(force: Boolean) {
                downloading = true
                downloadStatus = "Starting Python downloader..."
                downloadLogs = emptyList()
                scope.launch {
                    try {
                        val method = if (force) "reinstall_builtin_features" else "install_builtin_features"
                        val response = withContext(Dispatchers.IO) {
                            service.callAttr(method, scriptRoot, 4, 15).toString()
                        }
                        val result = JSONObject(response)
                        val ready = result.optJSONArray("results")?.length() ?: 0
                        downloadStatus = when {
                            !result.optBoolean("ok") ->
                                "Some scripts failed. Check the download log and retry."
                            force ->
                                "$ready feature scripts re-downloaded and loaded immediately."
                            else ->
                                "$ready feature scripts ready. Restart if you just changed the script directory."
                        }
                    } catch (error: Exception) {
                        downloadStatus = "Download failed: ${error.message}"
                    } finally {
                        runCatching { refreshDownloadLogs() }
                        downloading = false
                        scriptRevision++
                        runCatching { rescanFeatureList() }
                        onRefreshFeatureList()
                    }
                }
            }
            Button(
                enabled = !downloading,
                onClick = { startBuiltinDownload(force = false) }
            ) {
                Text(if (downloading) "Downloading scripts..." else "Download missing feature scripts")
            }
            Button(
                enabled = !downloading,
                onClick = { startBuiltinDownload(force = true) }
            ) {
                Text(if (downloading) "Re-downloading all scripts..." else "Re-download all feature scripts")
            }
            Text("Download or refresh one feature script", style = MaterialTheme.typography.labelLarge)
            builtinFeatureFiles.forEach { filename ->
                val installed = File(featureDirectory, filename).isFile
                Row(
                    Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
                ) {
                    Column(Modifier.weight(1f)) {
                        Text(filename)
                        Text(
                            if (installed) "Present in py_updates" else "Not downloaded",
                            style = MaterialTheme.typography.bodySmall
                        )
                    }
                    Button(
                        enabled = !downloading,
                        onClick = {
                            downloading = true
                            downloadStatus = "Downloading $filename..."
                            downloadLogs = emptyList()
                            scope.launch {
                                try {
                                    val response = withContext(Dispatchers.IO) {
                                        service.callAttr(
                                            "install_builtin_feature",
                                            scriptRoot,
                                            filename,
                                            4,
                                            15
                                        ).toString()
                                    }
                                    val result = JSONObject(response)
                                    downloadStatus = if (result.optBoolean("ok")) {
                                        "$filename downloaded. Restart or reload the feature to use it."
                                    } else {
                                        "$filename download failed. Check the download log and retry."
                                    }
                                } catch (error: Exception) {
                                    downloadStatus = "$filename download failed: ${error.message}"
                                } finally {
                                    runCatching { refreshDownloadLogs() }
                                    downloading = false
                                    scriptRevision++
                                    runCatching { rescanFeatureList() }
                                    onRefreshFeatureList()
                                }
                            }
                        }
                    ) {
                        Text(if (installed) "Download again" else "Download")
                    }
                }
            }
            // 添加新 feature：输入裸名（demo -> feature_demo.py），从配置的 URL
            // 根下载到 py_updates，完成即刷新列表。文件名只允许标识符字符。
            Text("Add a feature from the URL root", style = MaterialTheme.typography.labelLarge)
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
            ) {
                OutlinedTextField(
                    value = newFeatureName,
                    onValueChange = {
                        newFeatureName = it.trim().removePrefix("feature_").removeSuffix(".py")
                            .filter { ch -> ch.isLetterOrDigit() || ch == '_' }
                    },
                    modifier = Modifier.weight(1f),
                    singleLine = true,
                    label = { Text("feature_<name>.py") },
                    placeholder = { Text("name") }
                )
                Button(
                    enabled = !downloading && newFeatureName.isNotBlank(),
                    onClick = {
                        val name = newFeatureName.trim()
                        downloading = true
                        downloadStatus = "Downloading feature_$name.py..."
                        downloadLogs = emptyList()
                        scope.launch {
                            try {
                                val response = withContext(Dispatchers.IO) {
                                    service.callAttr("install_named_feature", scriptRoot, name, 4, 20).toString()
                                }
                                val result = JSONObject(response)
                                downloadStatus = if (result.optBoolean("ok")) {
                                    "feature_$name.py downloaded and ready."
                                } else {
                                    "feature_$name.py download failed. Check the download log."
                                }
                            } catch (error: Exception) {
                                downloadStatus = "feature_$name.py download failed: ${error.message}"
                            } finally {
                                runCatching { refreshDownloadLogs() }
                                downloading = false
                                scriptRevision++
                                runCatching { rescanFeatureList() }
                                onRefreshFeatureList()
                            }
                        }
                    }
                ) {
                    Text(if (downloading) "Adding..." else "Add feature")
                }
            }
            if (downloadStatus.isNotBlank()) SelectionContainer {
                Text(downloadStatus, style = MaterialTheme.typography.bodySmall)
            }
            Text("Download log (long-press text to select/copy)", style = MaterialTheme.typography.labelLarge)
            // 下载日志可能含排查 URL/错误，长按可选择复制。
            SelectionContainer {
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
        }
        if (status.isNotBlank()) SelectionContainer { Text(status, style = MaterialTheme.typography.bodySmall) }
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
        if (status.isNotBlank()) SelectionContainer {
            Text(status, style = MaterialTheme.typography.labelMedium)
        }
        LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
            items(candidates) { permission ->
                val granted = ContextCompat.checkSelfPermission(context, permission) == PackageManager.PERMISSION_GRANTED
                Text("${if (granted) "OK" else "--"}  ${permission.removePrefix("android.permission.")}")
            }
        }
    }
}
