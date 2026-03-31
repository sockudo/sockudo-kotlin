package io.sockudo.client

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.MediaType.Companion.toMediaType
import okio.ByteString.Companion.toByteString
import java.net.URI
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class SockudoClient(
    val key: String,
    val options: SockudoOptions,
    internal val httpClient: OkHttpClient = OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build(),
) {
    init {
        require(key.isNotBlank()) { throw SockudoException.InvalidAppKey }
        require(options.cluster.isNotBlank()) {
            throw SockudoException.InvalidOptions("Options must provide a cluster.")
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    internal val p = ProtocolPrefix(options.protocolVersion)
    internal val config = ResolvedConfiguration(options, httpClient)
    private val dispatcher = EventDispatcher()
    private val channels = linkedMapOf<String, SockudoChannel>()
    private var webSocket: WebSocket? = null
    private var activityJob: Job? = null
    private var unavailableJob: Job? = null
    private var retryJob: Job? = null
    private var currentTransport: SockudoTransport? = null
    private var attemptedFallback: Boolean = false
    private var manuallyDisconnected: Boolean = false
    private val deltaManager: DeltaCompressionManager? =
        options.deltaCompression?.let { deltaOptions ->
            DeltaCompressionManager(deltaOptions, { event, data ->
                sendEvent(event, data, null)
            }, p)
        }
    private val deduplicator: MessageDeduplicator? =
        if (options.messageDeduplication) MessageDeduplicator(options.messageDeduplicationCapacity) else null
    private val channelSerials = linkedMapOf<String, Int>()

    val user = UserFacade()
    val watchlist = WatchlistFacade()

    var connectionState: ConnectionState = ConnectionState.INITIALIZED
        private set

    var socketId: String? = null
        private set

    val shouldUseTls: Boolean
        get() = config.useTls

    init {
        user.attach(this)
        watchlist.attach(this)
    }

    fun on(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken =
        dispatcher.bind(eventName, callback)

    fun bind(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken = on(eventName, callback)

    fun onGlobal(callback: (String, Any?) -> Unit): EventBindingToken = dispatcher.bindGlobal(callback)

    fun bindGlobal(callback: (String, Any?) -> Unit): EventBindingToken = onGlobal(callback)

    fun off(eventName: String? = null, token: EventBindingToken? = null) {
        dispatcher.unbind(eventName, token)
    }

    fun unbind(eventName: String? = null, token: EventBindingToken? = null) = off(eventName, token)

    fun unbindAll() {
        dispatcher.unbind()
    }

    fun channel(name: String): SockudoChannel? = channels[name]

    fun allChannels(): List<SockudoChannel> = channels.values.sortedBy { it.name }

    fun subscribe(channelName: String, options: SubscriptionOptions? = null): SockudoChannel {
        val channel = channels.getOrPut(channelName) { createChannel(channelName) }
        options?.let {
            channel.filter = it.filter
            channel.deltaSettings = it.delta
        }
        channel.subscribeIfPossible()
        return channel
    }

    fun subscribe(channelName: String, filter: FilterNode): SockudoChannel =
        subscribe(channelName, SubscriptionOptions(filter = filter))

    fun unsubscribe(channelName: String) {
        val channel = channels[channelName]
        when {
            channel == null -> return
            channel.subscriptionPending -> channel.subscriptionCancelled = true
            channel.isSubscribed -> {
                channels.remove(channelName)
                channel.unsubscribe()
            }

            else -> channels.remove(channelName)
        }
        channelSerials.remove(channelName)
        deltaManager?.clearChannelState(channelName)
    }

    fun connect() {
        if (webSocket != null) {
            return
        }
        val transports = transportSequence()
        if (transports.isEmpty()) {
            updateState(ConnectionState.FAILED)
            return
        }
        manuallyDisconnected = false
        attemptedFallback = false
        updateState(ConnectionState.CONNECTING)
        openWebSocket(transports.first())
        setUnavailableTimer()
    }

    fun disconnect() {
        manuallyDisconnected = true
        invalidateTimers()
        webSocket?.close(1000, null)
        webSocket = null
        currentTransport = null
        channels.values.forEach { it.disconnect() }
        updateState(ConnectionState.DISCONNECTED)
    }

    fun signIn() {
        user.signIn()
    }

    fun getDeltaStats(): DeltaStats? = deltaManager?.getStats()

    fun resetDeltaStats() {
        deltaManager?.resetStats()
    }

    fun close() {
        disconnect()
        scope.cancel()
    }

    internal fun launchSubscription(block: suspend () -> Unit) {
        scope.launch { block() }
    }

    internal fun sendEvent(name: String, data: Any?, channel: String?): Boolean {
        val socket = webSocket ?: return false
        val payload = linkedMapOf<String, Any?>(
            "event" to name,
            "data" to data,
        )
        channel?.let { payload["channel"] = it }
        return when (val encoded = ProtocolCodec.encodeEnvelope(payload, options.wireFormat)) {
            is String -> socket.send(encoded)
            is ByteArray -> socket.send(encoded.toByteString())
            else -> false
        }
    }

    private fun subscribeAll() {
        channels.values.forEach { it.subscribeIfPossible() }
    }

    private fun createChannel(name: String): SockudoChannel =
        when {
            name.startsWith("private-encrypted-") -> EncryptedChannel(name, this)
            name.startsWith("presence-") -> PresenceChannel(name, this)
            name.startsWith("private-") -> PrivateChannel(name, this)
            name.startsWith("#") -> {
                SockudoLogger.error("Cannot create a channel with name '$name'")
                SockudoChannel(name, this)
            }

            else -> SockudoChannel(name, this)
        }

    private fun openWebSocket(transport: SockudoTransport) {
        currentTransport = transport
        val url = socketUrl(transport)
        val request = Request.Builder().url(url).build()
        webSocket =
            httpClient.newWebSocket(
                request,
                object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: Response) = Unit

                    override fun onMessage(webSocket: WebSocket, text: String) {
                        handleRawMessage(text)
                    }

                    override fun onMessage(webSocket: WebSocket, bytes: okio.ByteString) {
                        handleRawMessage(bytes)
                    }

                    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                        dispatcher.emit("error", t)
                        handleSocketClosed(1006, t.message)
                    }

                    override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                        handleSocketClosed(code, reason)
                    }
                },
            )
    }

    private fun handleRawMessage(rawMessage: Any) {
        try {
            val event = decodeEvent(rawMessage)
            if (event.messageId != null && deduplicator != null) {
                if (deduplicator.isDuplicate(event.messageId)) {
                    return
                }
                deduplicator.track(event.messageId)
            }
            resetActivityTimer()
            // Track serial per channel for connection recovery
            if (options.connectionRecovery && event.channel != null && event.serial != null) {
                channelSerials[event.channel] = event.serial
            }

            val eventName = event.event
            when {
                eventName == p.event("connection_established") -> {
                    val payload = event.data as? Map<*, *> ?: throw SockudoException.InvalidHandshake
                    val newSocketId = payload["socket_id"] as? String ?: throw SockudoException.InvalidHandshake
                    socketId = newSocketId
                    val negotiatedTimeout = ((payload["activity_timeout"] as? Number)?.toDouble()
                        ?: config.activityTimeout.inWholeSeconds.toDouble()) * 1000.0
                    config.activityTimeout =
                        minOf(config.activityTimeout.inWholeMilliseconds.toDouble(), negotiatedTimeout).toLong()
                            .milliseconds()
                    clearUnavailableTimer()
                    updateState(ConnectionState.CONNECTED, mapOf("socket_id" to newSocketId))
                    subscribeAll()
                    if (options.connectionRecovery && channelSerials.isNotEmpty()) {
                        sendEvent(
                            p.event("resume"),
                            JsonSupport.encode(mapOf("channel_serials" to channelSerials)),
                            null,
                        )
                    }
                    if (options.deltaCompression?.enabled == true) {
                        deltaManager?.enable()
                    }
                    user.handleConnected()
                }

                eventName == p.event("error") -> dispatcher.emit("error", event.data)
                eventName == p.event("ping") -> sendEvent(p.event("pong"), emptyMap<String, Any>(), null)
                eventName == p.event("pong") -> Unit
                eventName == p.event("signin_success") -> user.handleSignInSuccess(event.data)
                eventName == p.internal_("watchlist_events") -> watchlist.handle(event.data)
                eventName == p.event("delta_compression_enabled") -> {
                    deltaManager?.handleEnabled(event.data)
                    dispatcher.emit(eventName, event.data)
                }

                eventName == p.event("delta_cache_sync") -> {
                    event.channel?.let { channelName ->
                        deltaManager?.handleCacheSync(channelName, event.data)
                    }
                }

                eventName == p.event("delta") -> {
                    event.channel?.let { channelName ->
                        val reconstructed = deltaManager?.handleDeltaMessage(channelName, event.data)
                        if (reconstructed != null) {
                            channels[channelName]?.handle(reconstructed)
                            dispatcher.emit(reconstructed.event, reconstructed.data)
                        }
                    }
                }

                eventName == p.event("resume_success") -> {
                    SockudoLogger.debug("Connection recovery succeeded", event.data)
                }

                eventName == p.event("resume_failed") -> {
                    val failData = event.data as? Map<*, *>
                    val failedChannelName = failData?.get("channel") as? String
                    if (failedChannelName != null) {
                        channelSerials.remove(failedChannelName)
                        SockudoLogger.warn("Connection recovery failed for channel", failedChannelName)
                        channels[failedChannelName]?.subscribeIfPossible()
                    }
                }

                else -> {
                    event.channel?.let { channelName ->
                        channels[channelName]?.handle(event)
                        if (!p.isPlatformEvent(eventName) &&
                            !p.isInternalEvent(eventName) &&
                            event.sequence != null
                        ) {
                            deltaManager?.handleFullMessage(
                                channel = channelName,
                                rawMessage = stripDeltaMetadata(rawMessage),
                                sequence = event.sequence,
                                conflationKey = event.conflationKey,
                            )
                        }
                    }
                    if (!p.isInternalEvent(eventName)) {
                        dispatcher.emit(eventName, event.data, EventMetadata(event.userId))
                    }
                }
            }
        } catch (error: Throwable) {
            dispatcher.emit("error", error)
        }
    }

    private fun decodeEvent(rawMessage: Any): SockudoEvent = ProtocolCodec.decodeEvent(rawMessage, options.wireFormat)

    private fun stripDeltaMetadata(rawMessage: Any): String =
        (rawMessage as? String ?: decodeEvent(rawMessage).rawMessage)
            .replace(Regex(""","__delta_seq":\d+"""), "")
            .replace(Regex(""""__delta_seq":\d+,"""), "")
            .replace(Regex(""","__conflation_key":"[^"]*"""), "")
            .replace(Regex(""""__conflation_key":"[^"]*","""), "")

    private fun handleSocketClosed(code: Int, reason: String?) {
        invalidateActivityTimer()
        clearUnavailableTimer()
        webSocket = null
        channels.values.forEach { it.disconnect() }

        when (closeAction(code)) {
            CloseAction.TlsOnly -> {
                config.useTls = true
                scheduleRetry(Duration.ZERO)
            }

            CloseAction.Backoff -> scheduleRetry(1.seconds)
            CloseAction.Retry -> scheduleRetry(Duration.ZERO)
            CloseAction.Refused -> updateState(ConnectionState.DISCONNECTED)
            null -> if (!manuallyDisconnected) {
                scheduleRetry(1.seconds)
            }
        }

        if (!reason.isNullOrBlank()) {
            dispatcher.emit("error", SockudoException.ConnectionUnavailable)
            SockudoLogger.warn("Socket closed", code, reason)
        }
    }

    private fun closeAction(code: Int): CloseAction? =
        when {
            code < 4000 -> if (code in 1002..1004) CloseAction.Backoff else null
            code == 4000 -> CloseAction.TlsOnly
            code < 4100 -> CloseAction.Refused
            code < 4200 -> CloseAction.Backoff
            code < 4300 -> CloseAction.Retry
            else -> CloseAction.Refused
        }

    private fun socketUrl(transport: SockudoTransport): String {
        val scheme = if (transport == SockudoTransport.wss) "wss" else "ws"
        val host = config.wsHost
        val port = if (transport == SockudoTransport.wss) config.wssPort else config.wsPort
        val path = "${config.wsPath}/app/$key"
        val query = listOf(
            "protocol=${p.version}",
            *(if (options.protocolVersion >= 2) arrayOf("format=${options.wireFormat.queryValue}") else emptyArray()),
            "client=kotlin",
            "version=0.1.0",
            "flash=false",
        ).joinToString("&")
        return URI(scheme, null, host, port, path, query, null).toString()
    }

    private fun transportSequence(): List<SockudoTransport> {
        var transports =
            if (config.useTls) listOf(SockudoTransport.wss) else listOf(SockudoTransport.ws, SockudoTransport.wss)
        config.enabledTransports?.let { enabled ->
            transports = transports.filter { it in enabled }
        }
        config.disabledTransports?.let { disabled ->
            transports = transports.filterNot { it in disabled }
        }
        return transports
    }

    private fun sendPing() {
        sendEvent(p.event("ping"), emptyMap<String, Any>(), null)
        invalidateActivityTimer()
        activityJob =
            scope.launch {
                delay(config.pongTimeout)
                scheduleRetry(Duration.ZERO)
            }
    }

    private fun resetActivityTimer() {
        invalidateActivityTimer()
        activityJob =
            scope.launch {
                delay(config.activityTimeout)
                sendPing()
            }
    }

    private fun invalidateActivityTimer() {
        activityJob?.cancel()
        activityJob = null
    }

    private fun setUnavailableTimer() {
        clearUnavailableTimer()
        unavailableJob =
            scope.launch {
                delay(config.unavailableTimeout)
                updateState(ConnectionState.UNAVAILABLE)
            }
    }

    private fun clearUnavailableTimer() {
        unavailableJob?.cancel()
        unavailableJob = null
    }

    private fun scheduleRetry(after: Duration) {
        if (manuallyDisconnected) {
            return
        }
        retryJob?.cancel()
        retryJob =
            scope.launch {
                delay(after)
                webSocket?.cancel()
                webSocket = null
                updateState(ConnectionState.CONNECTING)
                val transports = transportSequence()
                if (currentTransport == SockudoTransport.ws && !attemptedFallback && transports.contains(
                        SockudoTransport.wss
                    )
                ) {
                    attemptedFallback = true
                    openWebSocket(SockudoTransport.wss)
                } else {
                    attemptedFallback = false
                    openWebSocket(transports.firstOrNull() ?: SockudoTransport.wss)
                }
                setUnavailableTimer()
            }
    }

    private fun invalidateTimers() {
        invalidateActivityTimer()
        clearUnavailableTimer()
        retryJob?.cancel()
        retryJob = null
    }

    private fun updateState(state: ConnectionState, metadata: Map<String, Any?>? = null) {
        val previous = connectionState
        connectionState = state
        dispatcher.emit(
            "state_change",
            mapOf("previous" to previous.name.lowercase(), "current" to state.name.lowercase())
        )
        dispatcher.emit(state.name.lowercase(), metadata)
    }

    class UserFacade {
        private var client: SockudoClient? = null
        private val dispatcher = EventDispatcher { event, _ ->
            SockudoLogger.debug("No callbacks on user for $event")
        }

        var isSignInRequested: Boolean = false
            private set
        var userData: Map<String, Any?>? = null
            private set
        val userId: String?
            get() = userData?.get("id") as? String
        private var serverChannel: SockudoChannel? = null

        internal fun attach(client: SockudoClient) {
            this.client = client
        }

        fun on(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken =
            dispatcher.bind(eventName, callback)

        fun signIn() {
            isSignInRequested = true
            attemptSignIn()
        }

        internal fun handleConnected() {
            attemptSignIn()
        }

        internal fun handleSignInSuccess(data: Any?) {
            val payload = data as? Map<*, *> ?: run {
                cleanup()
                return
            }
            val userDataString = payload["user_data"] as? String ?: run {
                cleanup()
                return
            }
            val parsed = JsonSupport.fromJsonElement(JsonSupport.decode(userDataString)) as? Map<String, Any?> ?: run {
                cleanup()
                return
            }
            val userId = parsed["id"] as? String
            if (userId.isNullOrBlank()) {
                cleanup()
                return
            }
            userData = parsed
            subscribeServerChannel(userId)
        }

        private fun attemptSignIn() {
            val client = client ?: return
            if (!isSignInRequested || client.connectionState != ConnectionState.CONNECTED) {
                return
            }
            val socketId = client.socketId ?: return
            client.scope.launch {
                runCatching {
                    client.config.userAuthenticator.authenticate(UserAuthenticationRequest(socketId))
                }.onSuccess { auth ->
                    client.sendEvent(
                        client.p.event("signin"),
                        mapOf("auth" to auth.auth, "user_data" to auth.userData),
                        null,
                    )
                }.onFailure {
                    cleanup()
                }
            }
        }

        private fun subscribeServerChannel(userId: String) {
            val client = client ?: return
            val channel = SockudoChannel("#server-to-user-$userId", client)
            channel.onGlobal { eventName, data ->
                if (!client.p.isInternalEvent(eventName) && !client.p.isPlatformEvent(eventName)) {
                    dispatcher.emit(eventName, data)
                }
            }
            serverChannel = channel
            channel.subscribeIfPossible()
        }

        private fun cleanup() {
            userData = null
            serverChannel?.unbindAll()
            serverChannel?.disconnect()
            serverChannel = null
        }
    }

    class WatchlistFacade {
        private val dispatcher = EventDispatcher { event, _ ->
            SockudoLogger.debug("No callbacks on watchlist for $event")
        }

        fun on(eventName: String, callback: (Any?, EventMetadata?) -> Unit): EventBindingToken =
            dispatcher.bind(eventName, callback)

        internal fun attach(client: SockudoClient) = Unit

        internal fun handle(data: Any?) {
            val payload = data as? Map<*, *> ?: return
            val events = payload["events"] as? List<*> ?: return
            events.filterIsInstance<Map<*, *>>().forEach { event ->
                val name = event["name"] as? String ?: return@forEach
                dispatcher.emit(name, event)
            }
        }
    }

    internal class ResolvedConfiguration(
        options: SockudoOptions,
        private val httpClient: OkHttpClient,
    ) {
        val cluster: String = options.cluster
        var activityTimeout: Duration = options.activityTimeout
        var useTls: Boolean = options.forceTls != false
        val wsHost: String = options.wsHost ?: "ws-${options.cluster}.sockudo.io"
        val wsPort: Int = options.wsPort
        val wssPort: Int = options.wssPort
        val wsPath: String = options.wsPath
        val httpHost: String = options.httpHost ?: "sockjs-${options.cluster}.sockudo.io"
        val httpPort: Int = options.httpPort
        val httpsPort: Int = options.httpsPort
        val httpPath: String = options.httpPath
        val pongTimeout: Duration = options.pongTimeout
        val unavailableTimeout: Duration = options.unavailableTimeout
        val enableStats: Boolean = options.enableStats
        val statsHost: String = options.statsHost
        val timelineParams: Map<String, AuthValue> = options.timelineParams
        val enabledTransports: List<SockudoTransport>? = options.enabledTransports
        val disabledTransports: List<SockudoTransport>? = options.disabledTransports
        val channelAuthorizer: ChannelAuthorizationHandler =
            options.channelAuthorization.customHandler ?: makeChannelAuthorizer(options.channelAuthorization)
        val userAuthenticator: UserAuthenticationHandler =
            options.userAuthentication.customHandler ?: makeUserAuthenticator(options.userAuthentication)

        private fun makeChannelAuthorizer(options: ChannelAuthorizationOptions): ChannelAuthorizationHandler =
            ChannelAuthorizationHandler { request ->
                performAuthRequest(
                    endpoint = options.endpoint,
                    headers = options.headers + (options.headersProvider?.invoke() ?: emptyMap()),
                    params =
                        options.params +
                                (options.paramsProvider?.invoke() ?: emptyMap()) +
                                mapOf(
                                    "socket_id" to AuthValue.Text(request.socketId),
                                    "channel_name" to AuthValue.Text(request.channelName),
                                ),
                    parse = { json ->
                        val auth = json["auth"] as? String
                            ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                        ChannelAuthorizationData(
                            auth = auth,
                            channelData = json["channel_data"] as? String,
                            sharedSecret = json["shared_secret"] as? String,
                        )
                    },
                )
            }

        private fun makeUserAuthenticator(options: UserAuthenticationOptions): UserAuthenticationHandler =
            UserAuthenticationHandler { request ->
                performAuthRequest(
                    endpoint = options.endpoint,
                    headers = options.headers + (options.headersProvider?.invoke() ?: emptyMap()),
                    params =
                        options.params +
                                (options.paramsProvider?.invoke() ?: emptyMap()) +
                                mapOf("socket_id" to AuthValue.Text(request.socketId)),
                    parse = { json ->
                        val auth = json["auth"] as? String
                            ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                        val userData = json["user_data"] as? String
                            ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                        UserAuthenticationData(auth, userData)
                    },
                )
            }

        private suspend fun <T> performAuthRequest(
            endpoint: String,
            headers: Map<String, String>,
            params: Map<String, AuthValue>,
            parse: (Map<String, Any?>) -> T,
        ): T {
            val request =
                Request.Builder()
                    .url(endpoint)
                    .post(QueryString.encode(params).toRequestBody("application/x-www-form-urlencoded".toMediaType()))
                    .apply {
                        headers.forEach { (name, value) -> addHeader(name, value) }
                    }
                    .build()

            val response = httpClient.newCall(request).execute()
            response.use {
                if (!it.isSuccessful) {
                    throw SockudoException.AuthFailure(
                        it.code,
                        "Could not get auth info from endpoint, status: ${it.code}",
                    )
                }
                val body = it.body?.string()
                    ?: throw SockudoException.AuthFailure(it.code, "Auth endpoint returned an empty body")
                val parsed = JsonSupport.fromJsonElement(JsonSupport.decode(body)) as? Map<String, Any?>
                    ?: throw SockudoException.AuthFailure(200, "JSON returned from auth endpoint was invalid")
                return parse(parsed)
            }
        }
    }

    private enum class CloseAction {
        TlsOnly,
        Refused,
        Backoff,
        Retry,
    }
}

private fun Long.milliseconds(): Duration = Duration.parse("${this}ms")
