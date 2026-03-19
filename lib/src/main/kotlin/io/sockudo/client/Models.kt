package io.sockudo.client

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

enum class SockudoTransport { ws, wss }

enum class ConnectionState {
    INITIALIZED,
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
    UNAVAILABLE,
    FAILED,
}

enum class DeltaAlgorithm { fossil, xdelta3 }

sealed class AuthValue {
    abstract val stringValue: String

    data class Text(val value: String) : AuthValue() {
        override val stringValue: String = value
    }

    data class Integer(val value: Int) : AuthValue() {
        override val stringValue: String = value.toString()
    }

    data class Decimal(val value: Double) : AuthValue() {
        override val stringValue: String = value.toString()
    }

    data class Flag(val value: Boolean) : AuthValue() {
        override val stringValue: String = if (value) "true" else "false"
    }
}

data class ChannelDeltaSettings(
    val enabled: Boolean? = null,
    val algorithm: DeltaAlgorithm? = null,
) {
    fun subscriptionValue(): Any =
        when {
            enabled == null && algorithm != null -> algorithm.name
            enabled == false && algorithm == null -> false
            enabled == true && algorithm == null -> true
            else -> buildMap {
                enabled?.let { put("enabled", it) }
                algorithm?.let { put("algorithm", it.name) }
            }
        }
}

data class SubscriptionOptions(
    val filter: FilterNode? = null,
    val delta: ChannelDeltaSettings? = null,
)

data class DeltaOptions(
    val enabled: Boolean? = null,
    val algorithms: List<DeltaAlgorithm> = listOf(DeltaAlgorithm.fossil, DeltaAlgorithm.xdelta3),
    val debug: Boolean = false,
    val onStats: ((DeltaStats) -> Unit)? = null,
    val onError: ((Throwable) -> Unit)? = null,
)

data class ChannelDeltaStats(
    val channelName: String,
    val conflationKey: String?,
    val conflationGroupCount: Int,
    val deltaCount: Int,
    val fullMessageCount: Int,
    val totalMessages: Int,
)

data class DeltaStats(
    val totalMessages: Int,
    val deltaMessages: Int,
    val fullMessages: Int,
    val totalBytesWithoutCompression: Int,
    val totalBytesWithCompression: Int,
    val bandwidthSaved: Int,
    val bandwidthSavedPercent: Double,
    val errors: Int,
    val channelCount: Int,
    val channels: List<ChannelDeltaStats>,
)

data class PresenceMember(
    val id: String,
    val info: Any?,
)

data class SockudoOptions(
    val cluster: String,
    val activityTimeout: Duration = 120.seconds,
    val forceTls: Boolean? = null,
    val enabledTransports: List<SockudoTransport>? = null,
    val disabledTransports: List<SockudoTransport>? = null,
    val wsHost: String? = null,
    val wsPort: Int = 80,
    val wssPort: Int = 443,
    val wsPath: String = "",
    val httpHost: String? = null,
    val httpPort: Int = 80,
    val httpsPort: Int = 443,
    val httpPath: String = "/pusher",
    val pongTimeout: Duration = 30.seconds,
    val unavailableTimeout: Duration = 10.seconds,
    val enableStats: Boolean = false,
    val statsHost: String = "stats.pusher.com",
    val timelineParams: Map<String, AuthValue> = emptyMap(),
    val channelAuthorization: ChannelAuthorizationOptions = ChannelAuthorizationOptions(),
    val userAuthentication: UserAuthenticationOptions = UserAuthenticationOptions(),
    val deltaCompression: DeltaOptions? = null,
)
