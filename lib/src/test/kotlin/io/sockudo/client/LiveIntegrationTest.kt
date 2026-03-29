package io.sockudo.client

import com.iwebpp.crypto.TweetNaclFast
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import java.util.concurrent.atomic.AtomicReference
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody

class LiveIntegrationTest {
    private val httpClient = OkHttpClient()

    @Test
    fun localSockudoConnectsAndReceivesPublishedEvent() =
        runBlocking {
            if (!liveTestsEnabled()) {
                return@runBlocking
            }

            val connected = AtomicReference(false)
            val subscribed = AtomicReference(false)
            val received = AtomicReference<Map<*, *>?>()

            val client =
                SockudoClient(
                    "app-key",
                    SockudoOptions(
                        cluster = "local",
                        forceTls = false,
                        enabledTransports = listOf(SockudoTransport.ws),
                        wsHost = "127.0.0.1",
                        wsPort = 6001,
                        wssPort = 6001,
                        wireFormat = liveWireFormat(),
                    ),
                )

            val channel = client.subscribe("public-updates")
            client.bind("connected") { _, _ -> connected.set(true) }
            channel.bind("sockudo:subscription_succeeded") { _, _ -> subscribed.set(true) }
            channel.bind("integration-event") { data, _ -> received.set(data as? Map<*, *>) }

            client.connect()
            waitFor { connected.get() }
            waitFor { subscribed.get() }

            publishToLocalSockudo(
                channel = "public-updates",
                eventName = "integration-event",
                payload =
                    mapOf(
                        "message" to "hello from kotlin",
                        "item_id" to "kotlin-client",
                        "padding" to "x".repeat(140),
                    ),
            )

            val payload = waitForValue { received.get() }
            assertEquals("hello from kotlin", payload["message"])
            client.disconnect()
            client.close()
        }

    @Test
    fun localSockudoAppliesDeltaCompressedMessages() =
        runBlocking {
            if (!liveTestsEnabled()) {
                return@runBlocking
            }

            val connected = AtomicReference(false)
            val subscribed = AtomicReference(false)
            val prices = mutableListOf<Long>()

            val client =
                SockudoClient(
                    "app-key",
                    SockudoOptions(
                        cluster = "local",
                        forceTls = false,
                        enabledTransports = listOf(SockudoTransport.ws),
                        wsHost = "127.0.0.1",
                        wsPort = 6001,
                        wssPort = 6001,
                        wireFormat = liveWireFormat(),
                        deltaCompression = DeltaOptions(enabled = true),
                    ),
                )

            val channel =
                client.subscribe(
                    "price:integration",
                    SubscriptionOptions(delta = ChannelDeltaSettings(enabled = true)),
                )
            client.bind("connected") { _, _ -> connected.set(true) }
            channel.bind("sockudo:subscription_succeeded") { _, _ -> subscribed.set(true) }
            channel.bind("price-updated") { data, _ ->
                val map = data as? Map<*, *> ?: return@bind
                prices += (map["price"] as Number).toLong()
            }

            client.connect()
            waitFor { connected.get() }
            waitFor { subscribed.get() }

            publishToLocalSockudo(
                channel = "price:integration",
                eventName = "price-updated",
                payload =
                    mapOf(
                        "item_id" to "delta-item",
                        "price" to 101,
                        "padding" to "y".repeat(180),
                    ),
            )
            publishToLocalSockudo(
                channel = "price:integration",
                eventName = "price-updated",
                payload =
                    mapOf(
                        "item_id" to "delta-item",
                        "price" to 102,
                        "padding" to "y".repeat(180),
                    ),
            )

            waitFor { prices.contains(102L) }
            val stats = waitForValue { client.getDeltaStats()?.takeIf { it.totalMessages >= 2 } }
            assertNotNull(stats)
            assertEquals(102L, prices.last())
            client.disconnect()
            client.close()
        }

    @Test
    fun localSockudoDecryptsEncryptedChannelPayloads() =
        runBlocking {
            if (!liveTestsEnabled()) {
                return@runBlocking
            }

            val connected = AtomicReference(false)
            val subscribed = AtomicReference(false)
            val received = AtomicReference<Map<*, *>?>()
            val sharedSecret = ByteArray(32) { (it + 1).toByte() }

            val client =
                SockudoClient(
                    "app-key",
                    SockudoOptions(
                        cluster = "local",
                        forceTls = false,
                        enabledTransports = listOf(SockudoTransport.ws),
                        wsHost = "127.0.0.1",
                        wsPort = 6001,
                        wssPort = 6001,
                        wireFormat = liveWireFormat(),
                        channelAuthorization =
                            ChannelAuthorizationOptions(
                                customHandler =
                                    ChannelAuthorizationHandler { request ->
                                        ChannelAuthorizationData(
                                            auth = "app-key:${
                                                hmacSha256(
                                                    "${request.socketId}:${request.channelName}",
                                                    "app-secret"
                                                )
                                            }",
                                            sharedSecret = Base64.getEncoder().encodeToString(sharedSecret),
                                        )
                                    },
                            ),
                    ),
                )

            val channel = client.subscribe("private-encrypted-live")
            client.bind("connected") { _, _ -> connected.set(true) }
            channel.bind("sockudo:subscription_succeeded") { _, _ -> subscribed.set(true) }
            channel.bind("encrypted-event") { data, _ -> received.set(data as? Map<*, *>) }

            client.connect()
            waitFor { connected.get() }
            waitFor { subscribed.get() }

            val encryptedPayload =
                encryptPayload(
                    payload = """{"message":"secret hello","item_id":"enc-item"}""",
                    secret = sharedSecret,
                )

            publishRawToLocalSockudo(
                channel = "private-encrypted-live",
                eventName = "encrypted-event",
                payload = encryptedPayload,
            )

            val payload = waitForValue { received.get() }
            assertEquals("secret hello", payload["message"])
            client.disconnect()
            client.close()
        }

    private suspend fun waitFor(timeoutMs: Long = 8000, condition: () -> Boolean) {
        waitForValue(timeoutMs) { condition().takeIf { it } }
    }

    private suspend fun <T> waitForValue(timeoutMs: Long = 8000, supplier: () -> T?): T {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            supplier()?.let { return it }
            delay(50)
        }
        error("Timed out waiting for value")
    }

    private fun liveTestsEnabled(): Boolean = System.getenv("SOCKUDO_LIVE_TESTS") == "1"

    private fun liveWireFormat(): SockudoWireFormat =
        when (System.getenv("SOCKUDO_WIRE_FORMAT")?.lowercase()) {
            "messagepack", "msgpack" -> SockudoWireFormat.messagepack
            "protobuf", "proto" -> SockudoWireFormat.protobuf
            else -> SockudoWireFormat.json
        }

    private fun publishToLocalSockudo(channel: String, eventName: String, payload: Map<String, Any>) {
        val bodyObject =
            mapOf(
                "name" to eventName,
                "channels" to listOf(channel),
                "data" to JsonSupport.encode(payload),
            )
        publishBody(bodyObject)
    }

    private fun publishRawToLocalSockudo(channel: String, eventName: String, payload: Map<String, String>) {
        val bodyObject =
            mapOf(
                "name" to eventName,
                "channels" to listOf(channel),
                "data" to JsonSupport.encode(payload),
            )
        publishBody(bodyObject)
    }

    private fun publishBody(bodyObject: Map<String, Any>) {
        val path = "/apps/app-id/events"
        val body = JsonSupport.encode(bodyObject)
        val bodyMd5 = md5Hex(body.toByteArray(StandardCharsets.UTF_8))
        val timestamp = Instant.now().epochSecond.toString()
        val params =
            linkedMapOf(
                "auth_key" to "app-key",
                "auth_timestamp" to timestamp,
                "auth_version" to "1.0",
                "body_md5" to bodyMd5,
            )
        val canonicalQuery = params.entries.sortedBy { it.key }.joinToString("&") { "${it.key}=${it.value}" }
        val signature = hmacSha256("POST\n$path\n$canonicalQuery", "app-secret")
        val url = "http://127.0.0.1:6001$path?$canonicalQuery&auth_signature=$signature"

        val request =
            Request
                .Builder()
                .url(url)
                .post(body.toRequestBody("application/json".toMediaType()))
                .build()

        httpClient.newCall(request).execute().use { response ->
            check(response.code == 200 || response.code == 202) {
                "Unexpected status code ${response.code}"
            }
        }
    }

    private fun encryptPayload(payload: String, secret: ByteArray): Map<String, String> {
        val nonce = ByteArray(TweetNaclFast.SecretBox.nonceLength) { (it * 3 + 7).toByte() }
        val cipherText = TweetNaclFast.SecretBox(secret).box(payload.toByteArray(StandardCharsets.UTF_8), nonce)
        return mapOf(
            "ciphertext" to Base64.getEncoder().encodeToString(cipherText),
            "nonce" to Base64.getEncoder().encodeToString(nonce),
        )
    }

    private fun md5Hex(data: ByteArray): String = digestHex("MD5", data)

    private fun hmacSha256(value: String, secret: String): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(secret.toByteArray(StandardCharsets.UTF_8), "HmacSHA256"))
        return mac.doFinal(value.toByteArray(StandardCharsets.UTF_8)).joinToString("") { "%02x".format(it) }
    }

    private fun digestHex(algorithm: String, data: ByteArray): String =
        MessageDigest.getInstance(algorithm).digest(data).joinToString("") { "%02x".format(it) }
}
