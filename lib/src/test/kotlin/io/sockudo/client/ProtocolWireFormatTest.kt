package io.sockudo.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class ProtocolWireFormatTest {
    @Test
    fun encodesWebsocketUrlWithV2FormatQuery() {
        val client =
            SockudoClient(
                "app-key",
                SockudoOptions(
                    cluster = "local",
                    forceTls = false,
                    enabledTransports = listOf(SockudoTransport.ws),
                    wsHost = "ws.example.com",
                    wsPort = 6001,
                    wssPort = 6002,
                    wireFormat = SockudoWireFormat.messagepack,
                ),
            )

        val method = SockudoClient::class.java.getDeclaredMethod("socketUrl", SockudoTransport::class.java)
        method.isAccessible = true
        val url = method.invoke(client, SockudoTransport.ws) as String

        assertEquals("2", java.net.URI(url).query.split("&").associate {
            val (key, value) = it.split("=", limit = 2)
            key to value
        }["protocol"])
        assertEquals("messagepack", java.net.URI(url).query.split("&").associate {
            val (key, value) = it.split("=", limit = 2)
            key to value
        }["format"])
    }

    @Test
    fun roundTripsMessagepack() {
        val payload =
            ProtocolCodec.encodeEnvelope(
                linkedMapOf(
                    "event" to "sockudo:test",
                    "channel" to "chat:room-1",
                    "data" to linkedMapOf("hello" to "world", "count" to 3),
                    "__delta_seq" to 7,
                    "__conflation_key" to "room",
                ),
                SockudoWireFormat.messagepack,
            )

        val decoded = ProtocolCodec.decodeEvent(payload, SockudoWireFormat.messagepack)

        assertEquals("sockudo:test", decoded.event)
        assertEquals("chat:room-1", decoded.channel)
        assertEquals(mapOf("hello" to "world", "count" to 3L), decoded.data)
        assertEquals(7, decoded.sequence)
        assertEquals("room", decoded.conflationKey)
    }

    @Test
    fun roundTripsProtobuf() {
        val payload =
            ProtocolCodec.encodeEnvelope(
                linkedMapOf(
                    "event" to "sockudo:test",
                    "channel" to "chat:room-1",
                    "data" to linkedMapOf("hello" to "world"),
                    "__delta_seq" to 11,
                    "__conflation_key" to "btc",
                    "extras" to
                        linkedMapOf(
                            "headers" to linkedMapOf("region" to "eu", "ttl" to 5, "replay" to true),
                            "echo" to false,
                        ),
                ),
                SockudoWireFormat.protobuf,
            )

        val decoded = ProtocolCodec.decodeEvent(payload, SockudoWireFormat.protobuf)

        assertEquals("sockudo:test", decoded.event)
        assertEquals("chat:room-1", decoded.channel)
        assertEquals(mapOf("hello" to "world"), decoded.data)
        assertEquals(11, decoded.sequence)
        assertEquals("btc", decoded.conflationKey)
        assertEquals("eu", decoded.extras?.headers?.get("region"))
        assertEquals(5.0, decoded.extras?.headers?.get("ttl"))
        assertEquals(true, decoded.extras?.headers?.get("replay"))
        assertFalse(decoded.extras?.echo ?: true)
    }
}
