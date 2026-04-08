# sockudo-kotlin

Official Kotlin client for Sockudo.

`sockudo-kotlin` is a Pusher-compatible realtime client for Android and JVM applications. It preserves the familiar subscribe/bind/channel model while adding Sockudo-native features such as filter-aware subscriptions, delta reconstruction, and encrypted channel handling.

## Features

- WebSocket-based `SockudoClient` built on OkHttp
- Public, private, presence, and encrypted channels
- Channel authorization and user authentication
- Client events on private channels
- Filter-aware subscriptions
- Fossil and Xdelta3/VCDIFF delta reconstruction
- User sign-in and watchlist event handling
- Continuity-aware connection recovery and subscribe-time rewind on Protocol V2
- Live integration tests against Sockudo on `127.0.0.1:6001`
- Gradle CI and Maven publication workflow

## Installation

Add the dependency once published:

```kotlin
dependencies {
    implementation("io.sockudo:sockudo-kotlin:0.1.0")
}
```

For local development from this workspace:

```bash
./gradlew :lib:publishToMavenLocal
```

## Quick Start

```kotlin
import io.sockudo.client.SockudoClient
import io.sockudo.client.SockudoOptions
import io.sockudo.client.SockudoTransport

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
        ),
    )

val channel = client.subscribe("public-updates")
channel.bind("price-updated") { data, _ ->
    println(data)
}

client.connect()
```

## Advanced Usage

### Channel Auth

```kotlin
import io.sockudo.client.*

val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            forceTls = false,
            wsHost = "127.0.0.1",
            wsPort = 6001,
            channelAuthorization =
                ChannelAuthorizationOptions(
                    customHandler =
                        ChannelAuthorizationHandler { request ->
                            ChannelAuthorizationData(
                                auth = "signed-auth-token",
                                channelData = """{"user_id":"42"}""",
                            )
                        },
                ),
        ),
    )
```

### Filters and Delta Compression

```kotlin
val channel =
    client.subscribe(
        "price:btc",
        SubscriptionOptions(
            filter = Filter.eq("market", "spot"),
            delta = ChannelDeltaSettings(enabled = true, algorithm = DeltaAlgorithm.xdelta3),
        ),
)
```

### Recovery And Rewind

```kotlin
val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            protocolVersion = 2,
            forceTls = false,
            wsHost = "127.0.0.1",
            wsPort = 6001,
            connectionRecovery = true,
        ),
    )

val channel =
    client.subscribe(
        "market:BTC",
        SubscriptionOptions(rewind = SubscriptionRewind.Seconds(30)),
    )

channel.bind("message") { _, _ ->
    println(client.getRecoveryPosition("market:BTC"))
}

client.bind("sockudo:resume_success") { data, _ ->
    println(data)
}

channel.bind("sockudo:rewind_complete") { data, _ ->
    println(data)
}
```

### Encrypted Channels

`private-encrypted-*` channels use the `shared_secret` returned by your auth handler. Payload decryption is handled automatically.

## Testing

Standard tests:

```bash
./gradlew :lib:test
```

Live integration tests against a local Sockudo on port `6001`:

```bash
SOCKUDO_LIVE_TESTS=1 ./gradlew :lib:test --tests io.sockudo.client.LiveIntegrationTest
```

The live suite covers:

- public subscribe + publish round-trip
- delta-enabled channel delivery
- encrypted channel decryption

## Publishing

This package is configured for Maven-style publishing.

```bash
./gradlew :lib:publishToMavenLocal
./gradlew :lib:publish
```

GitHub Actions:

- CI: `.github/workflows/ci.yml`
- Publish: `.github/workflows/publish.yml`

## Status

The client currently covers the core Sockudo feature set used by the official JS and Swift clients, including encrypted channels and both supported delta algorithms.
