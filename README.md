# ffrwd/moq

Live publishing over Media over QUIC, hosted in wasm. `publish` is a
COPY destination: the query's video stream leaves the graph for a
relay broadcast, encoded on the way out.

```pgsql
COPY (
  SELECT f.video[1]
  FROM input('film.mp4') f
) TO ffrwd.moq.publish('moqt://203.0.113.7:4443', 'live/demo')
```

The compiler places the sink behind an encoder - shape it with the
COPY's `WITH` options, h264 - and the module packages the packets as
fragmented MP4: an init segment built from the stream's SPS/PPS,
published on its own track (`<track>.init`) so a late subscriber
always reads the decoder config first, then one `moof`+`mdat`
fragment per group of pictures, one MoQ frame each, a new MoQ group
at every keyframe. Any fmp4-speaking MoQ subscriber can reassemble
and play it.

The first publish is held until the track gains a subscriber: a MoQ
subscription starts at the latest group, so anything sent earlier
would never be seen. A run with nobody watching waits at the first
packet.

The relay's host is a name or an IP literal. The runner links no name
lookup, so a name is resolved over DNS-over-HTTPS against 1.1.1.1,
then 8.8.8.8 - both pinned by IP in the module - and every address
the answer carries is tried in turn. A private relay's certificate
travels as the `cert` argument, DER as hex - a public value, not a
secret; left
empty, the webpki roots baked into the module decide, which is what a
public relay's certificate chains to. One row leaves per published
group - packets, bytes, pts range - and a summary follows the last.

## License

This package is **MIT OR Apache-2.0**, and so is everything vendored
into it (`web-transport-quinn`, `quinn-udp`, `quinn-wasi` - each
carried for wasm32-wasip2 fixes upstream does not ship yet).

## Export

- `publish(v, relay, broadcast, track DEFAULT 'video', cert DEFAULT '')`
  returns `sink`: a COPY destination, nothing comes back.

## Recipes

- `publish` - a file's first video track to a relay.
- `publish-live` - the same shape from a live source URL; the run ends
  when the source does.

```
ffrwd ffrwd.moq.publish -v source=film.mp4 -v relay=moqt://203.0.113.7:4443 -v broadcast=live/demo
```

## Building

The module builds against the wit from the installed `ffrwd/wasm`
package, and ring's C sources want wasi-sdk's clang:

```
ffrwd install -g ffrwd/wasm
CC_wasm32_wasip2=<wasi-sdk>/bin/clang cargo build --target wasm32-wasip2 --release
```

The live pub/sub loop under `tests/` needs moq-relay and wasmtime on
the machine and runs only when invoked directly.
