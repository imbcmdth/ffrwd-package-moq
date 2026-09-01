# ffrwd/moq

Live publishing over Media over QUIC, hosted in wasm. `publish` is a
COPY destination: the query's streams leave the graph for a relay
broadcast, encoded on the way out.

```pgsql
COPY (
  SELECT f.video[1]
  FROM input('film.mp4') f
) TO ffrwd.moq.publish('moqt://203.0.113.7:4443', 'live/demo')
```

One stream is one track. Gather several and they are one broadcast -
a rendition ladder is `array_agg` over the rungs:

```pgsql
COPY (
  SELECT array_agg(scale(f.video[1], :widths[i.i], -2))
  FROM input(:'source') f, generate_series(1, :rungs) i
) TO ffrwd.moq.publish(:'relay', :'broadcast')
  WITH (video_bitrate :'bitrates'[i.i], gop 30)
```

The compiler places the sink behind an encoder - shape it with the
COPY's `WITH` options, and a value read once per row shapes each
rung's encoder separately. The module packages each stream as
fragmented MP4: one `moof`+`mdat` fragment per group of pictures, one
MoQ frame each, a new MoQ group at every keyframe. Audio rides beside
the video as AAC, cut on frame edges about once a second.

A `catalog.json` track names what the broadcast carries, in the
[hang](https://github.com/kixelated/moq) catalog schema: renditions
keyed by name, each with its codec string, its geometry or sample
rate, and its init segment carried in the entry. A hang player - or
anything speaking that catalog - picks a rendition and plays it; the
versions the claim was proven against are pinned in
`harness/hang-recv`.

The first publish is held until the track gains a subscriber: a MoQ
subscription starts at the latest group, so anything sent earlier
would never be seen. A run with nobody watching waits at the first
packet.

The relay's host is a name or an IP literal. The runner links no name
lookup, so a name is resolved over DNS-over-HTTPS against 1.1.1.1,
then 8.8.8.8 - both pinned by IP in the module - and every address
the answer carries is tried in turn. A private relay's certificate
travels as the `cert` argument, DER as hex - a public value, not a
secret; left empty, the webpki roots baked into the module decide,
which is what a public relay's certificate chains to. One row leaves
per published group - packets, bytes, pts range - and a summary
follows the last.

## License

This package is **MIT OR Apache-2.0**, and so is everything vendored
into it (`web-transport-quinn`, `quinn-udp`, `quinn-wasi` - each
carried for wasm32-wasip2 fixes upstream does not ship yet).

## Exports

- `publish(v, relay, broadcast, track DEFAULT 'video', cert DEFAULT '')`
  returns `sink`: a COPY destination, nothing comes back. `v` is
  `video_stream[]` - every video stream the SELECT carries.
- `publish_av(v, a, relay, broadcast, track DEFAULT 'video',
  audio_track DEFAULT 'audio', cert DEFAULT '')` - the same, plus one
  audio stream.

## Recipes

- `publish` - a file's first video track to a relay.
- `publish-live` - the same shape from a live source URL; the run ends
  when the source does.
- `publish-ladder` - a rendition ladder, one broadcast.
- `publish-ladder-audio` - the ladder with the file's audio beside it.

```
ffrwd ffrwd.moq.publish-ladder -v source=film.mp4 -v relay=moqt://203.0.113.7:4443 -v broadcast=live/demo -v rungs=3 -v widths=1920,1280,854 -v bitrates=6000k,3000k,1000k
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
