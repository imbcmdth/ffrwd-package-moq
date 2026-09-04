# ffrwd/moq

Media over QUIC in both directions, hosted in wasm. `publish` is a COPY
destination: the query's streams leave the graph for a relay broadcast,
encoded on the way out. `subscribe` is a FROM relation: a broadcast
arrives as rows, one per rendition, and the query does what it likes
with them.

```pgsql
COPY (
  SELECT f.video[1]
  FROM input('film.mp4') f
) TO ffrwd.moq.publish('moqt://203.0.113.7:4443', 'live/demo')
```

The relation IS the broadcast: one row is one rendition, and a
rendition ladder is one row per rung, not an array gathered into one:

```pgsql
COPY (
  SELECT scale(f.video[1], :widths[i.i], -2) AS v
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

## Subscribing

`subscribe` reads that same catalog back. The rows are the renditions,
carrying the rendition columns any manifest input has - `height`,
`width`, `bandwidth`, `codecs`, `name` - so a rung is a `WHERE` and
the widest is an `ORDER BY ... LIMIT 1`:

```pgsql
COPY (
  SELECT s.video[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast') s
  WHERE s.height = 720
) TO 'rung.mp4'
```

The catalog is read at compile time, the way ffprobe reads a file, so
the broadcast must be on the relay before the query compiles. Each
rendition's init segment carries its decoder configuration, and its
fragments are demuxed back to the packets they were built from, so a
rung crosses the graph encoded and a copy stays a copy. The relation
is unbounded: the run lasts as long as the publisher does.

What a broadcast carries decides the shape of a query over it. A
demuxed ladder puts a rung's video and the broadcast's audio on
different rows, so a query wanting both reads each off the rows that
have it; a muxed broadcast carries both on one row and wants a simpler
query. A subscription joins at the latest group, so a subscriber that
attaches late starts at the current one.

Both directions in one query is a relay, and whatever the query does
in between is a relay that transforms what it carries:

```pgsql
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'from') s
  WHERE s.height = 1080
) TO ffrwd.moq.publish(:'relay', :'into')
```

## The relay's address

The relay's host is a name or an IP literal. The runner links no name
lookup, so a name is resolved over DNS-over-HTTPS against 1.1.1.1,
then 8.8.8.8 - both pinned by IP in the module - and every address
the answer carries is tried in turn. A private relay's certificate
travels as the `cert` argument, DER as hex - a public value, not a
secret; left empty, the webpki roots baked into the module decide,
which is what a public relay's certificate chains to. A relay that
demands authentication takes a `token`, sent as the session's request
path - scoped, expiring credentials only, since it lands in the
module's params on the command line. One row leaves per published
group - packets, bytes, pts range - and a summary follows the last.

## License

This package is **MIT OR Apache-2.0**, and so is everything vendored
into it (`web-transport-quinn`, `quinn-udp`, `quinn-wasi` - each
carried for wasm32-wasip2 fixes upstream does not ship yet).

## Exports

- `publish(relay, broadcast, cert DEFAULT '', token DEFAULT '')`
  returns `sink`: a COPY destination, nothing comes back. It reads the
  whole relation - a video cell, an audio cell, either NULL - one
  rendition per row.
- `subscribe(relay, broadcast, cert DEFAULT '', token DEFAULT '')`
  returns `source`: a FROM relation, one row per rendition of the
  broadcast's catalog, a video cell and an audio cell, either NULL.
  Unbounded.

## Recipes

- `publish` - a file's first video track to a relay.
- `publish-live` - the same shape from a live source URL; the run ends
  when the source does.
- `publish-ladder` - a rendition ladder plus the file's audio, one
  broadcast.
- `republish-ladder` - an HLS or DASH ladder read back and republished
  as one MoQ broadcast, rendition for rendition.
- `subscribe` - one rung of a broadcast to a file, video and audio.
- `subscribe-widest` - the same rung ranked rather than picked.
- `relay` - a broadcast copied under a new name, rendition for
  rendition.

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

The live pub/sub loops under `tests/` need moq-relay on the machine,
and wasmtime for the ones that drive a guest directly. They run only
when invoked directly.
