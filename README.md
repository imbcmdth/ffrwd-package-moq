# ffrwd/moq

Media over QUIC in both directions, hosted in wasm. `publish` is a COPY
destination: the query's streams leave the graph for a relay broadcast,
encoded on the way out. `subscribe` is a FROM relation: a broadcast
arrives as rows, one per rendition, and the query does what it likes
with them.

## Publish

```pgsql
COPY (
  SELECT f.video[1]
  FROM input('film.mp4') f
) TO ffrwd.moq.publish('moqt://203.0.113.7:4443', 'live/demo')
```

The relation IS the broadcast: one row is one rendition, and a
rendition ladder is one row per rung:

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
fragmented MP4: one `moof`+`mdat` fragment per each frame, a new MoQ
group at every keyframe. Audio rides beside the video as AAC, cut on
frame edges.

### What a run says

`rows` decides. `'summary'`, the default, is one row per track every
five seconds - `track`, `groups`, `packets`, `bytes`, and `media`, the
seconds of media that have gone out on it, which is what says whether
a live run is keeping up - and one trailing row over the whole
broadcast (`tracks`, `groups`, `packets`, `bytes`, `init_bytes`).
`'groups'` is the older shape, a row per published group (`track`,
`group`, `packets`, `bytes`, `pts_start`, `pts_end`): ten a second for
audio alone, so it is for piping somewhere rather than for watching.
`'none'` leaves the trailing row alone.

### Groups

A group is what a relay forwards and what a subscriber joins at. Video
opens one at every keyframe, since a decoder can start nowhere else.
Audio has no keyframe - every AAC frame is a sync sample - so where it
is cut is a choice, and the choice is a delay: a relay forwards a
group once it is whole, and a player reading at the live edge holds
what it has until the group it is reading ends. A group that spans a
second is a second of sound arriving at once, with the picture beside
it running ahead.

`audio_group_ms` is that duration. The default is `100`, five AAC
frames at 48 kHz: short enough that nothing waits on it, long enough
that a run of groups still arrives one at a time. Before 0.6.2 the
rule was a fixed second, which is what the stutter was.

`0` gives every frame a group of its own. That is what upstream hang's
own publisher writes, and against a real relay it is **not yet sound**
here: a player reading such a broadcast skips about one audio group in
four. The shape is several groups appended and finished inside one
host call - a subscription's latency window defaults to zero, so a
group that is no longer the latest when a reader reaches it is skipped
rather than served, and groups written back to back give a reader that
chance. A local relay does not reproduce it: publishing a paced
twenty seconds through `moq-relay` on this machine, 939 groups
published and 939 received, at `0` as at `100`. So `0` is offered,
documented and not recommended until that is understood.

Tracks carry hang's own delivery priorities, higher sent first:
`catalog.json` at 100, audio at 80, video at 60. They break the tie on
a session whose subscriber - a relay reading the whole broadcast -
asks for every track alike, so a video keyframe cannot sit in the send
queue in front of a sound.

A `catalog.json` track names what the broadcast carries, in the
[hang](https://github.com/kixelated/moq) catalog schema: renditions
keyed by name, each with its codec string, its geometry or sample
rate, and its init segment carried in the entry. A hang player picks a rendition and plays it; the
versions the claim was proven against are pinned in
`harness/hang-recv`.

The first publish is held until the track gains a subscriber: a MoQ
subscription starts at the latest group, so anything sent earlier
would never be seen. A run with nobody watching waits at the first
packet.

## Subscribe

`subscribe` reads that same hang catalog. The rows are the renditions,
carrying the rendition columns any manifest input has - `height`,
`width`, `bandwidth`, `codecs`, `name`:

```pgsql
COPY (
  SELECT s.video[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast') s
  WHERE s.height = 720
) TO 'rung.mp4'
```

The catalog is read at compile time, the way ffprobe reads a file, so
the broadcast must be on the relay before the query compiles.

What a broadcast carries decides the shape of a query over it. A
demuxed ladder puts a rung's video and the broadcast's audio on
different rows; a muxed broadcast carries both on one row.

## Relay

Both directions in one query is a relay, and whatever the query does
in between is a relay that transforms:

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
the answer carries is tried in turn.

A relay that demands authentication takes a `token`, sent as the
session's request path. The relay URL's own path is that same field, so
the address a relay hands out with the token already in it -
`https://relay.example/<JWT>`, which is the form a hang player takes -
works as written:

```pgsql
COPY (...) TO ffrwd.moq.publish('https://relay.example/<JWT>', 'live/demo')
```

Either spelling alone wins, and they mean the same session. Naming the
same token in both is fine; naming different ones is refused before
anything is dialed, in a message that shows each token's first and last
four characters and no more. Where the path is a broadcast root rather
than a credential, put the token in the query instead: a `?jwt=` is
carried to the relay, which is where a relay reads one from. A
`?token=` is refused, since no relay reads that and the session would
go out unauthenticated with a token in hand, and so is a `#` fragment,
which reaches no relay at all. Nothing else in the URL is dropped on
the way.

## What the shared crates changed

The fmp4 and h264 layers under this package are
[ffrwd-bmff](https://github.com/imbcmdth/ffrwd-bmff) and
[ffrwd-nal](https://github.com/imbcmdth/ffrwd-nal), which several ffrwd
packages share. What stays here is what MoQ decides rather than what a
container says: the group discipline, the hang catalog, the relay. Most
of the move is invisible from the outside. These parts are not.

**The `avcC` is one byte shorter, and it is now ffmpeg's own record.**
ffmpeg's H.264 demuxer pads the SPS in its Annex-B extradata with a
`trailing_zero_8bits`, and the scanner this package used to carry gave
that byte to the SPS. So every record published before this carried a
27-byte SPS where ffmpeg's own MP4 muxer writes 26. The byte is padding
and no decoder reads it, but the record was not ffmpeg's. It is now,
which moves one byte in every published init segment and two hex digits
in every catalog entry's `description`. `core/tests/reference.rs` pins
the new record against the one ffmpeg wrote into the test fixture. For
that fixture the video init segment goes from 669 bytes to 668, and
every byte outside the record and the box sizes around it is where it
was.

**An audio init segment names its own brand.** The `ftyp` compatible
brands are `iso5 isom <sample entry> mp41`, so an AAC track's now read
`iso5 isom mp4a mp41` where they used to say `avc1`, which was the
video entry's name on an audio-only track. Four bytes, and nothing
else in the audio init segment moves.

**What a reader accepts has widened.** A box of declared size zero,
which a muxer writing into a pipe uses because it does not yet know the
length, runs to the end of the stream instead of being refused. A `moof`
carrying several `traf` boxes is read, and the track fragments that are
not this track's are skipped: a fragment with nothing for the track
yields no packets and does not end the subscription. A fragment with no
`tfdt` is timed from zero rather than refused. The `1 << 28` per-box
refusal is gone; where this package scans a byte stream it takes
ffrwd-bmff's own bound of 64 MiB for a box that must be held whole,
which is far past the single-sample fragments it publishes.

**A record that cannot be spelled is refused rather than truncated.**
More than 31 SPS, more than 255 PPS, or a parameter set past 65535
bytes used to be written as a wrapped count or length. None of them
comes off a real encoder, and all of them are now named.

**A wire timestamp can move by one microsecond.** Ticks round to
nearest on the way to microseconds where they used to truncate.

**A failure says less and points better.** The shared crates carry no
formatted strings on the error path, so a message no longer quotes the
presentation time or the size that caused it, and carries the byte
offset instead. This package puts back what it alone knows, at its own
call sites: which track, and which packet.

## License

This package is **MIT OR Apache-2.0**, and so is everything vendored
into it (`web-transport-quinn`, `quinn-udp`, `quinn-wasi` - each
carried for wasm32-wasip2 fixes upstream does not ship yet).

## Exports

- `publish(relay, broadcast, cert DEFAULT '', token DEFAULT '',
  audio_group_ms DEFAULT 100, rows DEFAULT 'summary')` returns `sink`: a COPY destination,
  nothing comes back. It reads the whole relation - a video cell, an
  audio cell, either NULL - one rendition per row.
- `subscribe(relay, broadcast, cert DEFAULT '', token DEFAULT '')`
  returns `source`: a FROM relation, one row per rendition of the
  broadcast's catalog, a video cell and an audio cell, either NULL.
  Unbounded.

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
