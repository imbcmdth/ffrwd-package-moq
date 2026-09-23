# ffrwd/moq

Media over QUIC in both directions, hosted in wasm. `publish` is a COPY
destination: the query's streams leave the graph for a relay broadcast,
encoded on the way out. `subscribe` is a FROM relation: a broadcast
arrives as rows, one per rendition, and the query does what it likes
with them.

Needs ffrwd 0.18.1 or later. Before it, sound that passed through a
module left the sidecar seconds at a time, and a live broadcast
published from such a query lost most of its audio.

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

A track row also carries what the session looked like over the window:
`appended` and `closed` (groups opened on the track and finished -
they differ only by the one still open), `sub_latency_ms`,
`sub_priority` and `sub_ordered` (what the relay is actually asking
for on that track, `-1` when nobody is subscribed), and `gap_max_ms`
and `call_max_ms` (the longest the QUIC session lay undriven, and the
longest one call held it: the session runs only while a host call is
on the executor, so the first is what a loaded machine costs).

### Groups

A group is what a relay forwards and what a subscriber joins at. Video
opens one at every keyframe, since a decoder can start nowhere else.
Audio has no keyframe - every AAC frame is a sync sample - so where it
is cut is a choice, and the choice is a delay: a relay forwards a
group once it is whole, and a player reading at the live edge holds
what it has until the group it is reading ends. A group that spans a
second is a second of sound arriving at once, with the picture beside
it running ahead.

`audio_group_ms` is that duration. The default is `200`, ten AAC
frames at 48 kHz. Before 0.6.2 the rule was a fixed second, which is
what the stutter was; 0.6.2 cut it to `100` and 0.6.3 doubled it
again, for a reason worth writing down.

Through a public relay, on a loaded machine, 100 ms groups lost WHOLE
audio groups: between 0.7% and 6% of them over several captures, one
group at a time, never a run. A player skipped them and a patient
stream-copy subscriber reading the backlog missed the same ones, so
they never came out of the relay to anybody. The publisher's own rows
showed every packet written, every group closed, no starvation of its
session, and nothing in moq-net 0.2.15 destroys a group inside its
retention window. At 200 ms, on the same machine and the same relay,
minutes later: five minutes with nothing skipped, and two ninety
second captures with no gap at all.

Halving the rate of groups should halve a per-group hazard, not end
it, so the hypothesis is a RATE: about 9.4 new audio streams a second
at 100 ms against 4.7 at 200, plus video, and a relay that limits how
fast it will take new group streams. That is a hypothesis, not a
finding - the publisher-side counters that would confirm it are still
being built - so the default is set where the evidence is, and
`audio_group_ms` is there for anyone whose relay is happier. The extra
100 ms is absorbed by the player's own jitter buffer, which this
package already asks for 600 ms of.

`0` gives every frame a group of its own, at about 47 streams a
second. That is what upstream hang's own publisher writes, and against
a real relay it is **not yet sound** here: a player reading such a
broadcast skips about one audio group in four, which is the same
hazard as above an order of magnitude louder. The shape is several groups appended and finished inside one
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

### Where a reader joins

`start` says which end of a running broadcast to join at. It is `'live'`
by default, and that is the answer for anything reading a live stream:

```pgsql
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'from') s
) TO ffrwd.moq.publish(:'relay', :'into')
```

**A live join is one GROUP behind the broadcast.** The publisher serves
a live subscription from its newest group, and the reader starts at the
first group it can decode from:

- **video**, at the first group whose FIRST FRAME is a keyframe. Every
  MoQ group here opens at a keyframe, by this package's convention and
  by hang's, but the join is exactly where trusting that would buy a
  broken picture, so the flag is read off the packets. That puts the
  first picture out of a live join up to one GOP old - 1 second at
  `gop 30` and 30 fps.
- **audio**, at the first whole group, every AAC frame being a sync
  sample. That is `audio_group_ms`, so 200 ms by default.

A ladder joined in one query joins together: every track's subscription
is registered before any of them is waited on, so they all take the same
edge rather than each one a round trip further along. Measured against a
local relay, a session pulling two video rungs and the audio joined all
three within 0.59 seconds of media of each other, every track one group
from the publisher's newest.

**`'backlog'` is the other end**, and what every version before 0.6.5
did: join at the oldest group the relay still holds. That is a retention
window of delay before the first packet comes out, and on a broadcast
that has been running it means reading the whole cache first. It is for
a publisher running AHEAD of real time - a file poured into a relay as
fast as it will take it - and a reader that wants every frame rather
than the newest. It is the capture tool:

```pgsql
COPY (
  SELECT s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', '', '', 'backlog') s
  WHERE s.height IS NULL
) TO 'whole.mkv'
```

A wasm function's arguments are positional, so `start` sits fifth,
before the three hold numbers 0.6.4 added - `subscribe(relay, broadcast,
cert, token, start, hold_ms, hold_mib, join_ms)`. A query written
against 0.6.4 that passed `hold_ms` positionally has to move it along
one.

**What a live subscription does when the reader lags.** moq-net's
`group_start` is "the first group the publisher should deliver, or
`None` to start at the latest group", and `None` is what a live join
sends. The publisher fixes that cursor ONCE, at the group that was
latest when it accepted the subscription (`start_group.or_else(||
track.latest())`), and from there serves every group in arrival order
off that one cursor. A reader that falls behind is never skipped
forward; it loses a group only if the publisher's cache evicts one
before the serving loop reaches it, and that takes `latency_max` of the
group going untouched - 30 seconds, here, both on the wire and in the
publisher's own retention window. So a node that lags catches up, which
is what a node should do. It is a `latency_max` of ZERO - moq-net's own
default, which this package never sends - that "skips a non-latest group
the moment a newer one exists".

Measured: a live reader joined a paced broadcast a minute into it and
was loaded on purpose while it read. It delivered every group from the
join to the end of the broadcast in sequence, with nothing abandoned and
nothing arriving below the cursor. `tests/live_edge.py` is that loop.

### The hold

A subscription asking for the backlog is served in the relay's own
arrival order with the newest group sent first. So the groups arrive
nothing like in sequence: reading two minutes of audio back off a local
relay, this package measured a hold 556 groups deep before the oldest
one landed, and blocks of a dozen groups arriving tens of seconds behind
their neighbours with the track silent in between. What leaves this
module is packets in decode order, which is what `ffrwd:av` promises and
what every stream-copy muxer downstream needs, so the reordering is
absorbed in a HOLD. The same hold runs on a live join, where what it has
to absorb is a group or two rather than hundreds.

The hold waits, and it does not give up on a missing group on a timer.
`hold_ms` (30000 by default, the subscription's own latency window) is
how long a hole stands open before the relay is ASKED for that group by
sequence, not how long before the group is written off. A relay that
still has it serves it and the hole closes; one that does not refuses,
and only that refusal lets the cursor step over the hole. The other
ways out are `hold_mib` (64 by default: the memory one track's hold may
use, which is 30 seconds of about 17 Mbit/s and three orders of
magnitude more than audio needs), a resubscribe after the wire broke,
since the new subscription starts at the live edge and what is missing
is gone, and the end of the track.

`join_ms` (2000 by default) is the other half, and a backlog idea alone.
A cursor fixed at the first group that arrives puts the whole backlog
BELOW itself, and every group of it is then a late arrival with nowhere
to go. So a reader that has just joined a backlog holds what it is given
and fixes its cursor at the lowest sequence that has stopped falling:
each lower group restarts the wait, and group 0 ends it outright. A live
join has nothing older coming and waits for none of it.

A session also has to be able to TAKE a backlog. MoQ opens one QUIC
stream per group, and quinn's default of 100 concurrent streams leaves
a relay queueing the rest in `open_uni`, where a group can wait tens of
seconds for credit the wire had to spare. This package now allows 1024,
which is what moq-native allows for the same reason.

Before 0.6.4 the hold gave a hole three seconds, or 256 held groups,
and then jumped the cursor past it; every older group that arrived
afterwards was dropped where it landed, without a word. A 120 second
backlog is 563 audio groups at the default grouping, well past that
bound.

What that cost, against a local relay, on the same 120 second backlog
read back by a reader that joins mid-publish and one that joins after:

| | mid | after |
| --- | --- | --- |
| 0.6.3 | 1 group lost | 1 group lost |
| 0.6.3's hold, this version's stream window | 41 groups, 7.3% | 25 groups, 4.4% |
| 0.6.4 | none | none |

0.6.3 loses a group at a time because the stream window under it never
let much of the backlog arrive at once. Widen that window alone and the
old hold throws away a twentieth of the broadcast: the bound WAS the
loss. 0.6.4 keeps all of it, 563 of 563 groups and 5626 of 5626 packets
on both readers over three runs, with nothing abandoned and nothing
dropped late. `tests/live_backlog.py` is that measurement, and
`--plain` is how it reads a package older than this one. Since 0.6.5 it
asks for `'backlog'` outright, the default having moved.

One thing the hold cannot put back is a group the relay no longer has.
A backlog join on a relay carrying a LIVE broadcast is reading a
retention window, and the oldest of it can age out while the replay is
still working through it: `tests/live_edge.py` sees one such hole per
track on a 40 second replay off a 30 second window. That is the relay's
answer rather than a defect, and it is named in a HOLE row like any
other. A relay told to keep more (`--cache-duration`) or a publisher
running ahead of real time, which is what `backlog` is for, does not
reach it.

### What a subscriber says

A packet source has no row channel in `ffrwd:av`: `next` hands back
packets and nothing else. So the subscriber's rows go to its own stderr
as `subscribe: row <json>` lines, one object to a line, in the shape
`describe` reports as `rows_schema`. A run under `ffrwd run` keeps that
stderr to itself unless the run failed; `FFRWD_DUMP_STDERR=<dir>`
writes every member's out either way, which is how the loop reads them.

A TRACK row goes out every five seconds and once more when the track
ends (`final`). Per track it carries `received` and `delivered` (groups
the relay handed over, and groups handed on in sequence), `repeated`,
`bytes`, `holes_opened` and `holes_filled`, `holes_abandoned_gone`,
`holes_abandoned_budget`, `holes_abandoned_restart` and
`holes_abandoned_end`, `dropped_late` (groups that arrived below the
cursor, which no consumer taking packets in decode order can be
handed), `skipped_join` (groups held at a live join that began before
the group the reader started at - a video group with no keyframe to
begin in; not a loss, since nothing had started, but not silent
either), `fetches` and `fetches_refused` (groups asked for outright,
and the ones the relay would not serve), `hold_max_groups` and
`hold_max_bytes`, `reorder_max` (the widest the hold ever had to
stretch in sequence), and `first` and `last`.

Beside them, a row per incident: a HOLE row for every hole given up on
(`from`, `to`, `reason`, and what was held at the time), a LATE row for
every group that arrived too late to use, and a SKIPPED row, the same
shape, for every group a live join stepped over on its way to one a
decoder can start at. None of them is ever silent, so
a run that lost something says which groups by sequence. The first 256
of each per track are spelled out and the rest are only counted.

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
  audio_group_ms DEFAULT 200, rows DEFAULT 'summary')` returns `sink`: a COPY destination,
  nothing comes back. It reads the whole relation - a video cell, an
  audio cell, either NULL - one rendition per row.
- `subscribe(relay, broadcast, cert DEFAULT '', token DEFAULT '',
  start DEFAULT 'live', hold_ms DEFAULT 30000, hold_mib DEFAULT 64,
  join_ms DEFAULT 2000)`
  returns `source`: a FROM relation, one row per rendition of the
  broadcast's catalog, a video cell and an audio cell, either NULL.
  Unbounded. `start` is which end of a running broadcast to join at,
  `'live'` or `'backlog'`; the three numbers are the hold the groups are
  put back in order in. See above.

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
