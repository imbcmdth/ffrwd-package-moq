# ffrwd/moq

Media over QUIC in both directions, hosted in wasm. `publish` is a COPY
destination: the query's streams leave the graph for a relay broadcast,
encoded on the way out. `subscribe` is a FROM relation: a broadcast
arrives as rows, one per rendition, and the query does what it likes
with them.

Requires ffrwd 0.29, whose `ffrwd/wasm` is 0.19.1.

Both are nodes of `ffrwd:av` 0.19.1, built on
[ffrwd-node](https://github.com/imbcmdth/ffrwd-node). "On ffrwd 0.29"
says what that changed and how it was checked.

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

Each rendition's catalog entry states its `bitrate`, hang's own field,
when its encoder is set to one: `video_bitrate` and `audio_bitrate`, in
bits a second. A reader picks a rung by it, as a player does or an
egress worker reading `rung: {under: 6M}`. A rung encoded to a quality
(`crf`) states none.

A subscription starts at the latest group, so the first media is held
for a first subscriber, up to `hold_s` seconds (10 by default), to keep
a file's opening from being lost to a reader that arrives a moment
late. A live source loses nothing by starting at once: `hold_s => 0`
publishes the first media straight away, which saves a head nobody
watches yet those ten seconds on every start.

### What a run says

`rows` decides. `'summary'`, the default, is one row per track every
five seconds - `track`, `groups`, `packets`, `bytes`, and `media`, the
seconds of media that have gone out on it, which is what says whether
a live run is keeping up - and one trailing row over the whole
broadcast (`tracks`, `groups`, `packets`, `bytes`, `init_bytes`, and
`queue_max`, `wait_max_ms` and `unpaced` over the whole run).
`'groups'` is the older shape, a row per published group (`track`,
`group`, `packets`, `bytes`, `pts_start`, `pts_end`): ten a second for
audio alone, so it is for piping somewhere rather than for watching.
`'none'` leaves the trailing row alone.

A track row also carries what the session looked like over the window:
`appended` and `closed` (groups put on the wire and finished there -
they differ only by the one still open, and `groups` runs ahead of
both by what the track is holding back), `queued`, `queue_max`,
`wait_max_ms` and `unpaced` (groups held back now, the most held at
once, the longest one was held, and how many went before the one
ahead of them was known delivered; see "One group at a time"),
`sub_latency_ms`,
`sub_priority` and `sub_ordered` (what the relay is actually asking
for on that track, `-1` when nobody is subscribed), and `gap_max_ms`
and `call_max_ms` (the longest the QUIC session lay undriven, and the
longest one call held it: the session runs only while a host call is
on the executor, so the first is what a loaded machine costs, or on a
host that calls only as packets arrive, how far apart they do).

A text track's row also counts what it made of its cue rows: `cues`
(finished cues written), `growing` (rows of an utterance still growing,
passed over), `clipped` (cues written from where the track had got,
since they arrived after part of them had gone out; counted in `cues`
too), `late` (cues that ended before the track had got that far, which
are dropped) and `empty` (rows with no span, no text or that do not
read). The trailing row carries the same five over every text track. See
"Text tracks".

A session the relay dropped is replaced rather than the run ended; see
"A dropped session" below. The publisher says so in a row of its own,
whatever `rows` asks for: `event` is `reconnect`, with `attempts` (the
dials it took), `down_ms` (from the old session ending to the new one
being in) and `error` (what ended the old one).

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

Since 0.7.2 the cause is better understood (see "One group at a time"),
and `0` is still not sound, for a new reason: a group a frame long is
shorter than the round trip it has to wait for. **`audio_group_ms` must
be longer than the round trip to the relay plus a few tens of
milliseconds**, and about twice the round trip to keep the queue short.
Below that the track's queue fills to its bound and groups go
unacknowledged anyway, which the rows count as `unpaced` rather than
letting the audio fall behind real time. Measured with
`tests/live_pacing.py`, which puts a round trip between the publisher
and a local relay:

| `audio_group_ms` | round trip | held at once | longest held | unpaced |
| --- | --- | --- | --- | --- |
| 200 | 20 ms | 1 | 126 ms | 0 |
| 200 | 40 ms | 1 | 124 ms | 0 |
| 200 | 80 ms | 1 | 158 ms | 0 |
| 100 | 40 ms | 2 | 157 ms | 0 |
| 50 | 20 ms | 4 | 219 ms | 0 |
| 0 | 20 ms | 8 | 294 ms | 2480 of 4492 |
| 0 | 40 ms | 8 | 295 ms | 3070 of 4502 |

The held columns leave out the first ten seconds of each run, which hold
the backlog from the wait for a first reader; `unpaced` is the whole
run. No row fell behind real time.

Tracks carry hang's own delivery priorities, higher sent first:
`catalog.json` at 100, audio at 80, video at 60, and a data track and a
text track, which hang has no number for, at 90 and 85 (see "Data
tracks" and "Text tracks"). They break the tie on
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

### One group at a time

A relay built on moq-transport's `serve` model (the IETF stack in
cloudflare/moq-rs, which Cloudflare's relay runs) keeps only a track's
LATEST group, with no cache behind it, and forwards whichever group is
latest when a subscriber's task next wakes. A group overtaken by the
next one on its track before then is gone for good, and a fetch for it
answers `not found`. On Cloudflare's draft-16 relay that lost the first
of two messages written in one call five times in seven. Audio lost
groups the same way: about one in four at a frame per group, groups out
of a burst after a stalled call, and one group exactly where a pair of
messages had held a call.

So every track's groups leave one at a time, audio, video and data
alike. A group goes onto the wire once the one before it on its track
has been delivered (read to its end by the session, its last byte
acknowledged by the relay) and 10 ms have passed since that one
finished. Until then it waits in its track's queue, gathering its
frames, and the call that cut it returns without waiting for it. The
next call lets it go, first thing, once the acknowledgement is in. A
group whose predecessor was acknowledged already goes out at once, as
before, and a track nobody is subscribed to holds nothing back.

Two bounds keep a queue from holding its track up. A group goes after
250 ms behind an unacknowledged one regardless: a stalled session or a
distant relay must not stop the track, and every millisecond held is a
millisecond behind live. And a track holds at most 8 groups: one more
sends the oldest on at once. Either way the group went without the
protection, and the rows count it in `unpaced`.

The session runs only inside a host call, so a held group goes out from
a later call. Since 0.7.3 a call with no packets in it is a TURN: the
session runs, what the queues may let go goes onto the socket, and the
call returns. As a node, publish keeps time by a rate of 50 ticks a
second, so a tick that brings nothing is a turn and a held group goes out
within 20 ms of the acknowledgement it waits on, however its packets
arrive; its last tick comes once every input has ended.

That matters on a LEAF, a query reading a broadcast and publishing it
again. Its packets arrive the way the upstream groups do, whole: a GOP of
picture and the sound beside it, about a second apart. A host that calls
only as packets arrive leaves the session still for that second, and on
0.7.2 every group queued behind an unacknowledged one waited for it, well
past the cap, and went unpaced. Measured with `tests/live_pacing.py
--shape leaf` (a head publishing to a local relay, and a leaf reading it
and publishing again through a 40 ms round trip), over the windows after
the join:

| | 0.7.2, ffrwd 0.21.0 | 0.7.3, calls with no packets |
| --- | --- | --- |
| longest the session lay still | 1045 to 1164 ms | 33 to 36 ms |
| longest call | 377 to 505 ms | 41 to 48 ms |
| audio: held at once, longest held | 8, 2281 ms | 5 to 6, 378 ms |
| audio: unpaced | 164 | 0 |
| video: longest held, unpaced | 1171 ms, 87 | 160 ms, 0 |
| data: longest held, unpaced | 184 ms, 0 | 125 ms, 0 |

A second of sound is five groups cut at once, so on a leaf they still
queue behind each other: each goes one round trip, the 10 ms and at most
one turn after the one before it, about 65 ms apart here. The last of a
lump is held about a third of a second, and none goes unpaced.

With a host that makes no turns, once the media has been quiet for
250 ms a call waits for its own queue instead, as every call did before
0.7.2, since it has no media to hold up.

Before 0.7.2 only a data track was paced, and the wait sat in the call:
a pair of messages held the whole call, media included, for a round
trip. A leaf publishing to Cloudflare showed it as `call_max_ms` rising
from 38-49 ms to 57-91 ms around each break. Measured with
`tests/live_pacing.py` (video, audio at 200 ms, pairs of messages every
2.5 seconds, a round trip put between the publisher and a local relay),
the longest call per five second window, over the 90 seconds after the
join:

| round trip | 0.7.1 median | 0.7.1 max | 0.7.2 median | 0.7.2 max |
| --- | --- | --- | --- | --- |
| 20 ms | 31.5 ms | 72 ms | 31 ms | 39 ms |
| 40 ms | 48 ms | 97 ms | 38 ms | 42 ms |
| 80 ms | 110 ms | 202 ms | 35.5 ms | 46 ms |

What is left is the three short turns a call takes after writing a
message. What the queue costs instead is a round trip on the group that
waits, and the relay's own acknowledgement delay and the time to the
next call on top: at those round trips a video group (a GOP, a second
here) was held at most 94, 126 and 157 ms, once, at its start, and ran
live after. Neither track fell behind real time in any run: the reader
saw each packet come out no later against its pts at the end than at
the start. The median delay from pts to the reader moved by between
-10 and +27 ms.

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

A row's `bandwidth` is its catalog `bitrate`: its video's, or its
audio's on a row with no video, and null where the catalog states none.

The catalog is read at compile time, the way ffprobe reads a file, so
the broadcast must be on the relay before the query compiles: the node's
shape is the catalog, a packets output per media track and a data output
per data track and per text track, grouped into relation rows, and only the outputs the
query reads are subscribed to. A shape that sees no announce within 15 seconds asks again on a new session
before it refuses the query: Cloudflare's relay has been seen not to
announce a broadcast that was up to a fresh session, and a second one
took. A broadcast that is not there is refused within 30 seconds.

What a broadcast carries decides the shape of a query over it. A
demuxed ladder puts a rung's video and the broadcast's audio on
different rows; a muxed broadcast carries both on one row.

### A broadcast from OBS

The moq-dev OBS plugin publishes through libmoq, in hang's `legacy`
container rather than CMAF: every frame is a QUIC varint of the
sample's pts in microseconds and then the sample itself, an H.264
access unit in Annex B (`avc3`, its parameter sets in the keyframes) or
a raw AAC frame. Its catalog names the tracks `1.avc3` and `0.aac`, and
carries no init segment. Since 0.8.0 `subscribe` reads that, and takes
a legacy broadcast as one programme: its audio sits on its video's row,
so OBS reads as one row.

```pgsql
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', 'live/obs', '', :'token') s
) TO 'obs.mkv'
```

Legacy video is H.264, HEVC in Annex B (`hev1`) or AV1, and legacy
audio is AAC. Opus and FLAC, which the plugin can also pick, are
refused by name: ffrwd's packet wire does not carry them yet. An `avc1`
or `avc3` entry that carries an `avcC` description frames its pictures
length-prefixed, as WebCodecs reads it, and is reframed into Annex B on
the way through. The container carries no decode time, so a packet's
dts is its pts, and an encoder that reorders pictures (B-frames) is
refused at its first reordered picture: set the encoder's B-frames to 0.
A keyframe is read off the picture's own units: an IDR slice for H.264,
an IRAP picture for HEVC, a sequence header for AV1. A video frame that
carries a pts and no sample, which libmoq sends now and then, is passed
over.

libmoq writes its catalog once, when it makes its tracks. Cloudflare's
draft-16 relay keeps no history and answers FETCH with "not supported",
and a subscription used to start at the object after the relay's newest
one, so a reader that joined after OBS started never saw the catalog at
all ("sent no catalog within 15s"). This package's moq-net now asks a
draft-15 or later relay for everything from `{0, 0}`. Cloudflare answers
with the newest group it has, from its first object: the catalog, and a
video group from its keyframe. A relay holding a backlog would answer
with all of it, so a live join drops every group older than the newest
one SUBSCRIBE_OK names. `tests/live_obs.py` reads a broadcast someone is
publishing and checks every track decodes.

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

Since 0.9.2 the hold takes a group a frame at a time, and the group at
its cursor goes out the same way: each frame is handed on as it
arrives, and the cursor moves past the group once its end has been
read. A frame is whole on its own, so nothing waits for the rest of its
group. A group behind a hole, or behind the group still being read,
waits whole as before. See "Latency".

The hold waits, and it does not give up on a missing group on a timer.
`hold_ms` is how long a hole stands open before the relay is ASKED for
that group by sequence, not how long before the group is written off.
Left out, it follows `start`: 30000 on a backlog join, the
subscription's own latency window, and 1000 on a live join. A live
reader has given up the past on purpose, and every group behind a hole
waits on it: in a demo tree one audio group the relay never delivered
held 67 groups behind it for 26 seconds, and the leaf's encoder, waiting
on its sound, stopped painting. A relay that
still has it serves it and the hole closes; one that does not refuses,
and only that refusal lets the cursor step over the hole. The other
ways out are `hold_mib` (64 by default: the memory one track's hold may
use, which is 30 seconds of about 17 Mbit/s and three orders of
magnitude more than audio needs, and held to as a burst fills the hold
since 0.9.2, where before a burst could take it past), a resubscribe
after the wire broke, since the new subscription starts at the live
edge and what is missing is gone, and the end of the track.

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

### Latency

Before 0.9.2 a subscriber handed a video packet on only once the whole
group it belonged to had arrived. A group is a GOP, so the first picture of
each group waited a GOP and the last none: packets left in batches of a
GOP, and their latency was a sawtooth a GOP high. The foundmeat ingest
proof measured that as "about 300 ms in a sawtooth, batches of about 12":
its gateway cut a group at every keyframe, and x264 put a keyframe every
9 to 13 frames there. The pull's drive loop (up to 200 sleeps of a
millisecond once it had something) was not what made the batches: it
ended once five turns passed with no group arriving, 5 to 80 ms.

0.9.2 changes three things:

- The hold takes a group a frame at a time, and the group at its cursor
  goes out the same way (see "The hold"). A frame is a whole `moof`+`mdat`
  fragment, a legacy sample or a message, so nothing waits for the rest of
  its group.
- A pull returns as soon as it has packets. It lets the reader tasks and
  the QUIC driver run once first, which takes in what the wire brought
  while the host had the call, and waits on them only while it has
  nothing to hand on. The host calls a source again as soon as a call
  returns: `gap_max_ms` on the track rows, the longest the session lay
  undriven between two pulls, stayed under 80 ms in the Windows runs
  below, with one 239 ms while a `backlog` join was being written out.
- The socket reactor under the session sweeps the sockets once a
  millisecond, and a datagram that lands between sweeps waits for the next
  one. Under wasmtime on Windows a one millisecond sleep lasts 15.6 ms. So
  where a 1 ms sleep measures over 4 ms, the module has the reactor wait in
  `wasi:io/poll` on its sockets instead, and a datagram is seen when the
  host has it. On Linux, where a sleep lasts what it asks for, nothing
  changes.

Measured against a native reader (moq-native, the ingest proof's probe)
on the same relay at the same time: each frame's arrival at a query reading
`ffrwd.moq.subscribe` into a NUT pipe, less its arrival at the native
reader, over 60 seconds of a paced 1080p broadcast with a keyframe a
second. Windows is the 9950X with the sidecar of ffrwd 0.29.4; Linux is an
i7-8750H in a container of foundmeat's runner image (ffrwd 0.32.5).

| | fps | 0.9.1 | 0.9.2 |
| --- | --- | --- | --- |
| Windows | 60 | 592 / 994 / 1091 | 1.0 / 1.4 / 15 |
| Windows | 30 | 623 / 1013 / 1377 | 1.1 / 1.6 / 14 |
| Linux | 60 | 522 / 921 / 1010 | 3.2 / 4.9 / 15 |
| Linux | 30 | 523 / 920 / 1004 | 3.7 / 5.4 / 21 |

Median, p90 and max, in ms. Before, the median frame was half a GOP
late and the worst a whole one; after, a frame leaves subscribe within a
few milliseconds of the native reader having it. On Linux the reactor
still sweeps every millisecond, which is most of what is left there.

By the ingest proof's own method (its `tools/latency.py`, the clock in the
picture, SRT at 200 ms, a subscriber writing MPEG-TS over UDP), on
Windows: 779 ms before and 515 after at 30 fps, against 446 for its
native reader on the same relay, and 563 before and 365 after at 60 fps,
against 348. What is left, 69 ms at 30 fps and 17 at 60, is one or two
frame intervals on the way out through MPEG-TS: the proof found its
MPEG-TS reader alone holds one, 33 ms at 30 fps, since it knows a picture
is whole only when the next one starts, and the comparison above, through
NUT, shows none of it in subscribe.

`tests/live_latency.py` holds this: a paced 1080p publisher, a reader
under `ffrwd run`, and each packet's arrival against its own decode time.
It fails when one packet in ten arrives in a batch of three or more. On
Windows at 60 fps, over its `--long` minute:

| | lateness above the floor: median / p90 / max | packets per batch, p90 |
| --- | --- | --- |
| 0.9.1 | 507 / 903 / 1327 ms | 60 |
| 0.9.2 | 8.7 / 15.4 / 273 ms | 1 |

The floor is the least late packet within a second and a half of each
one. The maximum after is one stall upstream of the reader in the run;
the comparison above, which has a native reader beside it, shows none
in subscribe.

What is left on Windows is the publisher's: its sidecar's timers tick at
15.6 ms too, so its packets reach the relay on that grid, and the native
reader sees the same 15.6 ms sawtooth.

`tests/live_chain.py` measures a node of a tree end to end: a source
publishing 1080p60 H.264 and AAC to relay A with the wall clock drawn into
every picture, a query that reads it with `subscribe`, re-encodes it and
publishes it with `publish` to relay B, relay A dropped for four seconds
50 seconds in, and each picture's clock read back at three readers: a
native reader of the source, `subscribe` stream-copying the source, and a
native reader of the republished broadcast. Medians, p90 and max in ms
from the clock in the picture to its decoded picture, over 150 seconds
with the join and the 45 seconds after the drop left out, libx264
veryfast, GOP 120, 8 Mbit/s:

| | native, the source on A | subscribe | the copy on B |
| --- | --- | --- | --- |
| 0.9.1 | 384 / 407 / 541 | 1663 / 2312 / 2610 | 2011 / 2626 / 3023 |
| 0.9.2 | 396 / 417 / 551 | 402 / 427 / 578 | 656 / 696 / 849 |
| 0.9.1, B-frames | 1211 / 1235 / 1393 | 2772 / 3574 / 5562 | 3900 / 4506 / 5368 |
| 0.9.2, B-frames | 1220 / 1253 / 1329 | 1225 / 1258 / 1401 | 2308 / 2351 / 2434 |
| 0.9.1, `backlog` reader | 384 / 406 / 531 | 1692 / 2355 / 3995 | 1995 / 2600 / 2876 |
| 0.9.2, `backlog` reader | 401 / 425 / 577 | 398 / 418 / 571 | 668 / 706 / 1008 |

The B-frame rows encode with two B-frames and x264's `film` tuning, whose
lookahead costs about 800 ms an encode. The node itself, from the source
on A to its copy on B, went from 1.6 s to 260 ms, which is now a decode,
an encode, a publish and a second relay. With the subscribe reader at
`backlog` the chain still joins live: a chain that re-encodes and joins a
live source at `backlog` stops on ffrwd's own pipe check
(`BUFFER_OVERFLOW`) on 0.9.1 and 0.9.2 alike.

On the Linux box, with h264_nvenc at `p1`, GOP 120 and 8 Mbit/s for
both encoders, 60 seconds without a drop: subscribe sat 1504 ms behind
the native reader at 60 fps and 2548 ms at 30 before, and 2.3 and 1.8 ms
behind it after; the copy on B followed the source by 1681 and 3070 ms
before and by 228 and 504 ms after. That box is a six-core laptop part,
and the whole chain ran on it at once.

The republished broadcast keeps its catalog, its init segments and a group
per GOP (keyframes 120 frames apart, and the one gap the drop left), hang's
own stack reads it and decodes it without an error, and B-frames go
through as B-frames. Its sound sits against its picture where the
source's does, read off a click every second against the clock in the
pictures: within 2.5 ms, before the drop and after it, in every Windows
run on either version but one, a B-frame run of 0.9.2 a frame (11 to
18 ms) off, which four repeats did not show.

### The group pacer and a relay that keeps several groups

`moq_core::delivery::Pacer` holds each new group on a track until the
relay has acknowledged the one before it and 10 ms have passed (see "One
group at a time"). That is what keeps a relay that holds only a track's
newest group, Cloudflare's, from losing the older one, and it is not
changed here. A relay that keeps several groups (moq-relay, which this
package's loops and foundmeat's ingest run) loses nothing either way, and
there the hold is only delay: the ingest proof's native reader saw the
first frames of each group arrive 50 to 80 ms late and catch up.

Making it optional would take:

- a mode on `Pacer` in which `release` treats the last group as settled,
  the way it already does for a track nobody is subscribed to, so every
  group goes onto the wire when it is cut and `wait_max_ms` stays 0;
- a way to choose it: a `publish` argument after `hold_s` (positional,
  defaulting to paced, so no existing query changes), and the same switch
  wherever else a `Pacer` is built (foundmeat's gateway builds its own);
- the rows to say which mode a track ran in, since `unpaced` would no
  longer mean a group at risk;
- measurements before it is offered: `tests/live_pacing.py` with the mode
  on, to show the per-group wait gone and nothing lost against moq-relay,
  and the same against Cloudflare with the mode off, to show the default
  still holds there. The default stays paced.

### A dropped session

A public relay resets sessions now and then, every one at once: two
leaves of a demo tree on the same root lost theirs at the same instant
after 45 minutes, and their one resubscribe on the dead session failed.
Both halves of this package open a new session instead, for as long as
`reconnect_s` allows (60 by default; 0 ends the run on the first drop).

The subscriber notices when the session's protocol task ends, or when
a track cannot be subscribed to again on it. It dials again, a doubling
wait apart (half a second up to five), waits for the broadcast to be
announced, reads the catalog again and takes every track up at the live
edge. The tracks have to come back under the same names, carrying what
a decoder reads unchanged, or what follows is not the stream this run
described, and that stops it. A track whose first group on the new
session is numbered below where the reader had got belongs to a
publisher that started again, which the next section is about.
Otherwise each hold is told its track restarted, which gives up the
hole between the two sessions at once, and a video track waits for a
sync sample again, since the frames it would have referred back to went
with that hole.

The publisher keeps its broadcast in the module rather than the
session, so groups go on being written while a new session is dialed
in the background, the new session announces the same broadcast, and
its group numbers carry on. The catalog goes out again the moment the
new session is in.

A relay that goes away without closing the session - a process gone, a
network gone - says nothing either side can hear, so each finds out
when nothing has come back for the QUIC idle timeout, 30 seconds, with
a keep-alive every 2 seconds holding a quiet session open meanwhile. It
is no shorter because the session runs only inside a host call: a
module the host stops calling for a while, behind a full pipe or a slow
rows output, is silent to the relay for that long, and a shorter
timeout would turn that stall into a dropped session. A relay that
resets a session closes it, and that is heard at once.
`tests/live_reconnect.py` kills a local relay a few seconds into a
paced broadcast and starts it again on the same port: both halves come
back on the first dial, and the reader's video and audio go on after a
gap of the idle timeout plus the time the relay was away, the first
video packet after it a keyframe. A reader with `still` set hands a
still on while it waits instead; see "A still while the publisher is
away".

### A publisher that starts again

A program that publishes a broadcast is sometimes replaced. Its process
is lost and a new one is started on another machine, or its run ends,
its input having ended, and the next run of the same query starts. Each
time, a new publisher publishes the same broadcast name with the same
query and settings, and its timestamps and group numbers start over. A
reader that copies the broadcast on, to an RTMP push, an HLS output or a
file, carries on across that with a gap rather than ending, or with a
still held across the gap when it has `still` set (see the next
section).

What a subscriber sees depends on how the publisher went. Against
moq-relay 0.14.13:

- A publisher killed outright says nothing, and the relay hears it gone
  only at its QUIC idle timeout, 30 seconds. A new publisher that
  announces the name before then takes it over at once: the relay
  replaces a live broadcast from another publisher rather than wait out
  the old session. The subscriber's subscriptions are reset, and on its
  own session, which stays up, the broadcast is unannounced and
  announced again, its route now starting at the new publisher's origin.
  With nobody to take over, the relay fails the tracks at its idle
  timeout and drops the broadcast once its linger, 5 seconds, is over.
- A publisher whose run ends finishes its tracks, and the subscriber's
  tracks end. The relay goes on announcing the finished broadcast until
  the publisher's session closes and the linger after it is over, 7 to 8
  seconds in all, unless a new publisher takes the name first. A
  subscriber that dials in that time is handed the finished broadcast,
  its catalog and its last group again.
- Either way the new publisher numbers its groups from 0, and its
  timestamps start at 0.

So the reader watches what its session's relay announces at the
broadcast's path. When the broadcast it reads goes away, its tracks
finished or the broadcast closed under a session that is still up, it
waits for the path to be announced again, reads the catalog and takes
every track up at the live edge. A session that ends meanwhile is dialed
again. A broadcast the reader has already read to its end is passed
over, since the relay goes on announcing it for a few seconds. The
reader knows it as the same broadcast on the same session, by a route
that starts at the same origin on a new session, and, where the wire
carries no route, by its last group arriving again byte for byte. That
last case is said on stderr, and the group is dropped.

Whether that is the same publisher decides the rest. A publisher keeps
its origin across its own sessions and a new process does not, so a route
that starts at another origin is a new publisher. A publisher that
finished its tracks does not come back. Where the wire carries no route,
as moq-transport up to draft 16 does not (Cloudflare's relay speaks
draft 16), the reader takes it as a publisher that started again when:

- a track's first group is numbered below where the reader had got;
- a track's first group has the number the reader had just reached, but
  its first frame is not the frame the reader had;
- the first media packet the new subscription brings is behind the last
  one handed on. What a track still held of the old publisher and lets go
  of before then goes out under the old shift and says nothing either way.

A publisher that carried on is taken up as after a dropped session, its
packets going out as they come. For one that started again:

- What a track still holds of the old publisher, groups kept behind a
  hole it never filled, goes out first, and the hole is named in a HOLE
  row. Then each track joins the new numbering at the first group a
  decoder can start at, so the first video packet after the gap is a
  keyframe.
- Every track has to carry what a decoder reads unchanged: its codec,
  its decoder configuration (the `avcC`, the `esds`), its geometry, its
  timescale, its sample rate and its channels. A real ffrwd program
  started again with the same query writes the same init segments byte
  for byte. One that differs only where no decoder looks (a track
  number, a creation time, the fragment defaults) carries on, its
  fragments read by the init segment that came back. Anything else stops
  the run, naming what changed.
- Every track's timestamps get the same shift, so the sound keeps its
  place against the picture. The shift makes the gap in the output as
  long as the stream was away by the wall clock, and no timestamp goes
  back. Every later packet of that publisher gets the same shift, and the
  next restart works out a new one.
- The shift is worked out from packets that arrived when they were made.
  A live join hands each track the group in progress all at once (for
  video a GOP that began up to a GOP ago, for audio its newest group),
  and then the publisher's new frames one at a time. So the reader holds
  what the tracks bring in for at least 300 ms, which reaches past that
  burst, and until every media track has a packet, for 2 seconds at most
  after the first one. The packet in the hold that waited least decides
  the shift. Messages are held with the media and get the same shift, but
  decide nothing while the reader has media tracks: a message alone waits
  for the media, for 2 seconds at most, and with no media by then the
  shift is read off the messages. A media track whose first packet comes
  after the shift is fixed gets the same shift, and a media packet that
  would go out at or before its track's last one is dropped, as is what
  follows it until a sync sample past that point.
- The gap is measured against each track's own pace before it. For every
  packet of the track's last two seconds, the reader takes the packet's
  end and carries it forward by the wall-clock time since the packet
  arrived, which says where the output had got when the last packet
  before the gap went out; the median of those is the track's pace. Each
  track has its own, because a publisher whose input ends hands on its
  encoder's delay at once: in the loop below, the last 11 pictures of a
  run that ended, a third of a second of them, arrived within 10 ms,
  while the sound stayed paced.

`restart_s` is how long a reader whose publisher finished its tracks
waits for a new one, in seconds. A run that ends and a file published to
its end look the same on the wire when they finish, and a file must
still end its run, so the default, 0, ends it there as before. A live
egress sets it to how long its program may take to come back.

A lost publisher leaves a broadcast dropped with its tracks unfinished,
and the relay fails those tracks some seconds before it drops the
broadcast. Either one is waited for as long as `restart_s` or
`reconnect_s` allows, whichever is longer, since a broadcast that went
away under a reader was already waited for as long as a dropped session.

`tests/live_restart.py` is the loop that holds this. It runs a local
relay and three publishers of one rung, its audio and a track of
messages, one after another:

- the first is killed outright 15 seconds in, every process of it at
  once, so nothing closes its session;
- the second starts 3 seconds later and runs to the end of its file;
- the third starts 4 seconds after that.

The reader is in the egress shape: `subscribe` stream-copied to an FLV
under `ffrwd run`, with `restart_s` at 20. Every source has a white frame
and a click on each of its own whole seconds, so the loop can measure
each gap and the sound's place against the picture. The egress shape
reads no messages, so a second reader takes the picture, the sound and
the messages into a NUT beside it.

Against moq-relay 0.14.13 the reader follows both restarts, `replaced`
and then `finished`. Each gap in the FLV is within 20 ms of how long the
stream was away by the publishers' own clocks. The first video packet
after each gap is a keyframe, and the clicks stay within a millisecond of
where they were against the white frames. The reader ends `restart_s`
after the last publisher's tracks finish. With each new publisher started
45 seconds after the last one went, past the relay's idle timeout, the
gaps are still within 20 ms of the time away, and `ffrwd run` does not
stop a reader that hands on nothing for that long. The second reader's
messages never go back, and every one after a gap is a message of the
publisher's own at its own pts under the shift its reader's row states.

### A still while the publisher is away

An egress piece that pushes a broadcast on over RTMP feeds a connection
that the service at the other end closes when nothing has come over it
for a while. While a reader waits for its publisher it hands nothing
on, so across a restart of the program the push can go quiet for longer
than that; the program's media then meets a closed connection, the
piece connects again, and the service starts a new broadcast. With
`still` at 1 the reader keeps the push fed while the publisher is away,
so the media that follows goes out on the same connection. Per track it
hands on:

- for a picture, the last sync sample the track handed on, again every
  500 ms under new timestamps. The bytes are the same, so nothing is
  encoded and the stream's decoder configuration holds; each still is a
  keyframe a decoder can start at.
- for sound, silent AAC frames of 1024 samples, one after another with
  no hole between them. A silent frame is a single channel element for
  mono (`01 40 20 07`) or a channel pair element for stereo
  (`21 10 05 00 a0 1c`) whose window has no scale factor bands, so it
  decodes to samples of zero at any sample rate. That covers AAC-LC with
  1024-sample frames in mono or stereo; other sound (HE-AAC, 960-sample
  frames, more channels) has no still, and the reader says so once on
  stderr when it starts.
- for messages and cues, nothing.

The stills start at once while the reader waits for a publisher it
knows has gone (its tracks finished, or its broadcast dropped) or for a
dropped session to come back. A publisher lost outright says nothing a
reader can hear until the relay times it out, so they also start once
no media has come off the wire for a second; a live track's frames come
a frame apart, and one that is late by less than that starts nothing.
Each track's stills carry on from the last packet it handed on, at the
pace of the wall clock: a still goes out once as much time has passed
since that packet went out as its timestamp is past the packet's. So a
still is never behind its track, and the stills run in real time. The
waits for a publisher or a session go on across pulls, so the pulls in
between hand the stills on.

The stills stop when the publisher's media comes off the wire again, or
with the run when the wait for it ends. A run whose publisher finishes
its tracks with `restart_s` at 0 still ends there; the end of a track
can reach the reader more than a second after its last media, so a
second or so of stills can be the last thing the run hands on. A still
counts as its track's
last packet, so the shift a publisher that started again gets places
its media after the stills on every track. The shift is read off the
pace as before, and moved further on where that would land a track's
first packet at or before its last still. It is one shift for every
track, so the sound stays where it was against the picture. A new
publisher's first keyframe can be up to a GOP older than its newest
sound, so after a restart the output can run ahead of the wall clock by
up to a GOP. Whatever is left between a track's last still and the
publisher's first packet on it is filled before that packet goes out,
to within 500 ms on a picture and within a frame on sound.

A publisher whose clock carried on, after a stall or after its own
session was replaced, comes back behind the stills, which ran on in
real time meanwhile. Rather than drop its media as packets that would
go back, the reader holds it and shifts it past the stills in the same
way, and says so in a RESTART row with `seen` set to `overtaken`.
After a still a picture starts again at a sync sample: a decoder that
has just decoded the still no longer holds the pictures the publisher's
next frames refer to.

The reader says on stderr when the stills start, and when they stop how
long they went out for and how many pictures and silent frames there
were. A RESTART row for a gap the reader held a still across carries
`still_ms` and `stills`, the same figures.

`still` is a number, 1 or 0, rather than a boolean: ffrwd's compiler
takes literals and NULL among a source's arguments, and `true` is
neither to it. The module itself also takes `true` and `false`.

`tests/live_still.py` is the loop that holds this. It is
`tests/live_restart.py`'s shape with the reader in the egress shape and
`still` at 1: a first publisher killed outright 12 seconds in, a second
started 6 seconds later that runs its file to the end, a third started
5 seconds after that, and `restart_s` at 8. Each source marks its
frames with the publisher that made them and puts its keyframes half a
second off its white frames, so a still is told by being the last
keyframe again, byte for byte. Against moq-relay 0.14.13, in the FLV the
reader writes:

- decode times never go back, no two video packets are more than
  500 ms apart, and the largest hole in the sound is 2.7 ms, under a
  frame;
- each of the three times the publisher was away is a run of stills
  500 ms apart (13, 19 and 19 pictures) beside a run of silent frames
  (329, 442 and 468), as many as the reader's stderr says, and the
  silence decodes to samples of zero;
- the stills start a second after the last media and run at the pace of
  the wall clock. They cover 7.02 s of media over the 7.1 s since the
  first publisher's last packet; 9.43 s over the 9.7 s since the
  second's, less the moment the third publisher's first packets are held
  while its shift is read; and 9.98 s over the 10.0 s after the last
  publisher's, until `restart_s` was spent;
- the next publisher's first keyframe follows the last still by 33 ms
  and 73 ms, and its sound the last silent frame by 2.7 ms and 0.7 ms,
  and none of its media is dropped behind a still;
- the clicks sit within 0.3 ms of where they were against the white
  frames across both gaps, and the RESTART rows say `replaced` and
  `finished`, each with the still it held.

### What a subscriber says

The subscriber's rows are the node's rows, beside its outputs, in the
shape `describe` reports as `rows_schema`. They went to its stderr while
it was a packet source, which had no row channel; ffrwd 0.29's compiler
does not write a node's rows anywhere yet, so for now they reach a run
only when its sidecar is given a rows label (`[@rows=r]`).

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
stretch in sequence), `first` and `last`, and `behind` (media packets
that would have gone out at or before the track's last packet, and the
packets after them until a sync sample, dropped; see "A publisher that
starts again"). Since 0.9.2 it also
carries `gap_max_ms` and `call_max_ms` over the window, as the
publisher's rows do: the longest the session lay undriven between two
pulls, and the longest one pull lasted, most of which is waiting for
something to hand on. The session runs only while a pull is on the
executor, so `gap_max_ms` is what a host that stops calling costs the
wire.

Beside them, a row per incident: a HOLE row for every hole given up on
(`from`, `to`, `reason`, and what was held at the time), a LATE row for
every group that arrived too late to use, and a SKIPPED row, the same
shape, for every group a live join stepped over on its way to one a
decoder can start at. A RECONNECT row says a dropped session was
replaced (`attempts`, `down_ms`, `error`, as the publisher's). A RESTART
row says the reader followed a publisher that started again: `seen`
(`finished`, its tracks finished and a broadcast came after; `replaced`,
another publisher announced it; `renumbered` or `rewound`, a track's
first group or first timestamp came back behind where the reader had
got; `overtaken`, its first media came back behind the stills the reader
handed on meanwhile), `away_ms` (how long the stream was away, as the
output shows it: from the end of the publisher's last packet before the
gap to the newest of the first packets after it), `shift_us` (what that
publisher's timestamps get from then on, in microseconds) and, where
the reader held a still across the gap, `still_ms` and `stills` (how
long it held it, and how many stills went out). The reader says the same
in one line on its stderr. A publisher that started again before the
reader had handed anything on leaves no gap and loses nothing, so it has
no RESTART row. None of
them is ever silent, so a run that lost something says which groups by
sequence. The first 256
of each per track are spelled out and the rest are only counted.

## Data tracks

A data stream is a sequence of messages, each one JSON object at its
own pts. `publish` takes one as a column beside the rows, and each
becomes a MoQ track of its own:

```pgsql
COPY (
  SELECT f.video[1], f.audio[1], d.data[1]
  FROM input(:'source', realtime => true) f, input('deal.nut', realtime => true) d
) TO ffrwd.moq.publish(:'relay', :'broadcast')
```

A data pad is named by the rendition name the host hands it, when it
hands one, and otherwise `data`, `data.1`, ... in the order the query
names the columns. ffrwd 0.19.0 hands a data pad no name, so a column
alias (`awards.d AS deal`) does not reach the module yet, and the track
is `data`.

**One message is one group.** Each message is the one frame of a group
of its own, in hang's `legacy` framing: a QUIC varint of the pts in
microseconds, then the message's bytes exactly as they arrived. The
frame's own timestamp is set to the same pts, but a reader cannot
count on it: over an IETF draft of the wire (moq-transport 16, which
Cloudflare's relay speaks) neither end sends a track's timescale, and a
subscriber stamps every frame with the time it ARRIVED. Media survives
that, its time being inside its fragments. A message would not, so its
pts rides inside the frame.

**The pts is when the message was emitted.** A break announced five
seconds ahead is a message at the moment of the announcement, with the
cue as a field of its own inside the JSON (`start_pts`). What matters
about such a message is that it arrives BEFORE the media it refers to,
so a call writes its messages first, each group finished and driven
onto the socket before any media handed over in the same call is
touched. A data track's delivery priority is 90, above audio's 80: a
message is a few hundred bytes now and then, which costs the sound
nothing, and it must not wait in a send queue behind it. It asks for
ordered delivery, as audio does, since a newer message does not stand
in for an older one.

**Messages leave one at a time**, as every track's groups do (see "One
group at a time"). On Cloudflare's draft-16 relay the first of two
messages written in one call was lost five times in seven before
0.7.1. Now the second waits in the track's queue until the relay has
acknowledged the first, a round trip later, and nothing waits for a
message that follows the one before it further apart than that.
`tests/live_data.py --fixture pairs` publishes such pairs: moq-relay
keeps every group and loses nothing either way, and the loop holds that
every message of a pair arrives, whole and in order.

The catalog names the tracks in a `data` section of this package's
own, beside hang's two:

```json
"data": {"renditions": {"data": {"codec": "json", "container": {"kind": "legacy"}, "timescale": 1000000}}}
```

`timescale` is what the messages' pts count in: the stream's own ticks
per second when its time base is `1/n`, microseconds otherwise. A
player reads past the section, since hang's catalog denies no unknown
field, and nothing it plays changes. A data track filed under `video`
or `audio` instead would fail hang's validation and take the whole
broadcast off the player, which is why it has a section of its own.

`subscribe` lists every track of that section as a data cell of the
FIRST row, beside that row's picture and sound, so `s.data[1]` reads
the broadcast's first data track:

```pgsql
COPY (
  SELECT s.video[1], s.audio[1], s.data[1]
  FROM ffrwd.moq.subscribe(:'relay', :'from') s
) TO ffrwd.moq.publish(:'relay', :'into')
```

Each group becomes one packet: the message's bytes, at the pts its
frame carries, in the track's time base (`1/timescale`), a keyframe
with its dts at its pts. A group is handed on the moment it has been
read to its end, not when the next one begins: the next message may be
a minute away. A live join starts at the newest message, and a dropped
session takes the data tracks up again with the rest.

`tests/live_data.py` is the loop that holds this: a paced publisher
with video, audio and 43 messages over 98.5 seconds, and a reader
under the sidecar timing each packet as it leaves. Every message
arrives byte for byte, at the publisher's pts to the microsecond, and
ahead of the first picture at or past its pts. The margin is a second
to two and a half against a local relay before 0.9.2, since a picture
waited for its group to end and a message did not. A picture now goes
on as it arrives too, and a message follows the picture of its own pts
by up to the publisher's own lag: its host hands a message to publish on
its clock's tick after that picture, 15 to 48 ms later at 30 fps.
`tests/live_data.py` bounds that at 100 ms. Held to moq-transport 16
(`--relay-version`), the same holds, the pts coming out of the frame;
with the relay taken away mid-run (`--reconnect`), what is lost is only
what went out while the reader had no session.

## Text tracks

Captions travel as a text track beside the picture and the sound: WebVTT
in fragmented MP4 (ISO/IEC 14496-30), which is the text track CMAF
defines and the one DASH and HLS players read as
`application/mp4; codecs="wvtt"`. `publish` writes one from a column of
cue rows, and `subscribe` hands one back as cue rows.

### Publishing captions

A column of cue rows beside the media becomes a text track:

```pgsql
COPY (
  SELECT f.video[1], f.audio[1], ffrwd.parakeet.transcribe(f.audio[1]) AS captions
  FROM input(:'source', realtime => true) f
) TO ffrwd.moq.publish(:'relay', :'broadcast')
```

A cue row is what ffrwd's transcribers write and its caption consumers
read:

| field | |
| --- | --- |
| `text` | the cue's text, WebVTT cue text: `<i>`, `<b>`, `<c.class>` and the rest are its own |
| `start_t`, `end_t` | where it shows, in seconds on the program's timeline |
| `final` | false on each row of an utterance still growing, true on the row that ends it; a row without it is a whole cue |
| `voice` | who is speaking, written as a `<v Name>` span around the text |

ffrwd binds the column by its fields: a column whose rows carry `text`,
`start_t` and `end_t` goes to the text track, and any other data column
to a data track (see "Data tracks"). The track is named after the
column's rendition, or `text`, `text.1`, ... in the order the query names
them.

Only finished cues are written. Each growing row of an utterance would
otherwise be another cue over the same words, so rows with `final` false
are passed over and counted. A cue is known only once it has finished,
after it started, so the track's timeline is held `text_hold_ms` behind
the newest time the publisher has seen on any input, 3000 by default. A
cue that arrives within the hold is placed at its own start. One that
arrives later starts where the track had got, and one that ended before
that is dropped; the track's rows count both. The text track runs that
far behind the picture.

The default is measured. Over ffrwd/parakeet's `transcribe` on four
LibriSpeech recordings and seven clips of synthesized commentary, 25
final rows in all, a final row is written 0.40 to 1.28 s after its last
word (median 0.80 s), and 1.2 to 16.8 s after its first (median 4.0 s),
since parakeet ends an utterance at a pause. A row reaches publish once
the 160 ms it was heard in has run, so the latest arrives 1.44 s after
its cue ends, and 3000 drops none with 1.5 s to spare. How much is
clipped depends on how long an utterance runs. At 3000, 15 of the 25
cues start late, by a median of 1.5 s and at most 14 s, the long read
sentences of the LibriSpeech recordings by 4 to 11 s, and a clipped cue
keeps 1.6 to 2.4 s on screen. At 2000, 18 start late, by a median of
2.2 s, and a clipped cue keeps 0.6 to 1.4 s.

A cue's text is made safe for a `.vtt` file made from the track: a cue
ends at an empty line there and a timing line is anything with `-->` in
it. So lines that are empty or only white space are dropped, every line
ends in a line feed whatever ended it before, and `-->` is written
`--&gt;`, which a WebVTT reader shows as `-->`. A row whose text is
nothing once its empty lines are gone is counted as `empty`. Tags and
character references pass through as written.

What goes on the wire:

- The catalog names the track in a `text` section of this package's own,
  beside `data`, with its init segment inside the entry as a media
  rendition's is, and the language its rendition states:

  ```json
  "text": {"renditions": {"captions": {"codec": "wvtt", "container": {"kind": "cmaf", "init": "<base64>"}, "language": "en"}}}
  ```

  The init segment is one `trak`: a `text` handler, a `wvtt` sample entry
  whose `vttC` is the WebVTT header, and the language in its `mdhd`. A
  hang player reads past the section, as it does `data`.
- The samples tile the track's timeline in milliseconds. Each covers a
  span and carries every cue shown over it, a `vttc` box apiece with its
  `vsid` and `payl`, or a `vtte` where nothing is shown. A cue that runs
  across several samples is in each of them, so a reader that joins at
  the newest group has the cues on screen from its first sample.
- Samples are cut where a cue starts or ends and at least once a second.
  A span with nothing shown that has settled for more than two seconds at
  once, which a jump in the publisher's time leaves, goes out as one
  sample.
- Each sample is one `moof`+`mdat` fragment and one MoQ group of its own,
  paced like every track's groups (see "One group at a time"). The track's
  delivery priority is 85, between the data tracks' and the audio's.

### Reading captions

`subscribe` lists each track of the catalog's text section as a data cell
of the first row, after its data tracks, so `s.data[1]` is the text track
of a broadcast with no data track:

```pgsql
COPY (
  SELECT s.data[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast') s
) TO 'captions.nut'
```

Its rows are cue rows with times in seconds, time base 1/1000, in the
form a streaming transcriber writes: a row with `final` false for each
sample that carries a cue, with the cue's own `start_t` each time and
`end_t` the end of that sample, at the sample's start, and a row with
`final` true at the cue's end. A consumer that reads streaming rows, such
as ffrwd/cea608's `cues` or ffrwd/blitz's, has the text from the cue's
first sample. The rows with `final` true are the cues that went in, their
text as the track carries it, `voice` back out of its `<v>` span.

A reader that joins with a cue on screen starts that cue at the sample it
joined at. A session the relay dropped, and a publisher that started
again, end every cue on screen where the samples stopped, under the
timestamps that publisher had; the rows after the gap carry the shift
every other track gets (see "A publisher that starts again").

### What holds this

`tests/live_text.py` publishes a picture and ten cue rows through a local
relay: one growing row, a whole cue, markup, an empty line and a `-->`, a
cue that ends before the track got there, one that starts before it, and
a long one. A reader there from the start gets every finished cue back
as it went in, the late one dropped and the early one clipped; a reader
joining 22 s in has the long cue on screen from its first row; and
`SELECT s.data[1]` under `ffrwd run` reads the same rows. Given
`--hang-recv` and `--shaka`, it also takes the text track off the wire
with hang's own stack and reads it with Shaka Player's WebVTT-in-MP4
parser (`tests/shaka_cues.js`): Shaka shows each of the eight cues at the
same times with the same text, the italic one italic and `--&gt;` as
`-->`. `tests/live_restart.py` and `tests/live_reconnect.py` carry a text
track across their gaps.

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

## Names a host allows

A host that runs other people's queries on shared relays sets
`FFRWD_MODULE_MOQ_PREFIXES` in the modules' environment: broadcast path
prefixes separated by commas, such as `jobs/4f2a,jobs/4f2a-preview`. A
query cannot set it. While it is set, `publish` and `subscribe` refuse
any broadcast that is not under one of the prefixes, when the query is
compiled, when the run starts and on every reconnect. A list with no
prefixes in it refuses every broadcast. Unset, nothing is checked.

The name checked is the path the relay sees: the relay URL's path, or the
token where the URL has none, then the broadcast. A relay opens a session
under that path, so `moqt://relay.example/jobs/other` with broadcast
`out` is `jobs/other/out`, and a run held to `jobs/4f2a` is refused it. A
prefix matches whole segments: `jobs/4f2a` and `jobs/4f2a/` both cover
`jobs/4f2a/out`, and neither covers `jobs/4f2ab`. A name with an empty,
`.` or `..` segment is refused. The refusal names the path the relay
would see and the prefixes allowed.

A subscriber held to prefixes also asks the relay to announce only the
broadcasts under them, so it does not learn the names other runs publish
on the same relay. `tests/live_prefixes.py` checks all of this against a
local relay.

## What the shared crates changed

**moq-net is a branch of upstream 0.2.15.** Since 0.8.0 this package
builds on
[`ffrwd/moq-net-0.2.15`](https://github.com/imbcmdth/moq/tree/ffrwd/moq-net-0.2.15),
the `moq-net-v0.2.15` release of moq-dev/moq with our commits on top,
pinned by revision in `Cargo.toml`. What it changes: an IETF live join
asks for an absolute start of `{0, 0}` and drops the groups older than
the newest SUBSCRIBE_OK names (see "A broadcast from OBS"), a join at a
named group asks for that group, SUBSCRIBE decodes an absolute start's
location instead of refusing it as trailing bytes, and SUBSCRIBE_OK
carries LARGEST_OBJECT through draft-16. Draft-14 and moq-lite sessions
are as they were.

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

## On ffrwd 0.29

0.9.0 moves both modules to the node world. `publish` reads packets
inputs `video` (one or more, h264), `audio` (any, aac) and `data` (any,
JSON), each as it arrives, on a rate clock of 50 ticks a second; its rows
are the node's rows. `subscribe` reads nothing and keeps its own time:
its shape is the catalog, read over the network when the query
compiles, a packets output per media track and a data output per data
track, grouped into relation rows, and `init` subscribes only to the
outputs the query reads. Its rows leave its stderr for the node's rows.
`build.rs` and the 0.17 bindings go.

What the 0.29 compiler does not do yet, and so what was checked around
it, against 0.8.0 on ffrwd 0.28.0:

- It refuses a node at a COPY's TO (`RETURNS sink`: "a struct is not a
  stream"), so `publish` cannot be called from a query on 0.29, and
  neither the recipes that publish nor the loops that run them can run.
  `tests/live.py`'s run was repeated with the sidecar stage its plan
  makes started by hand: 240 frames reassembled in 8 groups, 1281228
  bytes end to end, every group's packets and bytes the same as 0.8.0's
  on 0.28.0, and both scoped runs (the path in the URL and as the token)
  the same 60 frames and 307274 bytes.
- It writes no node's rows anywhere, so the rows above were read off the
  sidecar with a rows label.
- A node source's streams written to a file are not stamped back
  (`-copyts` is missing at the destination), so a file starts at 0 where
  0.28 kept the broadcast's pts.

`subscribe` on 0.29 and on 0.28 read one broadcast at once (published by
0.8.0, paced, with a data track): over the span both read, every packet
was the same, pts, dts, size, flags and bytes (180 video, 276 audio, 27
messages). The 0.29 reader joined 16 s of broadcast later: it dials the
relay three times before its first packet (the compiler's shape, the
shape the instance resolves at `init`, and `init` itself). With both
nodes on one broadcast, 43 messages of `tests/data/messages.nut` went
out as 43 groups and came back whole, in order and at their pts.

0.9.1 is the same two nodes on ffrwd:av 0.19.1 and ffrwd-node 0.2.0. On
the node world's CLI and sidecar at 81f1b69, which lowers a node at a
COPY's TO, writes a node's rows and keeps a node source's pts at a file:

- `tests/live.py` runs as written: 240 frames reassembled in 8 groups,
  1281228 bytes end to end, and both scoped runs 60 frames each. Run by
  hand as before, the publisher's groups are 0.8.0's on ffrwd 0.28.0,
  packets and bytes, group for group, and the scoped runs 60 frames and
  307274 bytes. Its rows carry the host's `pts` and `time` beside its own.
- `subscribe` on 0.29 and on 0.28 reading one broadcast at once: over
  the span both read, every packet the same, pts, dts, size, flags and
  bytes (1050 video, 1606 audio, 41 messages), with no shift between the
  two files now that a node source's file keeps the broadcast's pts. The
  0.29 reader joined 5 s of broadcast later.
- With both nodes on one broadcast, the 43 messages of
  `tests/data/messages.nut` went out as 43 groups of the 612 published
  and came back whole, as on 0.9.0.

0.9.2 is subscribe's latency (see "Latency") and the loops under `tests/`
on the node world. The ones that ran the module straight under the
sidecar, with a NUT output per track and its rows on stderr, did not run
on ffrwd 0.29: a node's outputs are now the streams of one NUT, named
after the catalog's tracks in a `-filter_complex`, and its rows are
NDJSON on stdout. `common.start_subscribe_node` runs it that way, and
`live_subscribe`, `live_edge`, `live_reconnect`, `live_data`,
`live_pacing` and `live_backlog` pass on ffrwd 0.29.4 against 0.9.1 and
0.9.2 alike. `live_ladder` (its recipe finds no audio in the source it
makes) and `live_burst` (it calls an `audio` export the installed
ffrwd/switch no longer has) fail on both for reasons outside this
package's modules.

## License

This package is **MIT OR Apache-2.0**, and so is everything vendored
into it (`web-transport-quinn`, `quinn-udp`, `quinn-wasi` - each
carried for wasm32-wasip2 fixes upstream does not ship yet).

## Exports

- `publish(relay, broadcast, cert DEFAULT '', token DEFAULT '',
  audio_group_ms DEFAULT 200, rows DEFAULT 'summary',
  reconnect_s DEFAULT 60, hold_s DEFAULT 10, text_hold_ms DEFAULT 3000)`
  returns `sink`: a COPY destination,
  nothing comes back. It reads the whole relation - a video cell, an
  audio cell, either NULL - one rendition per row, and any data columns
  beside it, a track of messages apiece, or a text track for a column of
  cue rows. `text_hold_ms` is how far a text track runs behind the
  newest time seen; see "Text tracks".
- `subscribe(relay, broadcast, cert DEFAULT '', token DEFAULT '',
  start DEFAULT 'live', hold_ms DEFAULT NULL, hold_mib DEFAULT 64,
  join_ms DEFAULT 2000, reconnect_s DEFAULT 60, restart_s DEFAULT 0,
  still DEFAULT 0)`
  returns `source`: a FROM relation, one row per rendition of the
  broadcast's catalog, a video cell and an audio cell, either NULL,
  and the catalog's data tracks as data cells of the first row, its text
  tracks after them, each a stream of cue rows.
  Unbounded. `start` is which end of a running broadcast to join at,
  `'live'` or `'backlog'`; the three numbers after it are the hold the
  groups are put back in order in, `reconnect_s` how long a dropped
  session is tried again for, `restart_s` how long a reader whose
  publisher finished its tracks waits for one that starts again, and
  `still` whether it hands a still on while its publisher is away. See
  above.

## Building

The modules build on ffrwd-node, which carries the world and its
bindings, and ring's C sources want wasi-sdk's clang:

```
CC_wasm32_wasip2=<wasi-sdk>/bin/clang cargo build --target wasm32-wasip2 --release
```

The live pub/sub loops under `tests/` need moq-relay on the machine,
and wasmtime for the ones that drive a guest directly. They run only
when invoked directly. `tests/fast.py` is the tier to run after a
change, in about two minutes: `cargo test` and the short forms of
`live_latency.py` and `live_chain.py`. Their `--long` forms and the other
loops are for before a push.
