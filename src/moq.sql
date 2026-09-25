-- Publishing as a sink: a COPY's rows leave the graph for a MoQ relay
-- broadcast, encoded on the way out (shape the encode with the COPY's
-- WITH options; the module takes h264 video and aac audio).
--
-- The relation IS the broadcast: one row is one rendition, a video
-- cell and an audio cell (either may be NULL), exactly as a manifest
-- destination reads a ladder. One row, one track apiece: a video-only
-- row publishes a video track, an audio-only row an audio track, and
-- a row carrying both publishes a muxed rendition. The broadcast also
-- carries catalog.json naming every track, so a subscriber picks one
-- and plays it. The catalog is the hang media layer's
-- (github.com/kixelated/moq): each entry carries its codec, its
-- geometry and its fmp4 init segment.
--
-- A data column (a data_stream of JSON messages: a JSON NUT's d.data[1],
-- a data filter's output, another broadcast's s.data[1]) rides beside
-- the rows and publishes as a track of its own, named by the rendition
-- name the host hands its pad, or data, data.1, ... in the order the
-- query names them (ffrwd 0.19.0 hands none, so an alias does not reach
-- the module yet). Each message is one MoQ group of one frame in hang's
-- legacy framing, its pts as a varint of microseconds and then its
-- bytes as they arrived, and it goes out the moment it arrives, ahead
-- of any media handed over with it; a message that follows the one
-- before it on its track closer than a round trip to the relay waits
-- that round trip in the track's queue, as every track's groups do
-- (see the README, "One group at a time"). A message's pts is when it was
-- emitted; a cue it announces is a field inside the JSON. The catalog
-- names these tracks in a data section of its own, which players read
-- past.
--
-- relay names the relay by URL, its host a name or an IP literal - a
-- name is resolved over DNS-over-HTTPS (1.1.1.1, then 8.8.8.8), since
-- the runner links no name lookup. cert is a private relay's own
-- certificate, DER as hex - a public value, not a secret; left empty,
-- the webpki roots baked into the module decide, which is what a
-- public relay's certificate chains to.
--
-- token is an auth token a public relay demands (a JWT, typically),
-- sent as the session's request path; empty sends none. It lands in
-- the module's params on the command line - scoped, expiring
-- credentials only, the same caveat the query text carries.
--
-- The relay URL's own path is that same request path, which is how a
-- relay hands its address out: publish('https://relay.example/<JWT>',
-- ...) opens the session the token argument would, and rides the
-- command line exactly as the argument does. Either spelling
-- alone is taken as it stands; both naming different tokens is
-- refused, both naming the same one is fine. A '?jwt=' rides along to
-- the relay, a '?token=' is refused as a credential no relay reads,
-- and a '#' fragment is refused rather than dropped.
-- audio_group_ms is how long one audio group runs, in milliseconds. A
-- relay forwards a group once it is whole and a player waits for the
-- group it is reading, so the duration is the delay: 200, the default,
-- is ten AAC frames at 48 kHz. Shorter groups mean more group streams
-- a second, and through a public relay under load 100 lost whole
-- groups where 200 lost none; see the README. 0 gives every frame a group of its
-- own, which is what upstream hang publishes and is EXPERIMENTAL here
-- - a player skips about one such group in four on a real relay, and a
-- group has to last longer than the round trip to the relay plus a few
-- tens of milliseconds, since each waits for the relay to acknowledge
-- the one before it - and a larger value trades delay for fewer streams. Video groups follow
-- the keyframes, and this does not touch them.
-- rows is what the sink reports: 'summary', the default, is one row
-- per track every 5 seconds - its groups, packets, bytes and how many
-- seconds of media have gone out on it - and one total row at the end;
-- 'groups' is a row per published group, which at ten audio groups a
-- second is for piping somewhere rather than for reading; 'none' is
-- the final total alone.
-- reconnect_s is how long a publisher whose session the relay dropped
-- keeps dialing for a new one, in seconds, 60 by default. The broadcast
-- lives in the module rather than the session, so groups go on being
-- written meanwhile and the new session announces the same broadcast,
-- its group numbers carrying on; a row with event 'reconnect' says when
-- it is back. 0 ends the run on the first drop.
-- hold_s is how long the first media waits for a first subscriber
-- before it goes out anyway, in seconds, 10 by default. A subscription
-- starts at the latest group, so the hold keeps a file's opening from
-- being lost to a reader that arrives a moment late. A live source loses
-- nothing by starting at once: 0 publishes the first media straight
-- away, which saves a head nobody watches yet those ten seconds.
CREATE FUNCTION publish(relay text, broadcast text,
                        cert text DEFAULT '', token text DEFAULT '',
                        audio_group_ms number DEFAULT 200,
                        rows text DEFAULT 'summary',
                        reconnect_s number DEFAULT 60,
                        hold_s number DEFAULT 10)
RETURNS sink
  AS 'target/wasm32-wasip2/release/publish.wasm', 'publish' LANGUAGE wasm;

-- Subscribing as a source: a MoQ relay broadcast IS a relation in
-- FROM, one row per rendition of its catalog.json, a video cell and
-- an audio cell (NULL where the rendition lacks the kind). WHERE,
-- ORDER BY and LIMIT over the rows pick rungs at compile time the
-- same way a manifest input's do: s.height, s.width, s.bandwidth,
-- s.codecs, s.name. A broadcast never ends of itself, so the relation
-- is unbounded - a live input, and the run lasts as long as the
-- publisher does.
--
-- Each track of the catalog's data section is a data cell on the FIRST
-- row, beside that row's picture and sound: s.data[1] is the first data
-- track, a data_stream of JSON messages (codec json, time base
-- 1/timescale as the catalog names it). Each message is handed over at
-- the pts its frame carries, never at the frame's own timestamp, which
-- over an IETF draft of the wire is only the time it arrived.
--
-- The catalog is read at compile time, so the broadcast must be on
-- the relay before the query compiles. Each rendition's init segment
-- carries the decoder configuration - h264's SPS/PPS, aac's
-- AudioSpecificConfig - and its fragments are demuxed back to the
-- packets they were built from, so a rung crosses the graph encoded.
--
-- relay, cert and token read exactly as publish's do.
--
-- start is where a reader joins a broadcast that is already running.
-- 'live', the default, joins at the publisher's newest group and starts
-- at the first group a decoder can begin at: for video the first group
-- whose first frame is a keyframe, for audio the first whole group. A
-- node reading a live broadcast is then ONE GROUP behind it - a fifth
-- of a second for audio, one GOP for video - and the publisher serves
-- every group from the join on, so a reader that falls behind catches
-- up rather than being skipped forward. 'backlog' joins at the oldest
-- group the relay still holds instead, which reads the whole cache
-- before the first packet comes out: it is for a publisher running
-- AHEAD of real time - a file poured into a relay as fast as it will
-- take it - and a reader that wants every frame of it. It stands where
-- it does, before the three hold numbers 0.6.4 added, because a wasm
-- function's arguments are positional: a query asking for the backlog
-- would otherwise have to spell out the tuning it does not care about.
--
-- A relay serves a backlog in its own arrival order with the newest
-- group sent first, so on a backlog join the groups arrive nothing like
-- in sequence. They are put back in order in a HOLD, since what leaves
-- this module is packets in decode order and no stream-copy muxer takes
-- a timestamp that goes backwards. The same hold runs on a live join,
-- where the reordering is a group or two rather than hundreds. hold_ms
-- is how long a hole in the sequence waits for the group that would
-- fill it before the relay is asked for it outright. Left NULL it
-- follows start: 1000 on a live join, which has given up the past
-- already and whose every later group waits behind the hole, and 30000
-- on a backlog join, the subscription's own latency window, which is as
-- long as the relay may still serve it. hold_mib is
-- how much one track holds meanwhile, 64 by default, which is 30
-- seconds of about 17 Mbit/s. join_ms is how long a reader that has
-- just joined a BACKLOG waits for a lower group sequence before it
-- fixes its cursor, 2000 by default: a cursor fixed at the first group
-- to arrive would put the rest of the backlog below itself. A live join
-- has nothing older coming and never waits. A hole given up on and a
-- group that arrives too late to be used are both counted and named in
-- a row on the module's stderr, never dropped in silence; see the
-- README.
--
-- reconnect_s is how long a reader whose session the relay dropped
-- keeps dialing for a new one, in seconds, 60 by default. A relay resets
-- sessions now and then, every one at once; the reader opens a new
-- session, reads the catalog again and takes its tracks up at the live
-- edge, so what is lost is the stretch the relay was away and the run
-- goes on. The tracks have to come back under the same names with the
-- same init segments, and a broadcast whose group numbers started again
-- - a publisher that restarted, its clock with it - stops the run
-- instead. A row with kind 'reconnect' says when it is back. 0 ends the
-- run on the first drop.
CREATE FUNCTION subscribe(relay text, broadcast text,
                          cert text DEFAULT '', token text DEFAULT '',
                          start text DEFAULT 'live',
                          hold_ms number DEFAULT NULL,
                          hold_mib number DEFAULT 64,
                          join_ms number DEFAULT 2000,
                          reconnect_s number DEFAULT 60)
RETURNS source
  AS 'target/wasm32-wasip2/release/subscribe.wasm', 'subscribe' LANGUAGE wasm;
