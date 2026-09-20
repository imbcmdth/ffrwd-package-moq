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
-- - a player skips about one such group in four on a real relay - and
-- a larger value trades delay for fewer streams. Video groups follow
-- the keyframes, and this does not touch them.
-- rows is what the sink reports: 'summary', the default, is one row
-- per track every 5 seconds - its groups, packets, bytes and how many
-- seconds of media have gone out on it - and one total row at the end;
-- 'groups' is a row per published group, which at ten audio groups a
-- second is for piping somewhere rather than for reading; 'none' is
-- the final total alone.
CREATE FUNCTION publish(relay text, broadcast text,
                        cert text DEFAULT '', token text DEFAULT '',
                        audio_group_ms number DEFAULT 200,
                        rows text DEFAULT 'summary')
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
-- The catalog is read at compile time, so the broadcast must be on
-- the relay before the query compiles. Each rendition's init segment
-- carries the decoder configuration - h264's SPS/PPS, aac's
-- AudioSpecificConfig - and its fragments are demuxed back to the
-- packets they were built from, so a rung crosses the graph encoded.
--
-- relay, cert and token read exactly as publish's do.
CREATE FUNCTION subscribe(relay text, broadcast text,
                          cert text DEFAULT '', token text DEFAULT '')
RETURNS source
  AS 'target/wasm32-wasip2/release/subscribe.wasm', 'subscribe' LANGUAGE wasm;
