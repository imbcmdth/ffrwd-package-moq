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
CREATE FUNCTION publish(relay text, broadcast text,
                        cert text DEFAULT '', token text DEFAULT '')
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
