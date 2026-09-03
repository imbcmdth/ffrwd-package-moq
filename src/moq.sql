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
