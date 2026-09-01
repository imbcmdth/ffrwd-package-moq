-- Publishing as a sink: the streams in the COPY's SELECT list leave
-- the graph for a MoQ relay broadcast, encoded on the way out (shape
-- the encode with the COPY's WITH options; the module takes h264).
--
-- v is an ARRAY of streams: one is one track, and several are one
-- broadcast carrying a rendition apiece, named under track by the
-- height each encodes. The broadcast also carries catalog.json, which
-- is how a subscriber finds out what is on offer and picks one.
--
-- relay names the relay by URL, its host a name or an IP literal - a
-- name is resolved over DNS-over-HTTPS (1.1.1.1, then 8.8.8.8), since
-- the runner links no name lookup. cert is a private relay's own
-- certificate, DER as hex - a public value, not a secret; left empty,
-- the webpki roots baked into the module decide, which is what a
-- public relay's certificate chains to. The catalog is the hang media
-- layer's (github.com/kixelated/moq): each rendition entry carries its
-- codec, its geometry and its fmp4 init segment, so a hang player can
-- play the broadcast as published.
--
-- publish_av() is the same module and the same export with an audio
-- parameter, for a query that has audio to publish beside the video.
CREATE FUNCTION publish(v video_stream[], relay text, broadcast text,
                        track text DEFAULT 'video', cert text DEFAULT '')
RETURNS sink
  AS 'target/wasm32-wasip2/release/publish.wasm', 'publish' LANGUAGE wasm;
