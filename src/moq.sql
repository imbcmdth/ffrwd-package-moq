-- Publishing as a sink: the stream in the COPY's SELECT list leaves
-- the graph for a MoQ relay broadcast, encoded on the way out (shape
-- the encode with the COPY's WITH options; the module takes h264).
--
-- relay names the relay by URL, its host a name or an IP literal - a
-- name is resolved over DNS-over-HTTPS (1.1.1.1, then 8.8.8.8), since
-- the runner links no name lookup. cert is a private relay's own
-- certificate, DER as hex - a public value, not a secret; left empty,
-- the webpki roots baked into the module decide, which is what a
-- public relay's certificate chains to. The init segment rides its
-- own track, <track>.init.
CREATE FUNCTION publish(v video_stream, relay text, broadcast text,
                        track text DEFAULT 'video', cert text DEFAULT '')
RETURNS sink
  AS 'target/wasm32-wasip2/release/publish.wasm', 'publish' LANGUAGE wasm;
