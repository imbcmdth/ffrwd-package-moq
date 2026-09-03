-- Republish an ABR ladder as one MoQ broadcast: every rung, rendition
-- for rendition, straight through - the relay invariant. ladder is
-- an HLS or DASH manifest; input() on it yields one row per rendition
-- (its own video and audio), and the sink rebuilds the catalog from
-- those rows, so nothing the manifest said is lost on the wire.
-- variables: ladder (source manifest path or URL), relay (relay URL, host by name or IP), broadcast (broadcast path), cert (a private relay's certificate, DER as hex; leave unset for a public relay), token (an auth token the relay demands; leave unset if it demands none)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/republish-ladder.sql -v ladder=out/master.m3u8 -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo
COPY (
  SELECT r.video[1], r.audio[1]
  FROM input(:'ladder') r
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''), COALESCE(:'token', ''))
