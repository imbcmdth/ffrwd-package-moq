-- Subscribe to a MoQ broadcast and write ONE rung to a file. The
-- relation IS the broadcast: one row per rendition of its
-- catalog.json, a video cell and an audio cell (either NULL where the
-- rendition lacks the kind), so WHERE picks the rung by its own
-- geometry. The broadcast is live, so the run ends when the publisher
-- finishes its tracks.
-- variables: relay (relay URL, host by name or IP), broadcast (broadcast path), height (the rung's frame height, e.g. 720), dest (output file path), cert (a private relay's certificate, DER as hex; leave unset for a public relay), token (an auth token the relay demands; leave unset if it demands none)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/subscribe.sql -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v height=720 -v dest=rung.mp4
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                           COALESCE(:'token', '')) s
  WHERE s.height = :height
) TO :'dest'
