-- Subscribe to a MoQ broadcast and write ONE rung to a file, video and
-- audio together. A demuxed broadcast, which is what publish-ladder
-- writes, puts a rung's video and the broadcast's audio on different
-- rows, so each subscript is read off a row set filtered to have that
-- kind: v is the rung picked by height, a is the row that has no
-- height, and the cross join pairs the two singles. A muxed broadcast
-- has no audio-only row, so it wants a query of its own. The broadcast
-- is live, so the run ends when the publisher's tracks do.
-- variables: relay (relay URL, host by name or IP), broadcast (broadcast path), height (the rung's frame height, e.g. 720), dest (output file path), cert (a private relay's certificate, DER as hex; leave unset for a public relay), token (an auth token the relay demands; leave unset if it demands none)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/subscribe.sql -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v height=720 -v dest=rung.mp4
COPY (
  SELECT v.video[1], a.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                           COALESCE(:'token', '')) v,
       ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                           COALESCE(:'token', '')) a
  WHERE v.height = :height AND a.height IS NULL
) TO :'dest'
