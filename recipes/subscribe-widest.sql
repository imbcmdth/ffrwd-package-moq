-- Subscribe to a MoQ broadcast and write its WIDEST rung to a file,
-- video and audio together: the same relation subscribe reads, ranked
-- instead of filtered. NULLS LAST is what makes it the tallest rung
-- rather than the audio row, since DESC alone sorts a row with no
-- height first. The audio comes from its own row, the way subscribe
-- takes it.
-- variables: relay (relay URL, host by name or IP), broadcast (broadcast path), dest (output file path), cert (a private relay's certificate, DER as hex; leave unset for a public relay), token (an auth token the relay demands; leave unset if it demands none)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/subscribe-widest.sql -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v dest=top.mp4
COPY (
  WITH v AS (
    SELECT s.video[1] AS video
    FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                             COALESCE(:'token', '')) s
    ORDER BY s.height DESC NULLS LAST
    LIMIT 1
  )
  SELECT v.video, a.audio[1]
  FROM v, ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                              COALESCE(:'token', '')) a
  WHERE a.height IS NULL
) TO :'dest'
