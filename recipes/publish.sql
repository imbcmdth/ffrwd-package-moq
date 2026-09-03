-- Publish a file's first video track to a MoQ relay as a broadcast.
-- The encode happens on the way out; shape it with WITH options.
-- variables: source (input media path), relay (relay URL, host by name or IP), broadcast (broadcast path), cert (a private relay's certificate, DER as hex; leave unset for a public relay)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/publish.sql -v source=film.mp4 -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo
COPY (
  SELECT f.video[1]
  FROM input(:'source') f
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''))
  WITH (gop 30, preset 'veryfast', tune 'zerolatency')
