-- Publish a live source to a MoQ relay as a broadcast: same shape as
-- publish, the source a URL ffmpeg reads live - srt, rtmp, udp, a
-- device. The run ends when the source does.
-- variables: source (live input URL), relay (relay URL, host by name or IP), broadcast (broadcast path), cert (a private relay's certificate, DER as hex; leave unset for a public relay)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/publish-live.sql -v source=srt://127.0.0.1:9000?mode=listener -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo
COPY (
  SELECT f.video[1]
  FROM input(:'source') f
) TO ffrwd.moq.publish(:'relay', :'broadcast', 'video', COALESCE(:'cert', ''))
  WITH (gop 30, preset 'veryfast', tune 'zerolatency')
