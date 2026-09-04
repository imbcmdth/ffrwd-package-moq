-- Publish a rendition ladder AND its audio to a MoQ relay as ONE
-- broadcast: one decode, one encode per rung, one row per rendition -
-- a video rung on its own row, the file's audio track on another -
-- plus catalog.json naming all of them, so a subscriber picks the
-- rung it wants. The relation stays rows: a row is one rendition, and
-- a rung's video and the file's audio never share one, so each
-- publishes its own track.
-- variables: source (input media path), relay (relay URL, host by name or IP), broadcast (broadcast path), rungs (how many rungs, matching the two lists), widths (comma list of rung widths, e.g. 1920,1280,854), bitrates (comma list of per-rung bitrates, e.g. 6000k,3000k,1000k), cert (a private relay's certificate, DER as hex; leave unset for a public relay), token (an auth token the relay demands; leave unset if it demands none)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/publish-ladder.sql -v source=film.mp4 -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v rungs=3 -v widths=1920,1280,854 -v bitrates=6000k,3000k,1000k
COPY (
  WITH vid AS (
    SELECT scale(f.video[1], :widths[i.i], -2) AS v, i.i AS rung
    FROM input(:'source') f, generate_series(1, :rungs) i
  ),
  aud AS (
    SELECT a AS t, :rungs + a.index AS rung
    FROM input(:'source') g, unnest(g.audio) a
  )
  SELECT vid.v, aud.t
  FROM vid FULL JOIN aud ON vid.rung = aud.rung
) TO ffrwd.moq.publish(:'relay', :'broadcast', COALESCE(:'cert', ''),
                     COALESCE(:'token', ''))
  WITH (video_bitrate :'bitrates'[vid.rung], gop 30, preset 'veryfast',
        tune 'zerolatency', audio_bitrate '128k')
