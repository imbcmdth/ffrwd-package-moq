-- Publish a rendition ladder AND its audio to a MoQ relay as ONE
-- broadcast: one decode, one encode per rung, one track per rung plus
-- an audio track plus catalog.json naming them all.
-- variables: source (input media path), relay (relay URL, host by name or IP), broadcast (broadcast path), rungs (how many rungs, matching the two lists), widths (comma list of rung widths, e.g. 1920,1280,854), bitrates (comma list of per-rung bitrates, e.g. 6000k,3000k,1000k), cert (a private relay's certificate, DER as hex; leave unset for a public relay)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/publish-ladder-audio.sql -v source=film.mp4 -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v rungs=3 -v widths=1920,1280,854 -v bitrates=6000k,3000k,1000k
COPY (
  SELECT array_agg(scale(f.video[1], :widths[i.i], -2)), f.audio[1]
  FROM input(:'source') f, generate_series(1, :rungs) i
) TO ffrwd.moq.publish_av(:'relay', :'broadcast', 'video', 'audio',
                          COALESCE(:'cert', ''))
  WITH (video_bitrate :'bitrates'[i.i], gop 30, preset 'veryfast',
        tune 'zerolatency', audio_bitrate '128k')
