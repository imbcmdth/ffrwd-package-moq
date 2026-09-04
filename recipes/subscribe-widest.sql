-- Subscribe to a MoQ broadcast and write its WIDEST rung to a file:
-- the catalog's renditions ranked by frame height, the tallest kept.
-- Same relation as subscribe, ranked instead of filtered - what a
-- player picking the top of a ladder does, decided at compile time
-- off the catalog rather than at play time.
-- variables: relay (relay URL, host by name or IP), broadcast (broadcast path), dest (output file path), cert (a private relay's certificate, DER as hex; leave unset for a public relay), token (an auth token the relay demands; leave unset if it demands none)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/subscribe-widest.sql -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v dest=top.mp4
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                           COALESCE(:'token', '')) s
  ORDER BY s.height DESC
  LIMIT 1
) TO :'dest'
