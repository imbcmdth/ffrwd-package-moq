-- Copy a whole broadcast to a new name on the same relay: subscribe
-- reads every rendition of one broadcast's catalog, publish writes
-- them back as another - rendition for rendition, the relay
-- invariant. Nothing is dropped and nothing is renamed: a row that
-- arrives carrying a rendition name leaves under it, so the copy's
-- catalog names what the original's did.
--
-- The copy joins at the LIVE EDGE, which is what subscribe does unless
-- it is told otherwise: every rendition starts at the first group a
-- decoder can begin at, and they start together, so the copy runs one
-- group behind the original rather than a retention window behind it.
-- A copy meant to carry a backlog wants subscribe's `start` argument
-- instead; see the README.
-- variables: relay (relay URL, host by name or IP), broadcast (the broadcast to read), into (the broadcast path the copy publishes under), cert (a private relay's certificate, DER as hex; leave unset for a public relay), sub_token (an auth token the relay demands of a subscriber; leave unset if it demands none), pub_token (the same for a publisher)
-- example: ffrwd compile -f packages/ffrwd/moq/recipes/relay.sql -v relay=moqt://127.0.0.1:4443 -v broadcast=live/demo -v into=live/copy
COPY (
  SELECT s.video[1], s.audio[1]
  FROM ffrwd.moq.subscribe(:'relay', :'broadcast', COALESCE(:'cert', ''),
                           COALESCE(:'sub_token', '')) s
) TO ffrwd.moq.publish(:'relay', :'into', COALESCE(:'cert', ''),
                       COALESCE(:'pub_token', ''))
