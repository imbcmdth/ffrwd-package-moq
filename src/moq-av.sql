-- Publishing video AND audio as one sink: the same broadcast as
-- publish(), with the query's audio stream on a track of its own.
--
-- v is the video ARRAY and a the audio stream, and the two go out
-- together: one catalog.json names every video rendition and the audio
-- track, so a subscriber picks a rung and the sound off one document.
-- The COPY's WITH options shape both encodes - the video ones per rung,
-- the audio ones (audio_codec, audio_bitrate, sample_rate) once - and
-- the module takes h264 video and aac audio.
--
-- track names the video renditions, by the height each encodes;
-- audio_track names the audio track. relay, cert and token read as
-- they do for publish().
--
-- a is ONE stream, not an array, because a ladder's own cross join
-- repeats the audio row once per rung: array_agg over it would gather
-- the same stream N times. The module reads any number; this is what a
-- query can name today.
--
-- publish() is the same module and the same export without the audio
-- parameter, for a query that has no audio to hand over.
CREATE FUNCTION publish_av(v video_stream[], a audio_stream,
                           relay text, broadcast text,
                           track text DEFAULT 'video',
                           audio_track text DEFAULT 'audio',
                           cert text DEFAULT '', token text DEFAULT '')
RETURNS sink
  AS 'target/wasm32-wasip2/release/publish.wasm', 'publish' LANGUAGE wasm;
