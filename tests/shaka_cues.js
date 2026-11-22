// A text rendition's cues as Shaka Player reads them: its init segment and
// fragments, as one file, through Shaka's own WebVTT-in-MP4 parser.
//
//     node tests/shaka_cues.js <shaka-player dir> <track.mp4>
//
// <shaka-player dir> is an installed `shaka-player` package, from npm.
// Prints one JSON line per cue Shaka makes, which is one per sample a cue
// is shown in: {"start": s, "end": s, "text": ..., "italic": bool}, the
// text as Shaka will draw it, its tags gone and its character references
// read. Shaka keeps a `<v>` span's name only as a style selector, so the
// voice is not in what it hands back.

"use strict";

const fs = require("fs");
const path = require("path");

// The compiled library looks for a browser's globals as it loads; the parser
// itself uses none of them.
globalThis.window = globalThis;
globalThis.self = globalThis;
globalThis.navigator = globalThis.navigator || { userAgent: "node" };
const [log, info] = [console.log, console.info];
console.log = console.info = () => {};
const shaka = require(path.resolve(process.argv[2], "dist", "shaka-player.compiled.debug.js"));
[console.log, console.info] = [log, info];

const data = fs.readFileSync(process.argv[3]);
const boxes = [];
for (let at = 0; at + 8 <= data.length;) {
  let size = data.readUInt32BE(at);
  if (size === 0) size = data.length - at;
  boxes.push({ type: data.toString("latin1", at + 4, at + 8), bytes: data.subarray(at, at + size) });
  at += size;
}
const first = boxes.findIndex((box) => box.type === "moof");
const fragments = [];
for (const box of boxes.slice(first)) {
  if (box.type === "moof") fragments.push([box.bytes]);
  else fragments[fragments.length - 1].push(box.bytes);
}

const parser = new shaka.text.Mp4VttParser();
parser.parseInit(new Uint8Array(Buffer.concat(boxes.slice(0, first).map((box) => box.bytes))));
const time = { periodStart: 0, segmentStart: 0, segmentEnd: 0, vttOffset: 0 };

const text = (cue) => (cue.nestedCues && cue.nestedCues.length
  ? cue.nestedCues.map(text).join("") : cue.payload);
const italic = (cue) => cue.fontStyle === shaka.text.Cue.fontStyle.ITALIC ||
  (cue.nestedCues || []).some(italic);

for (const fragment of fragments) {
  const media = new Uint8Array(Buffer.concat(fragment));
  for (const cue of parser.parseMedia(media, time, null, [])) {
    console.log(JSON.stringify({
      start: Math.round(cue.startTime * 1000) / 1000,
      end: Math.round(cue.endTime * 1000) / 1000,
      text: text(cue),
      italic: italic(cue),
    }));
  }
}
