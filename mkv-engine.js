/* SaathWatch MKV engine: demux MKV (mediabunny) -> remux to fragmented MP4 -> MSE on a normal <video>.
   Only browser-decodable tracks are accepted; the caller gates with MediaSource.isTypeSupported.
   Subtitle tracks (SRT/ASS) are extracted with a small EBML scanner and returned as plain-text cues. */
import * as MB from './mediabunny.min.mjs';

export class MkvEngine {
  constructor(file, video, hooks) {
    this.file = file;
    this.video = video;
    this.hooks = hooks || {};
    this.gen = 0;
    this.queue = [];
    this.queueBytes = 0;
    this.appending = false;
    this.stats = {appends: 0, maxChunk: 0, totalMB: 0, lastErr: null, feedsStarted: 0, feedsDone: 0, feedDrops: 0};
    this.cues = [];
    this.destroyed = false;
    this._seekTimer = null;
    this._onSeeking = () => {
      if (this.destroyed) return;
      clearTimeout(this._seekTimer);
      this._seekTimer = setTimeout(() => {
        if (!this.destroyed) this._startFeed(this.video.currentTime).catch(e => this._err(e));
      }, 350);
    };
  }

  async init() {
    const input = new MB.Input({source: new MB.BlobSource(this.file), formats: [MB.MATROSKA]});
    this.input = input;
    const tracks = await input.getTracks();
    this.vt = tracks.find(t => t.type === 'video');
    this.at = tracks.find(t => t.type === 'audio');
    if (!this.vt) throw new Error('Is MKV mein koi video track nahi mili.');
    this.vcs = await this.vt.getCodecParameterString();
    this.acs = this.at ? await this.at.getCodecParameterString() : null;
    if (!MediaSource.isTypeSupported('video/mp4; codecs="' + this.vcs + '"')) {
      throw new Error('Is MKV ka video codec (' + (this.vt.codec || 'unknown') + ' / ' + this.vcs + ') is phone ke browser mein nahi chal sakta.');
    }
    if (this.at && !MediaSource.isTypeSupported('audio/mp4; codecs="' + this.acs + '"')) {
      throw new Error('Is MKV ka audio codec (' + (this.at.codec || 'unknown') + ' / ' + this.acs + ') is phone ke browser mein nahi chal sakta. AAC/Opus wali file try karo.');
    }
    this.mime = 'video/mp4; codecs="' + [this.vcs, this.acs].filter(Boolean).join(', ') + '"';
    if (!MediaSource.isTypeSupported(this.mime)) {
      throw new Error('Ye video+audio combination browser mein play nahi ho sakta (' + this.mime + ').');
    }

    const ms = new MediaSource();
    this.ms = ms;
    this.video.src = URL.createObjectURL(ms);
    await new Promise((res, rej) => {
      ms.addEventListener('sourceopen', res, {once: true});
      ms.addEventListener('error', () => rej(new Error('MediaSource khul nahi paya')), {once: true});
    });
    this.sb = ms.addSourceBuffer(this.mime);
    try {
      const dur = await input.computeDuration();
      if (dur && isFinite(dur)) ms.duration = dur;
    } catch {}

    this.video.addEventListener('seeking', this._onSeeking);
    this.video.addEventListener('error', () => {
      if (this.video.error) this._err(new Error('Decode error (code ' + this.video.error.code + ') - file corrupt ya codec issue.'));
    });

    // background subtitle scan; cues delivered via hook when ready
    scanMkvSubtitles(this.file).then(cues => {
      if (this.destroyed || !cues.length) return;
      this.cues = cues;
      if (this.hooks.onCues) this.hooks.onCues(cues);
    }).catch(() => {});

    // kick off the feed in the background: it pauses itself once enough is
    // buffered ahead and resumes as playback advances, so init must NOT await
    // its completion - only the first playable frames
    this._startFeed(0).catch(e => this._err(e));
    if (this.video.readyState < 3) {
      await new Promise((res, rej) => {
        const to = setTimeout(() => rej(new Error('MKV buffering timeout - file shayad corrupt hai.')), 45000);
        this.video.addEventListener('canplay', () => { clearTimeout(to); res(); }, {once: true});
      });
    }
  }

  async _startFeed(t) {
    const gen = ++this.gen;
    this.stats.feedsStarted++;
    const oldOutput = this.output;
    this.output = null;
    if (oldOutput) { try { await oldOutput.cancel(); } catch {} }
    this.queue = [];
    this.queueBytes = 0;
    try { if (this.sb.updating) this.sb.abort(); } catch {}
    try { this.sb.abort(); } catch {}

    const target = new MB.StreamTarget(new WritableStream({
      write: (chunk) => {
        if (gen !== this.gen) { this.stats.feedDrops++; return; }
        if (chunk && chunk.type === 'write') {
          const u8 = new Uint8Array(chunk.data);
          this.queue.push(u8);
          this.queueBytes += u8.byteLength;
          this._pump(gen);
        }
      }
    }));
    const output = new MB.Output({
      format: new MB.Mp4OutputFormat({fastStart: 'fragmented', minimumFragmentDuration: 2}),
      target
    });
    this.output = output;
    const vsrc = new MB.EncodedVideoPacketSource(this.vt.codec);
    output.addVideoTrack(vsrc);
    let asrc = null;
    if (this.at) {
      asrc = new MB.EncodedAudioPacketSource(this.at.codec);
      output.addAudioTrack(asrc);
    }
    await output.start();
    if (gen !== this.gen) return;
    const vdc = await this.vt.getDecoderConfig();
    const adc = this.at ? await this.at.getDecoderConfig() : null;

    const feedTrack = async (track, src, dc) => {
      const sink = new MB.EncodedPacketSink(track);
      let startPkt = null;
      if (track.type === 'video') {
        startPkt = await sink.getKeyPacket(t) || await sink.getKeyPacket(Infinity) || await sink.getPacket(0);
      } else {
        startPkt = await sink.getPacket(t) || await sink.getPacket(0);
      }
      if (!startPkt) return;
      let first = true;
      for await (const p of sink.packets(startPkt)) {
        if (gen !== this.gen || this.destroyed) return;
        await this._throttle(gen);
        if (gen !== this.gen || this.destroyed) return;
        await src.add(p, first ? {decoderConfig: dc} : undefined);
        first = false;
      }
    };
    const jobs = [feedTrack(this.vt, vsrc, vdc)];
    if (this.at) jobs.push(feedTrack(this.at, asrc, adc));
    await Promise.all(jobs);
    if (gen !== this.gen || this.destroyed) return;
    this.stats.feedsDone++;
    await output.finalize();
    await this._drain(gen);
    if (gen === this.gen && !this.destroyed && this.ms.readyState === 'open') {
      try { this.ms.endOfStream(); } catch {}
    }
  }

  async _throttle(gen) {
    while (gen === this.gen && !this.destroyed) {
      const b = this.video.buffered;
      let ahead = 0;
      if (b.length) ahead = b.end(b.length - 1) - this.video.currentTime;
      // gate on BOTH what MSE has accepted and what is still queued for it:
      // demux+remux is much faster than appendBuffer, so a buffered-only gate
      // lets the queue balloon until the SourceBuffer hits its quota
      if (ahead < 15 && this.queueBytes < 4 * 1024 * 1024) return;
      await new Promise(r => setTimeout(r, 400));
    }
  }

  _pump(gen) {
    if (this.appending) { this.pumpWanted = gen; return; }
    this.appending = true;
    (async () => {
      while (this.queue.length && gen === this.gen && !this.destroyed) {
        const c = this.queue.shift();
        this.queueBytes -= c.length;
        try {
          if (this.sb.updating) await new Promise(r => {
            this.sb.addEventListener('updateend', r, {once: true});
            this.sb.addEventListener('abort', r, {once: true});
          });
          if (gen !== this.gen || this.destroyed) break;
          this.stats.appends++;
          this.stats.maxChunk = Math.max(this.stats.maxChunk, c.length);
          this.stats.totalMB = +(this.stats.totalMB + c.length / 1048576).toFixed(1);
          this.sb.appendBuffer(c);
          await new Promise((res, rej) => {
            this.sb.addEventListener('updateend', res, {once: true});
            this.sb.addEventListener('abort', res, {once: true});
            this.sb.addEventListener('error', () => rej(new Error('MSE append fail')), {once: true});
          });
          if (gen !== this.gen || this.destroyed) break;
          await this._evictBehind(gen);
        } catch (e) {
          this.stats.lastErr = e.message + ' (chunk ' + (c ? c.length : '?') + ' bytes, append #' + this.stats.appends + ')';
          const quota = e && (e.name === 'QuotaExceededError' || e.code === 22 || /full/i.test(e.message || ''));
          if (quota && c && gen === this.gen && !this.destroyed) {
            // small-quota device: free what we can behind the playhead, then
            // wait for playback to move forward and retry this same fragment
            this.queue.unshift(c);
            this.queueBytes += c.length;
            this.appending = false;
            await this._evictBehind(gen, 8);
            await new Promise(r => {
              const to = setTimeout(r, 2000);
              this.video.addEventListener('timeupdate', () => { clearTimeout(to); r(); }, {once: true});
            });
            if (gen === this.gen && !this.destroyed) this._pump(gen);
            return;
          }
          if (gen === this.gen) this._err(e);
          break;
        }
      }
      this.appending = false;
      if (this.pumpWanted != null) { const g = this.pumpWanted; this.pumpWanted = null; this._pump(g); }
    })();
  }

  async _evictBehind(gen, windowSec) {
    // rolling window: drop buffered media behind the playhead so long files
    // don't grow the SourceBuffer until MSE quota errors kill playback
    if (gen !== this.gen || this.destroyed) return;
    const keep = windowSec || 25;
    const b = this.video.buffered;
    const ct = this.video.currentTime;
    for (let i = 0; i < b.length; i++) {
      if (b.end(i) < ct - keep) {
        const start = b.start(i), end = b.end(i);
        try {
          this.sb.remove(start, end);
          await new Promise((res, rej) => {
            this.sb.addEventListener('updateend', res, {once: true});
            this.sb.addEventListener('error', () => rej(new Error('MSE remove fail')), {once: true});
          });
        } catch {}
        if (gen !== this.gen || this.destroyed) return;
      }
    }
  }

  async _drain(gen) {
    while ((this.queue.length || this.appending) && gen === this.gen && !this.destroyed) {
      await new Promise(r => setTimeout(r, 100));
    }
  }

  _err(e) {
    if (this.hooks.onError) this.hooks.onError(e);
  }

  destroy() {
    this.destroyed = true;
    this.gen++;
    clearTimeout(this._seekTimer);
    this.video.removeEventListener('seeking', this._onSeeking);
    if (this.output) { try { this.output.cancel(); } catch {} }
    try { if (this.ms && this.ms.readyState === 'open') this.ms.endOfStream(); } catch {}
    try { if (this.input) this.input.dispose(); } catch {}
  }
}

/* ---------- Minimal streaming EBML scanner for subtitle tracks ---------- */

export async function scanMkvSubtitles(file) {
  const CHUNK = 4 * 1024 * 1024;
  let buf = new Uint8Array(0);
  let pos = 0; // absolute file offset of buf[0]
  async function fill(need) {
    while (buf.length < need && pos + buf.length < file.size) {
      const end = Math.min(file.size, pos + Math.max(CHUNK, need));
      const part = new Uint8Array(await file.slice(pos + buf.length, end).arrayBuffer());
      const nb = new Uint8Array(buf.length + part.length);
      nb.set(buf); nb.set(part, buf.length);
      buf = nb;
      if (end >= file.size) break;
    }
    return buf.length;
  }
  function drop(n) { buf = buf.subarray(n); pos += n; }
  function readVint(at, strip) {
    const b0 = buf[at];
    if (b0 === undefined) return null;
    let len = 1, mask = 0x80;
    while (!(b0 & mask) && len < 8) { len++; mask >>= 1; }
    if (len > 8 || at + len > buf.length) return null;
    let v = strip ? (b0 & (mask - 1)) : b0;
    for (let i = 1; i < len; i++) v = v * 256 + buf[at + i];
    return {len, value: v};
  }
  const dec = new TextDecoder();
  const subTracks = {}; // trackNumber -> {codecId, header}
  let trackOrder = [];
  let scale = 1000000;
  const cues = [];

  // stack-based incremental element walk
  const stack = [{id: 0x18538067, end: Infinity}]; // pretend inside Segment
  let clusterTime = 0;
  let curBlock = null; // {track, time, dataParts, duration}
  let headerDone = false;

  function parseAssTime(s) {
    const m = s.trim().match(/^(\d+):(\d+):(\d+)[.:](\d+)/);
    if (!m) return null;
    return +m[1] * 3600 + +m[2] * 60 + +m[3] + (+m[4]) / 100;
  }
  function cleanAssText(t) {
    return t.replace(/\{[^}]*\}/g, '').replace(/\\N/gi, '\n').replace(/\\n/g, '\n').replace(/\\h/g, ' ').trim();
  }
  function finishBlock() {
    if (!curBlock) return;
    const st = subTracks[curBlock.track];
    if (st) {
      const textAll = dec.decode(curBlock.data);
      const absStart = (clusterTime + curBlock.time) * scale / 1e9;
      if (st.codecId === 'S_TEXT/ASS' || st.codecId === 'S_TEXT/SSA') {
        const parts = textAll.split(',');
        let s = null, e = null, text = '';
        if (parts.length >= 10 && /^\d+:\d\d/.test(parts[1])) {
          // full Dialogue line: Layer,Start,End,Style,...,Text
          s = parseAssTime(parts[1]);
          e = parseAssTime(parts[2]);
          text = cleanAssText(parts.slice(9).join(','));
        } else if (parts.length >= 9) {
          // matroska storage: ReadOrder,Layer,Style,Name,MarginL,MarginR,MarginV,Effect,Text
          // times come from block timestamp + BlockDuration
          s = absStart;
          e = curBlock.duration != null ? absStart + curBlock.duration * scale / 1e9 : absStart + 3;
          text = cleanAssText(parts.slice(8).join(','));
        }
        if (s != null && e != null && e > s && text) cues.push({start: s, end: e, text});
      } else if (st.codecId === 'S_TEXT/UTF8') {
        const text = textAll.trim();
        const e = curBlock.duration != null ? absStart + curBlock.duration * scale / 1e9 : absStart + 3;
        if (text) cues.push({start: absStart, end: e, text});
      }
    }
    curBlock = null;
  }

  async function step() {
    await fill(16);
    if (buf.length < 2) return false;
    const idv = readVint(0, false);
    if (!idv) return false;
    const szv = readVint(idv.len, true);
    if (!szv) return false;
    const headerLen = idv.len + szv.len;
    await fill(headerLen);
    if (buf.length < headerLen) return false;
    const id = idv.value;
    let size = szv.value;
    const unknownSize = size === Math.pow(2, 7 * szv.len) - 1;
    const dataEnd = unknownSize ? Infinity : headerLen + size;

    if (id === 0xA3 || id === 0xA1) { // SimpleBlock / Block
      await fill(dataEnd);
      if (buf.length < dataEnd) return false;
      const tv = readVint(headerLen, true);
      if (tv) {
        const p = headerLen + tv.len;
        const relTime = (buf[p] << 8 | buf[p + 1]) << 16 >> 16;
        const flags = buf[p + 2];
        const lacing = (flags & 0x06) >> 1;
        if (lacing === 0) {
          finishBlock();
          curBlock = {track: tv.value, time: relTime, data: buf.slice(p + 3, dataEnd), duration: null};
          if (!subTracks[curBlock.track]) { finishBlock(); }
        }
      }
      drop(Math.min(dataEnd, buf.length));
      return true;
    }
    if (id === 0x9B) { // BlockDuration
      await fill(dataEnd);
      if (buf.length < dataEnd) return false;
      let d = 0;
      for (let i = headerLen; i < dataEnd; i++) d = d * 256 + buf[i];
      if (curBlock) curBlock.duration = d;
      drop(dataEnd);
      return true;
    }
    if (id === 0xE7) { // Cluster Timestamp
      await fill(dataEnd);
      if (buf.length < dataEnd) return false;
      let ts = 0;
      for (let i = headerLen; i < dataEnd; i++) ts = ts * 256 + buf[i];
      clusterTime = ts;
      drop(dataEnd);
      return true;
    }
    if (id === 0x2AD7B1) { // TimestampScale
      await fill(dataEnd);
      if (buf.length < dataEnd) return false;
      let ts = 0;
      for (let i = headerLen; i < dataEnd; i++) ts = ts * 256 + buf[i];
      if (ts) scale = ts;
      drop(dataEnd);
      return true;
    }
    if (id === 0x86 || id === 0x63A2) { // CodecID / CodecPrivate inside TrackEntry
      await fill(dataEnd);
      if (buf.length < dataEnd) return false;
      const track = trackOrder[trackOrder.length - 1];
      if (track != null) {
        if (id === 0x86) {
          const cid = dec.decode(buf.slice(headerLen, dataEnd));
          if (cid === 'S_TEXT/ASS' || cid === 'S_TEXT/SSA' || cid === 'S_TEXT/UTF8') {
            subTracks[track] = {codecId: cid, header: ''};
          }
        } else if (subTracks[track]) {
          subTracks[track].header = dec.decode(buf.slice(headerLen, dataEnd));
        }
      }
      drop(dataEnd);
      return true;
    }
    if (id === 0xD7) { // TrackNumber
      await fill(dataEnd);
      if (buf.length < dataEnd) return false;
      let n = 0;
      for (let i = headerLen; i < dataEnd; i++) n = n * 256 + buf[i];
      trackOrder.push(n);
      drop(dataEnd);
      return true;
    }
    // master elements we descend into
    if (id === 0x18538067 || id === 0x1549A966 || id === 0x1654AE6B || id === 0xAE ||
        id === 0x1F43B675 || id === 0xA0 || id === 0x1043A770) {
      if (id === 0x1F43B675) clusterTime = 0;
      drop(headerLen);
      stack.push({id, end: unknownSize ? Infinity : (pos + dataEnd)});
      return true;
    }
    // everything else: skip payload (bounded; unknown-size non-master shouldn't occur for skipped ids)
    const skipTo = unknownSize ? headerLen : dataEnd;
    if (unknownSize) { drop(headerLen); return true; }
    await fill(skipTo);
    if (buf.length < skipTo) return false;
    drop(skipTo);
    return true;
  }

  // NOTE: stack-end handling: for simplicity we walk linearly; when inside a sized
  // master whose end we pass, parsing naturally continues at next element because
  // we never recurse - we just keep reading elements in file order.
  let guard = 0;
  while (pos + buf.length < file.size || buf.length > 0) {
    if (++guard > 2000000) break;
    const before = pos;
    const ok = await step();
    if (!ok) break;
    if (pos === before && buf.length === 0) break;
    if (pos >= file.size && buf.length === 0) break;
  }
  finishBlock();
  cues.sort((a, b) => a.start - b.start);
  return cues;
}
