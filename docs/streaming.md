# Streaming Behaviour

How the integration starts, sustains and recovers a live session on top of
the [low-level protocol](protocol.md).

> 🛠 **Agents: keep this file in sync with the code.** Any change to
> live-start ordering, wake retry, source-idle recovery, the idle stream,
> wake when watched, the Camera Awake semantics, MPEG-TS fan-out, or muxer
> timing rules must be reflected here in the same change. See [AGENTS.md](../AGENTS.md) for the full doc-maintenance
> policy.

## Live-start: send immediately after handshake

Direct evidence from the official CloudEdge app pcaps: the IVA handshake
sits in KCP push `sn=0` and `START_LIVE` in KCP push `sn=1` **back-to-back**,
with no delay between them and no wait for any camera-side echo or peer
confirmation.

- In the captured KCP stream: `sn=0` carries the IVA handshake `0x7012` and
  `sn=1` carries VVP `START_LIVE` (cmd `0x11FF`), back-to-back. Reproduce with
  your local capture tooling (see [`AGENTS.local.md`](../AGENTS.local.md)).

Earlier we gated `START_LIVE` on the camera echoing its IVA handshake (or a
short grace after media-peer confirmation). Both turn out to be wrong: on
some post-dormancy starts the camera ACKs our KCP push but **never echoes
the handshake AND ignores a `START_LIVE` sent later**, so the stream
silently stalls until a full reconnect. Sending `START_LIVE` in the same
burst as the handshake — the way the official app does — fixes that class
of stalls.

The engine therefore sends `kcp.send_handshake()` and a `START_LIVE` (reason
`startup`) back-to-back at session start, before entering the main loop.
`live_started = True` is set immediately so the keepalive and idle-retry
paths take over from there.

### The TURN relay probe must not block live-start

Before media starts, the native SDK sends a short TCP probe to the TURN host
(port 9371, see `relay_probe.py`). Its result never gates the media path:
permissions, channel binds and `START_LIVE` proceed whether it succeeds or
not. Some relays accept the TCP connection but never answer; a synchronous
probe then waits out all four receive timeouts on each probed host (the relay
IP and its hostname, usually the same server), stalling the first video frame
by ~10 s even though media then flows through that same relay within
milliseconds. The engine therefore sends the identical probe frames from a
background thread (`start_relay_probe`) and continues immediately; the
`TURN relay probe ok|failed` log line is emitted when the probe finishes.
If the probe thread cannot start, the engine logs the failure and continues
without the optional probe rather than aborting media startup.

Legacy `deviceP2P=ppcs` cameras have no KCP/IVA handshake. Once PPCS confirms
the direct peer, the engine sends the same VVP `START_LIVE` immediately on
reliable channel 0. Its authentication omits the modern licence component;
waiting for a control echo or adding one makes older cameras ignore the live
request.

## Dormancy wake supports both native sequences

A dormant snap (battery / solar) camera can follow either of two sequences in
official-app captures. Newer cameras provide coturn while dormant and wake from
the **SDP offer / live request**; their `status=online` push can lag 40–57 s,
so gating that path on status makes startup needlessly slow. Some older cameras
withhold coturn while dormant: the app sends its signaling + HTTP wake first,
waits for the pushed `online` status on the same MsgSvr connection, and only
then requests coturn.

The engine supports both without model-specific guesses:

- On `status=dormancy` it first requests coturn. If credentials arrive, it
  allocates the relay and enters `_negotiate_dormant_offer`: re-issue the SDP
  offer and re-fire `send_wake_connect` + HTTP `wake_device` every few seconds
  while polling for the camera's delayed SDP answer and trickle candidates.
  The answer is the wake confirmation for this fast path.
- If the dormant coturn request times out or returns no credentials, the engine
  keeps the same MsgSvr session, sends the signaling + HTTP wake immediately,
  and waits in short intervals for that camera's pushed `online` status. It
  retries coturn throughout the same wake budget and continues with the fresh
  online NAT/contact data once credentials become available.
- Signaling wake uses the status keepalive endpoint as a route only. Its two
  connect frames address `node=dev`, `domain=<device UUID>` and carry separate
  outer SIDs; forwarding the status response's `natsvr` identity verbatim is
  tolerated by some cameras but ignored by others.
- `DORMANCY_WAKE_TIMEOUT_S` (≈75 s) bounds each dormant wake phase before
  giving up to the next signaling candidate / a fresh session.
- If neither dormant-offer nor pre-relay wake produces coturn credentials
  within that budget, move to the next signaling candidate instead of spending
  TURN allocation retries on an empty address.

This brought cold-start latency from ~70 s (status-polling) down to ~14 s,
matching the app.

**The watchdogs must not restart underneath an active wake.** A deep-dormancy
wake routinely takes 30–45 s, but the no-first-video restart fires at
`LIVE_STARTUP_STALL_RESTART_S` (25 s) and the debug harness stall restart at
~30 s. Left unguarded they tear the session down *mid-wake*, lose all wake
progress, and — because the camera often only comes online around 40 s — the
stream never starts (observed as a session that sends `START_LIVE` then dies
immediately, 0 frames). The engine therefore exposes `awaiting_wake` while the
offer-driven wake is in progress (set on `dormancy`, cleared once the camera's
SDP answer yields candidates); the HA coordinator (`_stream_video_stale`) and
the debug stall watchdog both **skip the restart while it is set**, and the
coordinator starts its first-video clock from when the wake *completes*, not
from session start. A truly stuck wake still ends on its own when the engine's
`DORMANCY_WAKE_TIMEOUT_S` budget expires, after which the session restarts
normally.

The debug harness `--wake-timeout` default is 90 s to account for worst-case
deep-dormancy wakes (a 25–45 s wake plus first-keyframe time can exceed the
old 45 s window).

## Keeping a live session alive

On an ieGeek H.264 snap camera our live sessions ended on the camera side
about 2 min 13 s after start while the viewer was still connected; the
official app kept a session going for 3 min 47 s without a gap. The capture
shows what the app sends while live and the engine now matches it
(`p2p_streamer/keepalive.py`, ticked from the live loop):

- IVA `0x7012` heartbeat every ~3 s and VVP `0x888E` every 10 s (unchanged).
- MsgSvr heartbeat every 30 s (register `hb`, unchanged).
- **Routed webrtc hello every 21 s** (`SIGNAL_HELLO_INTERVAL_S`). Before,
  the engine said hello only once at setup. This is the leading suspect for
  the 2-minute stop: the camera acts on webrtcsvr session state.
- **Device status query every 120 s** (`SIGNAL_STATUS_INTERVAL_S`).
- No periodic `START_LIVE` (see below) and no HTTP wake while live.

All MsgSvr sends run on one short-lived worker; replies are read and
dropped so the media loop never blocks on the TCP socket.

## Source-idle recovery (sustaining the stream)

The `0x888E` VVP heartbeat keeps the **P2P session** alive. Native QHD HEVC
captures send one startup `START_LIVE`, then VVP heartbeats, then `STOP_LIVE`;
they do not proactively send `START_LIVE` while video is flowing. Re-sending it
mid-flow can make WAN relay sessions go silent on this camera. A capture of
the ieGeek app on an H.264 snap camera (3 min 47 s of live, LAN direct) shows
the same: one `START_LIVE` at start, one more only for a quality switch, and
none as keep-alive. H.264 therefore no longer re-sends it every 30 s
(`start_live_keepalive_s = 0` for both codecs).

The integration therefore:

- Uses VVP heartbeats while video is flowing.
- Late client joins use the cached PAT/PMT + keyframe seed; they do **not**
  proactively send another `START_LIVE` while video is current. A join-triggered
  `START_LIVE` is only used if the source is already stale enough to be in the
  reactive recovery path.
- **Reactively** re-issues `START_LIVE` when video stalls with no
  recoverable KCP gap (~5 s, `idle_start_live_retry_s`), repeating every few
  seconds. This fires whether the camera went fully silent or is still
  sending only audio — **do not** gate it on total UDP silence, the camera
  often keeps audio going after video stops. It also covers the "camera
  ACKed our handshake/live request but never pushed video" case where
  `_video_wait_s` falls back to `now - live_started_at` so the retry path
  still arms.

The idle retry path does **not** short-circuit the reconnect check: if a
session has stayed without video past `source_idle_reconnect_s` it still
gives up, even though several retry pings were issued. Without this,
retries would loop forever on a truly stuck camera.

QHD/HEVC uses a longer reconnect budget than H.264. WAN relay captures show
that the official app tolerates short HEVC pauses and keeps the existing
session alive; reconnecting too quickly can push a snap camera back through
dormancy/offline signaling, which is much slower than waiting out a temporary
source pause.

This resumes far faster than a full reconnect and avoids re-waking a
battery camera, which is itself flaky right after a dormancy transition.
A full reconnect remains the final escalation if the camera still doesn't
resume.

## Direct-LAN media

On the camera's LAN, TURN is still allocated and advertised as fallback, but the
native app moves QHD media to the direct host pair once ICE nominates it. The
engine mirrors that: it gives startup a short direct-ICE grace before KCP, keeps
rapid ICE checks going while seeking a direct peer, stops candidate fanout once
direct KCP is confirmed, and gives confirmed direct sessions a longer
source-idle window before reconnecting.

Legacy PPCS is direct by design. The engine obtains the camera's LAN/public
endpoints from its device-provided PPCS roots, punches both, and keeps the
confirmed endpoint for the session. Peer discovery gets the full dormancy-wake
budget because an older battery camera may register late. After connection,
heartbeats sustain the session; `START_LIVE` is reissued for wake retries,
stale client joins, and source-idle recovery, but not while video is current.

## Camera Awake reflects what the stream serves

A snap camera's stream is in one of three states: idle (the server repeats
the last keyframe as a ~2 fps still), waking (a live window is open and the
P2P session is connecting or waking the camera, but no frame has arrived) or
live (camera frames flow through the muxer). The coordinator keeps two flags
apart:

- `_session_active` is the internal gate. It goes up as soon as a wake is
  requested (button, motion) so the idle still loop stops writing into the
  muxer, and drops when the live window ends.
- `camera_awake` is the reported state behind the **Camera Awake** binary
  sensor (and the camera entity's `is_streaming`). It is on while the
  initial frame grab runs and while live frames keep arriving
  (`AWAKE_LIVE_FRAME_TIMEOUT_S`, 10 s, rides out short source pauses). It is
  off while a wake is still pending and whenever the idle still is served.

Reporting `camera_awake` from the wake request instead of from frames made
the sensor say "awake" for a camera that was still asleep, and it could miss
the live period entirely.

The Camera Awake sensor carries a `live_until` attribute (ISO-8601 UTC in
whole seconds, or `null`): when the current live window ends if nothing
extends it. The snap loop works in monotonic time, so the wall-clock value
is computed once, rounded to the second and stored (`_live_until_epoch`)
when the planned end moves by more than a second, and re-published only
then. Recomputing "wall now + monotonic remaining" on every state write
produced a slightly different timestamp each time, so every unrelated
update added a recorder row.

## Wake when watched

On a snap camera the live window (`_live_deadline`) is opened by motion or
an explicit wake; outside it the server serves the idle still. With *Wake
when watched* (`coordinator/watch_wake.py`, option `wake_when_watched`,
default on) the snap loop also treats a connected stream client as a reason
to be live:

- A client on the local MPEG-TS server (`StreamServer.client_count > 0`)
  starts a **viewing session**. If no live window is open, the coordinator
  sends the same HTTP wake as the Wake button; the live loop then starts the
  P2P session, which handles dormancy wake as described above. The window
  is opened **before** that wake and re-anchored on the time it returns:
  the wake is two blocking cloud calls (30 s read timeout each), and a
  window computed from the earlier time could already be over, making the
  next loop tick wake again. Watch-triggered HTTP wakes are also spaced at
  least `WATCH_WAKE_RETRY_S` (30 s) apart.
- While clients stay connected the live window is extended to
  `now + WATCH_WAKE_LINGER_S` (10 s) on every loop tick, so live ends about
  10 s after the last client leaves.
- **No wakes while live.** Only the initial wake is sent. The official app
  sends no HTTP wake during live; the session is kept up by the P2P and
  MsgSvr keep-alives (see "Keeping a live session alive"). An earlier
  90 s re-wake was removed after the app capture.
- **Still over stalls.** If live frames stop for more than
  `WATCH_STALL_STILL_S` (5 s) during a watch session, the idle loop takes
  the muxer back and serves the idle still (Camera Awake goes off, camera
  audio is dropped) instead of leaving viewers on a frozen picture until
  the source-idle reconnect. When frames return, the live path restarts on
  a keyframe (it requests one) with fresh mux timing, because the idle loop
  restarted the muxer for the still. Outside watch sessions the old
  behaviour is unchanged.
- **Session cap.** A viewing session gets at most
  `wake_when_watched_max_minutes` (default 5, 1–60) of watch-driven live
  time. The window never extends past the cap; afterwards the stream falls
  back to the idle still. Client gaps under `WATCH_SESSION_GAP_S` (30 s)
  belong to the same session.
- **A capped session is sticky.** It only ends after every client has been
  gone for `WATCH_CAPPED_QUIET_S` (5 min). Extra connections, a client that
  reconnects after a backoff (HA's stream worker restarting after the
  live→idle mux restart), an explicit Wake, and toggling the option do
  **not** re-arm it. An explicit Wake still gives its normal window (the
  motion timeout); it just does not start a new watch session.
- **Rolling budget.** Watch-driven live time is also limited to
  `WATCH_BUDGET_CAPS_PER_WINDOW` (3) session caps per rolling hour
  (`WATCH_BUDGET_WINDOW_S`). The budget counts the live time each
  extension actually grants, so motion or button windows that already cover
  the time are not charged. Once used up, new sessions are capped at once.
- **Survives reloads.** Session and budget state (`WatchState`) live in
  `hass.data[DOMAIN]["_watch_wake"][serial]`, not on the coordinator, so an
  options save or entry reload (which rebuilds the coordinator and the
  stream server) does not re-arm the cap. An HA restart does.
- Motion and the Wake button keep their own windows; wake when watched only
  ever extends, never shortens, the deadline.
- `live_until` reports the cap (or budget) end while a viewer keeps
  watching.

The integration itself never connects to its stream server (snapshots are
converted from in-memory keyframes), so every counted client is a real
consumer: HA's `stream` worker or go2rtc for a dashboard viewer, Frigate,
the debug harness's ffplay, and so on.

**"Connected" is not "watching".** The only signal is an open TCP client.
How long that outlives the human viewer depends on the player:

- **WebRTC** through go2rtc: go2rtc drops its producer about 20 s after the
  last WebRTC consumer leaves, so live ends ~30 s after the viewer.
- **HLS** through HA's `stream` worker: the worker keeps the source open
  until `OUTPUT_IDLE_TIMEOUT` (30 s in HA 2026.3,
  `homeassistant/components/stream/const.py`) after the last HLS request,
  so with the 10 s linger live ends ~40 s after the viewer.

Both connect from the HA host with an ffmpeg/libav reader, so there is no
honest way to tell them apart (or to tell an idle HLS worker from a
viewer). Either way the hold after closing is well under a minute, so the
cap is not sized for it. The default cap of 5 min is a battery budget per
viewing: enough to check what is going on at the door, short enough that a
forgotten tab or an always-connected client costs little. The rolling
budget (3 caps) works out to 15 min of watch-driven live per hour.

On stream errors HA's worker reconnects with a growing backoff of 10, 20,
30, 40 s… (`STREAM_RESTART_INCREMENT` = 10 s). Those reconnects stay inside
the same capped session: a capped session only ends after 5 min without
clients (`WATCH_CAPPED_QUIET_S`), so the backoff cannot re-arm the cap.
Before the cap, a backoff step above 30 s (`WATCH_SESSION_GAP_S`) does end
the session and the reconnect starts a new one; the rolling hourly budget
bounds what that can cost.

**Battery cost of always-connected clients.** HA's *Preload stream*,
Frigate, and a go2rtc producer kept warm hold the TCP connection open
permanently. Each such client costs one cap of live video per viewing
session: after every HA restart, and after every return following 5 min or
more without clients. The rolling budget bounds the total at three caps an
hour. Once capped, a human viewer opening the camera while such a client
stays connected sees the idle still; pressing Wake gives them the normal
wake window. If a client is always connected, disable the option and rely
on motion and the Wake button.

Because the watched camera is often mid-wake with nothing to broadcast, the
stream server also notices closed clients without sending to them: each
client writer polls its socket for EOF while its queue is idle. Otherwise a
viewer who left during the wake would count as watching until the first
live packet.

Client presence comes from the stream server itself
(`StreamServer.presence()`: count, when it last went from zero to one, when
it last dropped to zero), not from what the snap loop happened to see. The
loop blocks for long stretches (initial frame grab, wake calls, status
polls); with loop-sampled presence a single zero-client instant after such
a stretch looked like a 30 s gap and reset the cap. A session's clock
starts when the client connected, but never before the previous session
ended.

## Talk

Talk (HA → camera speaker) rides the running live session; wire format in
[protocol.md](protocol.md#talk-uplink-kcp-path).

- **Queue only from outside the loop.** `TalkChannel`
  (`p2p_streamer/talk.py`) lives on the coordinator and is handed to each
  `P2PStreamer`; HA calls `begin` / `push` / `stop` (the streamer exposes
  the same as `talk_start` / `talk_push` / `talk_stop`). The live loop,
  which owns the socket and the non-thread-safe `KcpTunnel`, calls
  `drain()` right after the VVP heartbeat block: it sends `0x1350`, then
  frames paced at 40 ms (`TALK_OPEN_LEAD_S` 0.3 s after open, at most 3
  frames per iteration, resync instead of bursting after a stall), then
  `0x1351` once the queue is empty after a stop.
- **Never left open.** A talk session stops after `TALK_IDLE_TIMEOUT_S`
  (2 s) without audio to send and after `TALK_MAX_S` (60 s) at most. A
  session ending (reconnect, stop) sends `0x1351` if talk was open; a talk
  still wanted re-opens on the next session. The frame sequence lives on
  the channel, so it continues across sessions like the app's.
- **Live first.** `async_talk_start` wakes the camera if it is not live (the
  normal wake window) and waits up to 45 s for live frames before opening.
  While talking, the live window is held `TALK_LIVE_LINGER_S` (5 s) past
  the queued audio, and that live time is charged to the wake-when-watched
  budget like watching. A capped watch session does not block talk (it is
  an explicit action), but talk uses up the hour's budget.
- Talk frames are sent with a 1 s KCP expiry so late audio is dropped, not
  retransmitted (see protocol.md, KCP).
- PPCS cameras and cameras whose capability says `vtk` 0 or 2 are refused.

## Timing rules

- Camera timestamps can be unstable across stalls, especially with HEVC and
  AUTO. The muxer **generates stable video PTS** at the advertised FPS and
  caps audio lead so audio cannot run seconds ahead while video is stalled.
- **Video PTS stays monotonic across reconnects.** A single muxer feeds one
  unbroken TS stream for the whole live session, but a reconnect restarts the
  ffmpeg child. The muxer carries the PTS base forward instead of resuming at
  0, otherwise consumers see a backward jump — HA's `stream_worker` wrap-
  corrects it by +2³³ and aborts with a "Timestamp discontinuity" (ffplay
  tolerates it, so the debug harness never reproduces this).
- **Do not inject old video frames** to hide loss. It creates visible time
  travel.
- When the source stops sending frames, the integration can only recover
  KCP, request keyframes, reconnect, or wait. It cannot make missing
  high-bitrate camera frames appear without transcoding or fabricating
  video.
- AUTO startup should not publish a tiny stale seed immediately. It should
  wait briefly for the adaptive stream to settle so players open on current
  media.

## MPEG-TS fan-out

- Maintain one P2P session per camera stream and fan out the same MPEG-TS
  TCP stream to all consumers.
- The local TCP listener is health-checked whenever HA requests a stream
  source. If its accept thread stopped unexpectedly, it is recreated and HA
  receives the new port instead of repeatedly opening a dead endpoint.
  Entity updates only read the current port (`current_stream_port`) to
  decide whether HA should re-pick its WebRTC provider; they never run the
  health check, which takes a lock and may rebind on the event loop. The
  provider refresh task is tracked and cancelled when the entity is removed.
- New clients need **PAT/PMT plus a current keyframe seed** before live
  packets.
- Idle streams reuse the latest keyframe/still with the same advertised
  stream properties and low refresh cost; they should **not** push still
  frames at high FPS.
- Video is copied. Audio-only encoding (G.711 µ-law → AAC) is expected.
  **Avoid video transcoding** so low-power Home Assistant hosts remain
  usable.
