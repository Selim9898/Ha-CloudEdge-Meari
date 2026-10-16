# Loox fork (branch `loox-dogan`)

NoorIQ's test fork of [ajeanney/Ha-CloudEdge-Meari](https://github.com/ajeanney/Ha-CloudEdge-Meari)
(MIT), used to run ieGeek battery cameras at one home through Home Assistant
and the Loox app. It tracks upstream and carries fixes we want to offer back.
Each change is a separate commit so it can go upstream as its own PR.

## How it differs from upstream

Based on upstream `v0.3.3` (5bc9f97). On top of that:

| Commit | What | Upstream |
|--------|------|----------|
| `207256e` | fix(camera): offer WebRTC once the stream server has a port | Open PR |
| `b95dae9` | fix(awake): Camera Awake reports live frames, not wake requests; `live_until` attribute | Not yet offered |
| `1d59d87` | feat(stream): wake when watched, capped per viewing session, plus a config switch and options | Not yet offered |
| `ffb0a2f` | LOOX-FORK.md | Fork only |
| `f6748f7` | fix(watch-wake): window survives a slow wake; wakes rate-limited | Fold into the wake-on-watch PR |
| `4599299` | fix(watch-wake): client presence from the stream server | Fold into the wake-on-watch PR |
| `6e67e26` | fix(watch-wake): sticky cap, rolling hourly budget, state survives reloads | Fold into the wake-on-watch PR |
| `b5eae08` | fix(watch-wake): default cap 5 min; viewer hold times documented (HLS figure corrected later) | Fold into the wake-on-watch PR |
| `277304b` | fix(awake): `live_until` stored as a whole-second wall clock | Fold into the awake PR |
| `c50d19c` | fix(camera): side-effect-free port read, tracked refresh task | Fold into the WebRTC PR |
| `c096f28` | docs(fork): commit table | Fork only |
| `8def0f6` | fix(watch-wake): 90 s keep-alive wake (removed in `32ac655`); idle still over stalls | Fold into the wake-on-watch PR |
| `a0fbff5` | docs(watch-wake): HLS idle timeout is 30 s | Fold into the wake-on-watch PR |
| `1117d94` | fix(stream): START_LIVE once on H.264, like the app | Own PR (capture-backed) |
| `94e1ece` | fix(stream): webrtc hello every 21 s, status query every 120 s while live | Own PR (capture-backed) |
| `32ac655` | fix(watch-wake): drop the 90 s re-wake | Fold into the wake-on-watch PR |
| `e8d5db4` | fix(kcp): resend only segments older than 200 ms; expire late audio | Talk PR |
| `baad458` | feat(talk): talk VVP commands, uplink frame builder, u-law encoder, capture fixtures | Talk PR |
| `7963190` | refactor(coordinator): idle still loop moved to `coordinator/idle.py` | Talk PR (prep) |
| `336cd8b` | feat(talk): queue-only talk API drained by the KCP live loop | Talk PR |
| `c15776c` | feat(talk): admin-only websocket talk API and `talk_tone` service | Talk PR |
| `8be1ea8` | docs: keep-alive status, stream-flag lever, talk test plan | Fork only |
| `ddb14f7` | fix(stream): keep the 30 s START_LIVE resend on PPCS; drop it on KCP only | With `1117d94` |
| `2e882a9` | fix(kcp): never drop unACKed segments; timer-driven resend | Talk PR |
| `0032e60` | fix(talk): drop stale audio before KCP; never reopen talk for leftovers; report dropped frames | Talk PR |
| `4ddc46f` | fix(watch-wake): lock deadline and budget writes; deadline only moves later | Fold into the wake-on-watch PR |
| `99e1a0b` | fix(watch-wake): recovery wake when a watched live session stalls | Fold into the wake-on-watch PR |
| `5399775` | fix(talk): budget refusal, 5 min talk per hour, 60 s per live view | Talk PR |
| `e1b1d0b` | fix(signaling): read keep-alive replies whole or reset the connection | With `94e1ece` |
| `7b083fe` | fix(stream): serve MPEG-TS over HTTP so go2rtc reports a producer url; WebRTC offers stop failing | Own PR (HA + go2rtc 1.9.14 bug) |

Behaviour details are in [docs/streaming.md](docs/streaming.md) (sections
"Camera Awake reflects what the stream serves" and "Wake when watched").
The manifest still says `0.3.3`. A version bump triggers the release
workflow, so we leave it alone.

### Wake when watched: what to expect at the test home

- Opening the camera wakes it (about 15–30 s to live) and keeps it live
  while the stream is open, up to 5 min per viewing session and 15 min of
  watch-driven live per rolling hour.
- **WebRTC viewers release the camera within ~20 s** of closing the view.
  **HLS viewers hold it live ~40 s** after closing: HA's stream worker
  keeps the source open 30 s (`OUTPUT_IDLE_TIMEOUT`), plus ~10 s of linger
  here. The integration cannot tell either from a viewer still watching.
  The 5 min default cap is a battery budget per viewing, not sized for
  either hold. Check which player the Loox app and the HA dashboard use.
- Anything that holds the stream open permanently (HA *Preload stream*,
  Frigate, a warm go2rtc producer) uses one cap after each HA restart and
  then blocks further watch wakes until it has been gone for 5 min. Turn
  the *Wake when watched* switch off if such a client exists.
- After the cap, Wake still gives the normal wake window (Motion Timeout).
- The camera ended live on its own after ~2 min in the 15 Oct live test.
  The fix now copies the official app's live keep-alives (webrtc hello
  every 21 s, status query every 120 s, START_LIVE only once); the 90 s
  re-wake was removed because the app never wakes during live. If live
  stalls anyway, viewers get the idle still after 5 s instead of a frozen
  frame and a recovery wake goes out at once (at most every 30 s). Confirm
  past 2:13 in the next live test, and check the log for "recovery wake".

### Live keep-alive after the app capture (to verify at the test home)

A capture of the official ieGeek app (3 min 47 s of live without a gap)
showed what we were missing while live: a routed webrtc hello every ~21 s
and a status query every ~120 s, and START_LIVE re-sent every 30 s on H.264
where the app sends it once. All three now match the app. Test past 2:13
with a viewer open, one change at a time if it still stops.

Not changed yet, in order of what to try next if live still stops:

1. **ieGeek START_LIVE stream flag.** The ieGeek 6.2.3 app sends flag `0`
   (byte `0x39`); the integration's ieGeek profile sends `1`. Next lever.
2. **ICE connectivity checks.** After ~10 s the app switches from Binding
   Requests every 1 s to a STUN Binding Indication every 30 s. We keep 1 s
   requests on purpose: our KCP retransmission rides on their Binding
   responses, so switching would first need its own retransmit timer, and
   the capture only shows the healthy LAN pattern, not that the faster
   cadence hurts. Low risk to leave.

### Talk (to verify at the test home)

Talk is implemented for KCP-path cameras from the same capture: VVP
`0x1350` / `0x1351` and byte-exact 40 ms µ-law frames. Limits: 60 s per
live view, 5 min of talk per hour, refused when the camera's hourly live
budget is used up; audio more than 2 s late is dropped, never sent. Try it first with
the admin service `cloudplus.talk_tone` (800 Hz, 2 s) while the camera is
live, then the websocket API from a client (`cloudplus/talk/start`,
`audio`, `stop`; see README). Raise the camera's Speaker Volume entity if
the tone is quiet. PPCS cameras are not supported.

## Install on a Home Assistant box

**HACS (custom repository)**

1. HACS → ⋮ → *Custom repositories* → add
   `https://github.com/Selim9898/Ha-CloudEdge-Meari`, type *Integration*.
2. Open the repository and choose *Download*. HACS installs the repo's
   default branch or a release. To get `loox-dogan`, either pick it under
   *Need a different version?* if HACS lists it, or make `loox-dogan` the
   fork's default branch on GitHub first.
3. Restart Home Assistant.

If upstream `cloudplus` is already installed through HACS, remove it first.
Both use the same domain, and only one copy can live in
`custom_components/cloudplus`. HA keeps the config entries and entities across
the swap.

**Manual copy**

Copy `custom_components/cloudplus/` from this branch to
`/config/custom_components/cloudplus/` on the HA box, replacing the old
folder, then restart HA.

**Use a secondary CloudEdge account for HA.** The Meari MQTT broker allows
one session per account. If HA and a phone share an account, motion events
stop arriving on one of them. Create a second account, share the cameras to
it from the owner's app, and add that account to HA.

## Next planned items

SD-card playback still needs a packet capture of the official app before
any code is written. Talk is implemented (see above) and needs a live test.

**SD-card playback as an HA media source.** The goal is to browse and play
the camera's SD recordings from HA's media browser, then from Loox. Nothing
in the repo implements it yet. The IoT codes only cover record mode and
format (`meari_commands.py`). What we need:

- a capture of the app listing a day's recordings and playing one clip
  (the list command, the playback start/seek/stop VVP commands, and how
  playback frames differ from live)
- confirmation that playback rides the same P2P session type as live
- then a `media_source` platform that lists days and clips and streams a
  clip through the existing muxer

**Two-way talk: background (implemented since).** Static research is in
`/Users/selim/Developer/Loox-Mobile/.claude/worktrees/cameras-1015/docs/efforts/active/cameras-1015/research/MEARI-TALK-RESEARCH.md`
(in the Loox repo, not here). In short:

- On legacy PPCS cameras the protocol is nearly byte-exact from the 2018 SDK:
  VVP `0x1350` opens talk, `0x1351` closes it, and audio goes as 320-byte
  G.711 µ-law frames every 40 ms with a 32-byte header on channel 1.
- Our ieGeek cameras use the newer MsgSvr/ICE/KCP transport. There the open
  command, the uplink frame layout inside IVA data, and any uplink
  encryption are all unverified.
- Talk should share the live P2P session, not open a second one.
- `vtk` in the capability JSON says whether the camera supports talk.

The 15 Oct capture settled the KCP path: open/close are plain VVPs, audio
frames are raw KCP payloads (not IVA), encrypted with the static stream key
from `0x28`. Sanitized fixtures are in `tests/fixtures/talk/`. Raw captures
contain host-key-derived MD5s and licence ids, so keep them out of any
repo. PPCS talk still needs a PPCS capture.
