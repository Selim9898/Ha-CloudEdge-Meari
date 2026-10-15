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
| this file | LOOX-FORK.md | Fork only |

Behaviour details are in [docs/streaming.md](docs/streaming.md) (sections
"Camera Awake reflects what the stream serves" and "Wake when watched").
The manifest still says `0.3.3`. A version bump triggers the release
workflow, so we leave it alone.

### Wake when watched: what to expect at the test home

- Opening the camera wakes it (about 15–30 s to live) and keeps it live
  while the stream is open, up to 5 min per viewing session and 15 min of
  watch-driven live per rolling hour.
- **WebRTC viewers release the camera within ~20 s** of closing the view.
  **HLS viewers hold it live ~5 min** after closing, because HA's stream
  worker keeps the source open that long (`OUTPUT_IDLE_TIMEOUT` = 300 s).
  The integration cannot tell the two apart, which is why the cap defaults
  to 5 min. Check which one the Loox app and the HA dashboard actually use.
- Anything that holds the stream open permanently (HA *Preload stream*,
  Frigate, a warm go2rtc producer) uses one cap after each HA restart and
  then blocks further watch wakes until it has been gone for 5 min. Turn
  the *Wake when watched* switch off if such a client exists.
- After the cap, Wake still gives the normal wake window (Motion Timeout).

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

Both need a packet capture of the official CloudEdge app before any code is
written.

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

**Two-way talk (phone mic to camera speaker).** Static research is in
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

What we need: an iPhone capture (`rvictl` + `tcpdump`, phone and camera on
the same LAN) of the official app doing live view, about 5 s of talk with a
tone, then release. The research doc has the full capture and test plan.
Captures contain host-key-derived MD5s and licence ids, so keep them out of
any repo.
