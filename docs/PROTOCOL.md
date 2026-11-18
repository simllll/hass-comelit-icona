# Comelit ICONA Bridge protocol reference

Reverse-engineered notes for the ICONA Bridge protocol as spoken by Comelit ViP
intercoms over **TCP/UDP port 64100**. Verified live against a **6742W "Mini ViP
handsfree Wifi"** (server-info `model: MnWi`, firmware `2.3.1`). Cross-references
the community 6701W notes (antoiba86) where behaviour differs.

> This documents the *local* protocol only. It exists so the integration is
> maintainable and portable across firmware; see "Firmware differences" for why
> a heavy protocol-abstraction layer is **not** needed.

## 1. Framing

Every message is an 8-byte header + body:

```
00 06            magic (constant)
LL LL            body length   (uint16, little-endian)
RR RR            request id    (uint16, little-endian)
00 00            padding
```

- **JSON body**: starts with `{` (0x7B). UTF-8, compact.
- **Binary body**: channel-management (`request id == 0`) or VIP signalling.

## 2. Channels

Open a channel with a COMMAND packet, use it, close with END.

| Name | wire type id | purpose |
|------|:-----------:|---------|
| `UAUT` | 7  | authentication / activation |
| `UCFG` | 2  | configuration (`get-configuration`) |
| `INFO` | 20 | `server-info` |
| `CTPP` | 16 | call/door control **and local call signalling** |
| `CSPB` | 17 | secondary control (opened alongside CTPP) |
| `PUSH` | 2  | push-token enrollment |
| `FRCG` | 7  | face recognition: parameters + monitor notifications (see §10) |
| `UADM` | ?  | admin (create user / generate code) — needs admin auth |
| `RTPC` | — | device-initiated: RTP control for video |
| `UDPM` | — | device-initiated: UDP media negotiation for video |

Channel open (COMMAND `0xABCD`):
```
cd ab            MessageType.COMMAND (LE16)
01 00            sequence = 1 (LE16)
TT TT TT TT      channel type id (LE32)
<name ascii>     e.g. "CTPP"
RR RR            request id (LE16)
00               trailing byte
[ 00  LEN(LE32)  <extra ascii> 00 ]   optional additional-data (e.g. apt addr)
```
The device echoes the request id as the channel's server id (used as the header
`request id` for subsequent data frames on that channel). Close = END `0x01EF`.

## 3. Authentication

On `UAUT`: `{"message":"access","user-token":"<32hex>","message-type":"request","message-id":2}`
→ `{"response-code":200,"response-string":"Access Granted"}`.

## 4. Configuration

On `UCFG`: `{"message":"get-configuration","addressbooks":"all","message-type":"request","message-id":3}`.
Returns `vip.apt-address`, `vip.apt-subaddress` (per identity!), and
`vip.user-parameters.{entrance,actuator,opendoor,rtsp-camera}-address-book`.

## 5. Activation — minting a dedicated token (no admin auth)

Create a user + code in the device web UI (`http://<ip>:8080/users.html`), then on
`UAUT` (the code is the credential — **no prior auth**):
```json
{"message":"user-cloud-activation","cloud-activation-code":"<CODE>",
 "description":"Home Assistant","message-type":"request","message-id":42}
```
→ `{"response-code":200,"response-string":"Success","user-token":"<32hex>"}`.

The subaddress for the new identity is read afterwards from `get-configuration`.
(Admin messages `activate-user` / `admin-fast-activation` on `UADM` require admin
auth — not used here.)

## 6. Door / actuator open (CTPP)

Open `CTPP` (extra-data = `apt-address`+`apt-subaddress`), send the init, then
`OPEN_DOOR` (`0x1800`) / `OPEN_DOOR_CONFIRM` (`0x1820`); actuators use a
`0x18..45BE` init variant. Addresses built from `apt-address` + `output-index`.

## 7. Local doorbell events (held CTPP registration)

Hold a `CTPP` registration and the device delivers incoming calls over it — **no
cloud**. Sequence:

1. Open `CTPP` with extra-data `apt-address`+`apt-subaddress` (e.g. `00000B061`).
   **The subaddress must match the authenticated identity.**
2. Send the init (`0x18C0`, flags `0x0011 0x0040`, addresses, our init timestamp).
3. Device replies `0x1800` (ACK) then `0x1860`/action `0x0010` (registration
   renewal).
4. **Answer every renewal** with a `0x1800` + `0x1820` ACK pair whose timestamp is
   **`init_ts + 0x01010000`** (6742W). Failing this → the device retransmits and
   eventually drops.
5. Keep the socket open (the device is quiet between renewals — don't self-timeout).

VIP frame layout: `prefix(LE16) ts(LE32) action(BE16) flags(BE16) … FFFFFFFF
caller\0 callee\0\0`.

Ring/event frames (device → client):

| prefix | action | meaning |
|:------:|:------:|---------|
| `0x18C0` | `0x0028` | incoming call (ring) |
| `0x1860` | `0x0001` | IN_ALERTING (ring) |
| `0x1840` | `0x0000` | call ended unanswered (missed call) |
| `0x1860` | `0x0003` | door opened / FSM |
| `0x1860` | `0x0010` | registration renewal (answer with ACK pair) |

The **caller** VIP address in the frame identifies the source: an entrance panel
(e.g. `00000100`) vs the apartment's own floor/"Etagen" station (`apt-address` +
sub, e.g. `00000B060`).

**Floor-call origin tag (6741W).** On kit/single-house firmware the floor-door
ring carries the **entrance panel's** address as the caller — byte-identical to a
building-door ring — so the address alone cannot distinguish them. The only
stable discriminator is a **two-ASCII-byte tag immediately before the
`FF FF FF FF` marker** in the `0x18C0` frame: `PP` = entrance panel, `FF` = floor
door ("fuoriporta"). `parse_ctpp_message` exposes this as `call_tag`; when it is
`FF` the integration reports the caller as the apartment's own address so the
floor-call routing / "Floor call" entity fires (issue #45).

**Addressing differs by system mode.** Apartment-block systems (e.g. 6742W,
`model: MnWi`) use plain numeric addresses (`00000100`). **Kit / single-house
mode** (e.g. 6741W, `model: MSVF`) uses an **`S` mode prefix** in the
configuration address book (`SB100001` entrance, `SB000001` apartment,
`SBIO0255` actuator), but the ring frame reports the caller **without** that
prefix (`B100001`). So caller↔address-book comparisons must tolerate a
dropped-prefix mismatch — the integration matches by equality / either-direction
prefix / either-direction suffix (`address_matches`).

> **Identity matters:** the wall-monitor token (`local-monitor-dev`) is already
> held by the physical monitor, so a duplicate registration is kicked. Use a
> dedicated app-class identity (see §5). The official app registers then closes
> and relies on push — an always-on HA client instead *holds* the registration.

## 8. Cloud push events (fallback)

`push-info` on `PUSH` enrolls a push token (`os-type`, `device-token`,
`bundle-id`, `apt-address`). The device then delivers rings via Comelit cloud →
FCM/APNS (requires internet). Payload: `event: "incoming-event"`, body
"Incoming call received", `data` with `call-id` + `connection-info`
(direct `ip:64100` + cloud mqtt/stun). Used as automatic fallback when the local
held registration can't be established.

## 9. Video (on-demand)

Video is negotiated on `CTPP` and delivered as **H.264 RTP over UDP**, wrapped in
the ICONA header: `[ICONA 8B][RTP 12B][H.264]`, payload type 99, FU-A/STAP-A/single
NALs. Sequence: `call_init 0x18C0/0x0028` → `codec 0x1840/0x0008` →
`rtpc_link 0x1840/0x000A` → `video_config 0x1840/0x001A`; the device opens `RTPC`
+ `UDPM` channels and streams UDP media (`intercom:64100 → client:<udp port>`).
Depacketise (reorder by RTP seq, reassemble FU-A) → Annex-B H.264 → 320×240.
No RTSP URL is exposed unless `rtsp-camera-address-book` is populated.

## 10. Face recognition (FRCG)

Monitors with face recognition list `face-recognition-channel` in
`server-info.capabilities` (e.g. 6741W `MSVF`). The official app opens `FRCG`
(wire type 7) on every session, after CTPP/CSPB, and asks the parameters:
```json
{"message":"rcg-get-params","message-type":"request","message-id":121}
```
The monitor replies, then sends a `rcg-params-result` notification with
`parameters.enable` (bool) and `parameters.threshold` (int) (seen on a 6741W,
firmware 2.1.0). When a face is detected at a call it pushes, on the same
channel (PCAP of the Android app on a 6701W, firmware 2.1.0):
```json
{"message":"rcg-detected-recognition","message-type":"notification","message-id":1,
 "data":{"session-id":"...","external-id":"unknown","recognized":false,"activated":false,
         "similarity":"3.079382","confidence":"...",
         "image-url":"https://eps.cloud.comelitgroup.com/servicerest/facerecognition/getfaceimagebykey/v1/<key>.jpeg",
         "box":{"width":"...","height":"...","top":"...","left":"..."}}}
{"message":"rcg-detected-image","message-type":"notification","message-id":2,
 "image":{"path":"/etc/comelit/recognition/detected/unknown/<timestamp>.jpg",
          "external-id":"unknown","changed":true}}
```
Numbers arrive as strings. The match is made with Comelit's cloud (the image
URL points there), so it needs internet on the monitor. The integration relays
these as `comelit_intercom_face` events when the "Face recognition events"
option is on; the image URL is not forwarded. Sources: mnestrud/antoiba86 6701W
protocol reference (PCAP), davidebr90/pycomeca (6741W), grdw/viper-client.

**Verified on a 6741W (`MSVF`, firmware 2.1.0)** through the integration's
shared connection (HA identity, no app session open):
- `rcg-get-params` gets a `response-code: 200` reply, then
  `rcg-params-result` with `{"enable": true, "threshold": 95}`;
- an enrolled person ringing produced, about 6 s after the ring:
  ```json
  {"message":"rcg-detected-recognition","message-type":"notification","message-id":1,
   "notification-code":200,"notification-string":"Recognition Detected",
   "data":{"session-id":"114671927","external-id":"alice","recognized":true,
           "activated":true,"similarity":"99.8128","confidence":"99.9732",
           "image-url":"<omitted>", ...}}
  ```
  `external-id` is the name given to the face on the monitor/app (lowercase
  here; anonymised above). In this test `activated: true` came with the
  monitor's own lock release (several `0x1860/0x0003` VIP messages followed
  on CTPP), but it is no proof of it: after the lock-release action was
  removed for every face on the monitor, later recognitions still reported
  `activated: true`. No `rcg-detected-image` was received in that test.

## 11. Talk-back audio on an outbound call ("speak")

Used by `comelit_intercom.speak`. What is verified and what is only read from
captures is marked. Door tests on a 6741W (below): the panel plays the audio
on an answered ring over TCP, and nothing on a call opened from Home Assistant.

1. **Call.** Same outbound sequence as the live view (§9): call init, codec,
   RTPC link, video config. The device opens **its own RTPC channel**
   (device-initiated COMMAND); its request id is where our audio goes.
2. **Answer / talk enable (0x70).** Sent once video flows. The live view uses
   `[0x1840][ts LE32][inner_len BE16][0x0070][caller\0][01 00][FF FF FF FF][caller\0][entrance\0\0]`.
   The app's frame has a **4-byte flag** `01 00 00 00` (renewal: prefix
   `0x1860`, flag `00 00 00 00`), i.e. two more zero bytes, same `inner_len`:
   read from the app's outbound-call-with-audio captures of a 6742W and a 6741W
   (upstream PR #44, not merged) and identical to the inbound answer frame of
   the PCAP2 capture (verified working on the inbound answer). Speak sessions
   send the app frame; the live view is unchanged.
3. **TX audio.** RTP v2, PT 8 (G.711 A-law), 160-byte payload every 20 ms, seq
   +1, timestamp +160, fixed SSRC, addressed to the device's RTPC request id:
   - over **TCP**: an ICONA frame on the shared connection whose header request
     id is the device's RTPC (what PR #44 derived from the app capture);
   - over **UDP**: the same header on the media socket (what the inbound answer
     uses, verified on a 6701W).
   `auto` follows the transport of the panel's video. The app streams
   continuously (silence when idle) and, per the PR #44 analysis of the 6741W
   capture, that panel only starts sending its own audio (RX, on our RTPC1)
   after receiving ours, answering with its own 0x70 peer frame. Speak streams
   silence from the answer on and treats the first RX audio packet as proof
   that the audio path is open (`rx_audio`, `device_ack` in the response).
4. **Lease renewal** (~30 s, `0x1840/0x0003` CALL_END → inline
   re-establishment): the device opens a new RTPC; TX follows the newest one
   and the renewal `0x1860/0x0070` uses the app frame too.
5. **Hang-up.** As for the live view and snapshots, the channels are dropped
   locally; no `0x002D` hang-up frame is sent. Seen on a 6741W: after the
   integration released the on-ring preview, the panel kept streaming video to
   it for ~6 s, until its own call ended.

**When is the panel free?** Timeline of a recognised face (6741W, firmware
2.1.0, live log):

| time | event |
|------|-------|
| 16:38:24 | ring `0x18C0/0x0028`; the integration runs the inbound preview (snapshot) |
| 16:38:30 | FRCG `rcg-detected-recognition`, `activated: true` |
| 16:38:34 | preview released; the six `0x1800/0x0000` frames then read by the listener are ACKs queued during the preview, not the end of the call |
| 16:38:40.35 | last video packet from the panel: its call with the monitor ends |
| 16:38:40.5 | `0x1860/0x0003` sub `0x000E` (door opened / call FSM); retransmitted 4 more times over 7.4 s (the listener does not ACK it) |

The frame bytes: `60 18 | ts | 00 03 | 00 0E | 00 00 | FF FF FF FF | SB000001 | SB0000011`.
So speak treats the panel as busy from a ring until the **first**
`0x1860/0x0003` with flags `0x000E` after it, plus 3 s, or 90 s after the ring
when no such frame is seen; never while a session is active. Other flags are
only logged: what the panel sends to HA when the monitor *answers* a ring is
not known yet. If the panel still refuses the call, speak retries once 3 s
later (the frame is retransmitted for ~7.4 s, unacknowledged).

**Answer mode.** Instead of a new outbound call, speak can answer the incoming
call HA holds after a ring (the on-ring preview, or an auto-answered call):
- a preview is accepted with the same frames as the inbound answer (PCAP2,
  steps 13-18): PEER (inbound layout), call_accepted `0x1840/0x0002`, ACKs of
  the device's `0x000A`/`0x000E` with the transformed timestamp, wait for the
  device's RTPC, CTPP monitor;
- an auto-answered call only swaps its silence/mic sender for speak's;
- TX follows the transport of the call's media by default (`auto`), as on
  the outbound call. A 6741W sends an answered ring's media over TCP and
  plays only TCP audio (door test of 2026-10-26, below); the silence/mic sender of the
  Answer button still uses UDP;
- a `0x1840/0x0003` CALL_END on such a call ends it (no outbound
  re-establishment); teardown is local, as for the other sessions.
Answering makes HA the answering unit: on a 6741W the monitor stops ringing,
and it may skip its own actions.

Open points: why a 6741W plays nothing on a call opened from Home Assistant
(outbound "self-ignition"), and whether other models play it, perhaps only
after the next lease renewal (~30 s), as an older note on the 6701W says for
RX audio.

**Door test, 6741W, 2026-10-26:**
- outbound "call": the panel switches on but plays no audio (TCP and UDP);
- "answer": heard only when HA answered within tenths of a second of the face
  recognition (0.16 s: heard; 1.3-5.8 s: not heard). The monitor seems to act
  on the call about 1 s after recognising, even with its door action removed
  (the FRCG event still says `activated: true`). So speak answers first and
  builds the audio meanwhile;
- the TAP open (`0x18C0` registration + `0x1800`/`0x1820`/`0x18C0` door
  frames, §6) sent **during a ring** did not trip the lock, three times, while
  the same command at idle works. The panel still answered each one with
  `0x1860/0x0003` from `SB100001` (flags `0x000E`): that frame is no proof
  that the relay tripped.

**Door test, 6741W, 2026-10-26, later (answer first, in-call door open):**
- "answer" over UDP (then the default of `auto` in answer mode): the panel
  switches its speaker on, no voice. Over TCP the voice is heard, twice, also
  with `open_door` (greeting heard, gate opened). The panel's media on these
  calls arrive over TCP (`Media transport = TCP (RTPC2)`): `auto` now
  follows them in answer mode too;
- `lock.open` during a ring with the preview up: HA answers, sends
  `0x1840/0x000D`, the gate opens within 2-3 s. The panel sends **no ACK and
  no CALL_END** for it (§12).

## 12. Door open inside a call (`0x1840/0x000D`)

```
[LE16 0x1840] [LE32 counter] [BE16 0x000D] [BE16 0x002D]
[door address padded to 10] [LE32 output-index] [FF FF FF FF]
[our address padded to 10] [apartment address padded to 10]
```

Single frame on the call's CTPP, call counter +`0x00010000`; the panel ACKs
with `0x1800/0x0000` sent from the door's unit (e.g. `SB100001`), which is how
the integration tells the door's ACK from the bridge's own (`SB000001`).
Verified (antoiba86, app PCAPs `camera_feed_with_open_door_local` and
`notification+opendoorwhilevideo`) inside a call the app has answered, i.e.
an outbound view after the `0x70`; the mnestrud 6701W reference lists it as
"door open (during active video call)". Nothing shows it during an
*unanswered* inbound preview, and the TAP open sent during a ring was ignored
(above). So when the call is a ring preview the integration answers it first
(the same frames as speak's answer: PEER, `call_accepted`, ACKs, device RTPC,
then the silence sender as after the Answer button), sends `0x000D`, waits
at most 2 s in all for the ACK and for the panel's CALL_END `0x1840/0x0003`
sub `0x000E` (the relay fired, app PCAP), and hangs up (local teardown:
nothing is sent to the panel) unless speak or a viewer uses the call. If the
ring's call already ended (end frame queued and no more video), the preview
is closed and the idle TAP path is used. Neither the ACK nor a following
`0x1860/0x0003` proves that the lock tripped, and their absence proves
nothing either: a 6741W sends no ACK and no CALL_END for an in-call open, and
its lock trips (door test of 2026-10-26). On outgoing calls (live view, snapshot,
speak `call`) the integration does not send `0x000D`, although the app does:
with no ACK from a 6741W it could not tell whether the usual TAP is still
needed, so it sends only the TAP, as before (one door command per request).

## Firmware differences (why no heavy abstraction layer)

The core protocol (framing, channels, auth, config, door open, VIP signalling,
video) is **identical** across 6701W and 6742W. Known deltas are small and
parameterisable:

| aspect | 6701W (antoiba) | 6742W (`MnWi`) |
|--------|-----------------|----------------|
| CTPP renewal ACK increment | `0x01000000` | **`0x01010000`** |
| capabilities | local | `cloudnext-device`, `push-notifications-channel`, `fast-activation-channel` |
| events | local CTPP only | local CTPP **or** cloud FCM |

Recommendation: keep one client and **parameterise the few differing constants**
(e.g. the ACK increment) rather than maintaining separate protocol
implementations. Capability-gate optional features from `server-info.capabilities`.
