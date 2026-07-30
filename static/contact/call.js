function createCallSession({ wsUrl, onIncomingCall, onStateChange }) {
  const STUN_SERVERS = [{ urls: "stun:stun.l.google.com:19302" }];

  let ws = null;
  let pc = null;
  let localStream = null;
  let remoteAudioEl = null;
  let reconnectTimer = null;
  let closedByUser = false;

  function connect() {
    ws = new WebSocket(wsUrl);
    ws.onmessage = async (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === "offer") {
        onIncomingCall(msg.sdp);
      } else if (msg.type === "answer") {
        if (pc) await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
      } else if (msg.type === "ice") {
        if (pc && msg.candidate) {
          try {
            await pc.addIceCandidate(new RTCIceCandidate(msg.candidate));
          } catch (e) {
            console.error("ICE candidate error", e);
          }
        }
      } else if (msg.type === "hangup") {
        teardown();
        onStateChange("ended");
      }
    };
    ws.onclose = () => {
      if (!closedByUser) {
        clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(connect, 3000);
      }
    };
  }

  function send(msg) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }

  function teardown() {
    if (pc) {
      pc.close();
      pc = null;
    }
    if (localStream) {
      localStream.getTracks().forEach((t) => t.stop());
      localStream = null;
    }
    if (remoteAudioEl) {
      remoteAudioEl.remove();
      remoteAudioEl = null;
    }
  }

  async function createPeerConnection() {
    pc = new RTCPeerConnection({ iceServers: STUN_SERVERS });

    pc.onicecandidate = (e) => {
      if (e.candidate) send({ type: "ice", candidate: e.candidate });
    };

    pc.ontrack = (e) => {
      if (!remoteAudioEl) {
        remoteAudioEl = document.createElement("audio");
        remoteAudioEl.autoplay = true;
        remoteAudioEl.playsInline = true;
        document.body.appendChild(remoteAudioEl);
      }
      remoteAudioEl.srcObject = e.streams[0];
    };

    pc.oniceconnectionstatechange = () => {
      if (!pc) return;
      const state = pc.iceConnectionState;
      if (state === "connected" || state === "completed") {
        onStateChange("connected");
      } else if (state === "failed") {
        onStateChange("failed");
      } else if (state === "disconnected") {
        onStateChange("disconnected");
      }
    };

    localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    localStream.getTracks().forEach((t) => pc.addTrack(t, localStream));
  }

  async function startCall() {
    onStateChange("calling");
    try {
      await createPeerConnection();
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      send({ type: "offer", sdp: offer });
    } catch (e) {
      console.error(e);
      teardown();
      onStateChange("mic_error");
    }
  }

  async function acceptCall(offerSdp) {
    onStateChange("connecting");
    try {
      await createPeerConnection();
      await pc.setRemoteDescription(new RTCSessionDescription(offerSdp));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      send({ type: "answer", sdp: answer });
    } catch (e) {
      console.error(e);
      teardown();
      onStateChange("mic_error");
    }
  }

  function declineOrHangup() {
    send({ type: "hangup" });
    teardown();
    onStateChange("idle");
  }

  function close() {
    closedByUser = true;
    clearTimeout(reconnectTimer);
    teardown();
    if (ws) ws.close();
  }

  connect();

  return { startCall, acceptCall, declineOrHangup, close };
}
