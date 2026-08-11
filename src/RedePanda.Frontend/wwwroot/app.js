"use strict";

// The browser never speaks Kafka. It only ever calls this frontend's own origin under /api,
// which Caddy proxies to the backend. Open the network tab during the demo to see that.
const API = "/api";

const els = {
    join: document.getElementById("join"),
    nickname: document.getElementById("nickname"),
    room: document.getElementById("room"),
    chat: document.getElementById("chat"),
    currentRoom: document.getElementById("current-room"),
    messages: document.getElementById("messages"),
    send: document.getElementById("send"),
    text: document.getElementById("text"),
    error: document.getElementById("error"),
    status: document.getElementById("status"),
    leave: document.getElementById("leave"),
};

let source = null;
let nickname = "";
let room = "";

function setStatus(state, label) {
    els.status.textContent = label;
    els.status.className = `status status--${state}`;
}

function showError(message) {
    els.error.textContent = message;
    els.error.hidden = !message;
}

function addMessage(message) {
    const item = document.createElement("li");

    const meta = document.createElement("span");
    meta.className = "meta";
    // Timestamps arrive as UTC and are rendered in the viewer's local time.
    const at = new Date(message.timestamp);
    meta.textContent = `${at.toLocaleTimeString()} ${message.nickname}`;

    const body = document.createElement("span");
    body.className = "body";
    // textContent, never innerHTML: this string came from another user.
    body.textContent = message.text;

    if (message.nickname === nickname) {
        item.classList.add("own");
    }

    item.append(meta, body);
    els.messages.append(item);
    els.messages.scrollTop = els.messages.scrollHeight;
}

function connect() {
    source = new EventSource(`${API}/stream?room=${encodeURIComponent(room)}`);

    source.onopen = () => setStatus("online", "verbunden");

    source.onmessage = (event) => {
        try {
            addMessage(JSON.parse(event.data));
        } catch {
            // A malformed frame must not kill the stream.
        }
    };

    // EventSource reconnects on its own; this only reflects that in the UI.
    source.onerror = () => {
        setStatus(
            source.readyState === EventSource.CLOSED ? "offline" : "connecting",
            source.readyState === EventSource.CLOSED ? "getrennt" : "verbinde neu…",
        );
    };
}

els.join.addEventListener("submit", (event) => {
    event.preventDefault();
    nickname = els.nickname.value.trim();
    room = els.room.value.trim();
    if (!nickname || !room) {
        return;
    }

    els.currentRoom.textContent = room;
    els.join.hidden = true;
    els.chat.hidden = false;
    els.text.focus();
    setStatus("connecting", "verbinde…");
    connect();
});

els.send.addEventListener("submit", async (event) => {
    event.preventDefault();
    const text = els.text.value;
    if (!text.trim()) {
        return;
    }

    showError("");
    els.text.value = "";

    try {
        const response = await fetch(`${API}/messages`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ room, nickname, text }),
        });

        if (!response.ok) {
            const problem = await response.json().catch(() => ({}));
            showError(problem.error ?? `Senden fehlgeschlagen (HTTP ${response.status}).`);
            els.text.value = text;
        }
    } catch {
        showError("Backend nicht erreichbar.");
        els.text.value = text;
    }
});

els.leave.addEventListener("click", () => {
    source?.close();
    source = null;
    els.messages.replaceChildren();
    els.chat.hidden = true;
    els.join.hidden = false;
    showError("");
    setStatus("offline", "getrennt");
});
