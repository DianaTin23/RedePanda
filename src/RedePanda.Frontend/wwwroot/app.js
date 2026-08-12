"use strict";

// The browser never speaks Kafka. It only ever calls this frontend's own origin under /api,
// which Caddy proxies to the backend. Open the network tab during the demo to see that.
const API = "/api";

// Mirrors ChatMessage.DefaultMaxTextLength in RedePanda.Contracts. Both this and the maxlength
// attribute count UTF-16 code units, which is also what C#'s string.Length counts, so the
// counter below never disagrees with the server about the limit.
const MAX_TEXT_LENGTH = 500;
const COUNTER_FROM = 400;
const COUNTER_WARN = 480;

// Consecutive messages from one person inside this window share a single header.
const GROUP_WINDOW_MS = 5 * 60 * 1000;

// How close to the bottom still counts as "following the conversation".
const NEAR_BOTTOM_PX = 80;

const THEME_KEY = "redepanda.theme";

const els = {
    join: document.getElementById("join"),
    nickname: document.getElementById("nickname"),
    room: document.getElementById("room"),
    roomChip: document.getElementById("room-chip"),
    currentRoom: document.getElementById("current-room"),
    chat: document.getElementById("chat"),
    empty: document.getElementById("empty"),
    messages: document.getElementById("messages"),
    jump: document.getElementById("jump"),
    composer: document.getElementById("composer"),
    banner: document.getElementById("banner"),
    error: document.getElementById("error"),
    send: document.getElementById("send"),
    sendButton: document.querySelector(".send__button"),
    text: document.getElementById("text"),
    counter: document.getElementById("counter"),
    status: document.getElementById("status"),
    leave: document.getElementById("leave"),
    theme: document.getElementById("theme"),
};

let source = null;
let nickname = "";
let room = "";

// The open group a further message from the same person can be appended to.
let group = null;
let dayKey = "";

// Distinguishes "connecting for the first time" from "the connection dropped", which is the
// state the pod-delete demo is about.
let hasConnected = false;

const timeFormat = new Intl.DateTimeFormat("de-DE", { hour: "2-digit", minute: "2-digit" });
const dateFormat = new Intl.DateTimeFormat("de-DE", { weekday: "long", day: "numeric", month: "long" });
const instantFormat = new Intl.DateTimeFormat("de-DE", { dateStyle: "long", timeStyle: "medium" });

const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

// ---- Theme -------------------------------------------------------------------------------------
// Unset means "follow the operating system"; the stylesheet handles all three states.

function readStoredTheme() {
    try {
        return localStorage.getItem(THEME_KEY);
    } catch {
        // Storage can be unavailable in a private window. Following the OS is a fine fallback.
        return null;
    }
}

function applyTheme(theme) {
    if (theme === "dark" || theme === "light") {
        document.documentElement.dataset.theme = theme;
    } else {
        delete document.documentElement.dataset.theme;
    }
}

function resolvedTheme() {
    return document.documentElement.dataset.theme
        ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}

applyTheme(readStoredTheme());

els.theme.addEventListener("click", () => {
    const next = resolvedTheme() === "dark" ? "light" : "dark";
    applyTheme(next);
    try {
        localStorage.setItem(THEME_KEY, next);
    } catch {
        // The choice still applies to this page; it just will not survive a reload.
    }
});

// ---- Presentation helpers ----------------------------------------------------------------------

/** Stable per-nickname hue. Lightness and chroma come from the theme, so every hue stays legible. */
function hueFor(name) {
    let hash = 0;
    for (const character of name) {
        hash = (Math.imul(hash, 31) + character.codePointAt(0)) | 0;
    }
    return ((hash % 360) + 360) % 360;
}

function dayOf(date) {
    return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`;
}

function dayLabel(date) {
    const today = new Date();
    if (dayOf(date) === dayOf(today)) {
        return "Heute";
    }

    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);
    if (dayOf(date) === dayOf(yesterday)) {
        return "Gestern";
    }

    return dateFormat.format(date);
}

function isNearBottom() {
    return els.chat.scrollHeight - els.chat.scrollTop - els.chat.clientHeight < NEAR_BOTTOM_PX;
}

/**
 * Following an incoming message is instant on purpose: an animated scroll is still running when
 * the next message arrives, so isNearBottom() would measure the animation rather than the
 * reader's position and wrongly conclude they had scrolled away. Only the jump button animates.
 */
function scrollToEnd(smooth = false) {
    els.chat.scrollTo({
        top: els.chat.scrollHeight,
        behavior: smooth && !reducedMotion.matches ? "smooth" : "auto",
    });
    els.jump.hidden = true;
}

// ---- Rendering ---------------------------------------------------------------------------------

function addDaySeparator(at) {
    const item = document.createElement("li");
    item.className = "daysep";

    const label = document.createElement("span");
    label.textContent = dayLabel(at);

    item.append(label);
    els.messages.append(item);
}

/** Opens a new message group and returns the element further messages are appended to. */
function startGroup(message, at, own) {
    const item = document.createElement("li");
    item.className = own ? "msg msg--own" : "msg";
    item.style.setProperty("--nick-h", String(hueFor(message.nickname)));

    const avatar = document.createElement("span");
    avatar.className = "msg__avatar";
    avatar.setAttribute("aria-hidden", "true");
    // Array.from, not [0]: a nickname may begin with an emoji or another astral-plane character,
    // and indexing would take half a surrogate pair and render a replacement glyph.
    avatar.textContent = (Array.from(message.nickname)[0] ?? "?").toUpperCase();

    const name = document.createElement("span");
    name.className = "msg__name";
    name.textContent = own ? `${message.nickname} (du)` : message.nickname;

    const time = document.createElement("time");
    time.className = "msg__time";
    time.dateTime = at.toISOString();
    // The list shows HH:MM; the exact instant stays available on hover.
    time.title = instantFormat.format(at);
    time.textContent = timeFormat.format(at);

    const head = document.createElement("div");
    head.className = "msg__head";
    head.append(name, time);

    const body = document.createElement("div");
    body.className = "msg__body";
    body.append(head);

    item.append(avatar, body);
    els.messages.append(item);
    return body;
}

function addMessage(message) {
    // Timestamps arrive as UTC and are rendered in the viewer's local time.
    const at = new Date(message.timestamp);
    if (Number.isNaN(at.getTime())) {
        return;
    }

    // Decide before touching the DOM, or the height we are about to add changes the answer.
    const follow = isNearBottom();
    const own = message.nickname === nickname;

    els.empty.hidden = true;

    const day = dayOf(at);
    if (day !== dayKey) {
        dayKey = day;
        group = null;
        addDaySeparator(at);
    }

    const continues = group !== null
        && group.nickname === message.nickname
        && at - group.at < GROUP_WINDOW_MS;

    const body = continues ? group.body : startGroup(message, at, own);
    group = { nickname: message.nickname, at, body };

    const text = document.createElement("p");
    text.className = "msg__text";
    // textContent, never innerHTML: this string came from another user.
    text.textContent = message.text;
    body.append(text);

    if (follow || own) {
        scrollToEnd();
    } else {
        // Do not yank the view away from someone reading further up.
        els.jump.hidden = false;
    }
}

function resetMessages() {
    els.messages.replaceChildren();
    els.empty.hidden = false;
    els.jump.hidden = true;
    group = null;
    dayKey = "";
}

// ---- State -------------------------------------------------------------------------------------

function setStatus(state, label) {
    els.status.textContent = label;
    els.status.className = `status status--${state}`;

    // Without the stream there is nothing to show a sent message on, and a POST would very
    // likely fail anyway — so the composer closes rather than losing input silently.
    const connected = state === "online";
    els.text.disabled = !connected;
    els.sendButton.disabled = !connected;

    // The first connect needs no banner; the pill already says "verbinde…". A banner here means
    // an established connection dropped, which is exactly what the pod-delete demo produces.
    els.banner.hidden = connected || !hasConnected;
    els.banner.textContent = state === "connecting"
        ? "Verbindung unterbrochen — es wird automatisch neu verbunden."
        : "Keine Verbindung zum Backend.";
}

function showError(message) {
    els.error.textContent = message;
    els.error.hidden = !message;
}

function updateCounter() {
    const used = els.text.value.length;
    els.counter.hidden = used < COUNTER_FROM;
    els.counter.textContent = `${used}/${MAX_TEXT_LENGTH}`;
    els.counter.classList.toggle("counter--warn", used >= COUNTER_WARN);
}

// ---- Network -----------------------------------------------------------------------------------

function connect() {
    source = new EventSource(`${API}/stream?room=${encodeURIComponent(room)}`);

    source.onopen = () => {
        hasConnected = true;
        setStatus("online", "verbunden");
        els.text.focus();
    };

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

// ---- Wiring ------------------------------------------------------------------------------------

els.join.addEventListener("submit", (event) => {
    event.preventDefault();
    nickname = els.nickname.value.trim();
    room = els.room.value.trim();
    if (!nickname || !room) {
        return;
    }

    els.currentRoom.textContent = room;
    els.roomChip.hidden = false;
    els.join.hidden = true;
    els.chat.hidden = false;
    els.composer.hidden = false;
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
    updateCounter();

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
            updateCounter();
        }
    } catch {
        showError("Backend nicht erreichbar.");
        els.text.value = text;
        updateCounter();
    }
});

els.text.addEventListener("input", updateCounter);

els.jump.addEventListener("click", () => scrollToEnd(true));

els.chat.addEventListener("scroll", () => {
    if (isNearBottom()) {
        els.jump.hidden = true;
    }
}, { passive: true });

els.leave.addEventListener("click", () => {
    source?.close();
    source = null;
    hasConnected = false;

    resetMessages();
    els.text.value = "";
    updateCounter();
    showError("");

    els.composer.hidden = true;
    els.chat.hidden = true;
    els.roomChip.hidden = true;
    els.join.hidden = false;
    els.nickname.focus();

    setStatus("offline", "getrennt");
});
