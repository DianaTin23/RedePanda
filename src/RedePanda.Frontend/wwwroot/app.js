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

// Reconnect budget for the case EventSource gives up on (see connect()). Delays grow 1, 2, 4, 8 and
// then stay at the cap, so eight attempts span roughly 75 seconds — long enough for a rescheduled
// backend pod to become ready, short enough that a genuinely dead backend does not retry forever.
const MAX_RETRIES = 8;
const RETRY_BASE_MS = 1000;
const RETRY_CAP_MS = 15000;

// Deliberately above the backend's own PRODUCE_TIMEOUT_MS (10 s), so the normal case is the server
// answering 504 and saying why. This is the backstop for the case the server cannot answer at all —
// a frontend pod that has lost the backend — where nothing else would ever settle the promise.
const SEND_TIMEOUT_MS = 15000;

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
    bannerText: document.getElementById("banner-text"),
    reconnect: document.getElementById("reconnect"),
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

// The highest Kafka offset already rendered. Every data frame carries its offset as the SSE id, and
// within a room those only ever grow: the room is the Kafka record key, so all of a room's messages
// land on the same partition (see ChatRecord in the backend).
//
// When EventSource reconnects by itself it resends that id as Last-Event-ID and the server replays
// only what came after it. The other reconnect path cannot: connect() builds a brand-new
// EventSource, no JS API can put a header on one, and the server therefore sees a first-time client
// and replays the whole room. Filtering here is what stops that arriving as a second copy of the
// conversation — and it holds for both paths, whichever pod the reconnect lands on.
let lastOffset = -1;

// Reconnect state. A pending retry is `retryTimer !== null`, an exhausted budget is `gaveUp`, and
// a live or connecting stream is `source !== null` — those three are mutually exclusive.
let retryTimer = null;
let retryAttempt = 0;
let gaveUp = false;

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

    // Offsets belong to the topic, not to a room: the next room's messages can carry lower ones
    // than the last rendered here, and without this reset they would be filtered away as seen.
    lastOffset = -1;
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
    // either an established connection dropped — exactly what the pod-delete demo produces — or
    // that we ran out of retries, in which case it carries the only way back and must show even
    // when the very first connect never succeeded.
    els.banner.hidden = connected || !(hasConnected || gaveUp);
    els.reconnect.hidden = !gaveUp;
    els.bannerText.textContent = gaveUp
        ? "Keine Verbindung zum Backend."
        : "Verbindung unterbrochen — es wird automatisch neu verbunden.";
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
    // Held locally as well so every handler can tell whether it belongs to the current stream: a
    // handler of one we already closed must not write the status of the one that replaced it.
    const stream = new EventSource(`${API}/stream?room=${encodeURIComponent(room)}`);
    source = stream;

    stream.onopen = () => {
        if (source !== stream) {
            return;
        }

        // A stream that actually opened earns a fresh budget.
        retryAttempt = 0;
        gaveUp = false;
        hasConnected = true;
        setStatus("online", "verbunden");
        els.text.focus();
    };

    stream.onmessage = (event) => {
        // Advanced before rendering and regardless of whether rendering works out: the frame has
        // been consumed either way, and one we could not parse is not one to try again.
        //
        // A frame without an id yields NaN and counts as new — the safe direction, and heartbeats
        // never get here anyway because they carry the `ping` event type.
        const offset = Number.parseInt(event.lastEventId, 10);
        if (Number.isInteger(offset)) {
            if (offset <= lastOffset) {
                return;
            }

            lastOffset = offset;
        }

        try {
            addMessage(JSON.parse(event.data));
        } catch {
            // A malformed frame must not kill the stream.
        }
    };

    stream.onerror = () => {
        if (source !== stream) {
            return;
        }

        // Two different failures arrive through this one handler:
        //
        // An established stream that just ends leaves readyState at CONNECTING, and EventSource
        // retries by itself — there is nothing to do but say so.
        if (stream.readyState !== EventSource.CLOSED) {
            setStatus("connecting", "verbinde neu…");
            return;
        }

        // CLOSED means the retry reached a server and got an answer that was not an SSE stream —
        // with the backend pod gone, Caddy's reverse_proxy answers 502. The spec calls that fatal:
        // EventSource will never try again, so from here the reconnect is ours to drive.
        scheduleReconnect();
    };
}

/** Closes the dead stream and books the next attempt, or gives up once the budget runs out. */
function scheduleReconnect() {
    source?.close();
    source = null;

    if (retryAttempt >= MAX_RETRIES) {
        gaveUp = true;
        setStatus("offline", "getrennt");
        return;
    }

    // Jitter keeps a room full of tabs from hitting the new pod in one synchronised burst.
    const delay = Math.min(RETRY_BASE_MS * 2 ** retryAttempt, RETRY_CAP_MS);
    retryAttempt += 1;

    setStatus("connecting", "verbinde neu…");
    retryTimer = setTimeout(() => retryNow(), delay * (0.75 + Math.random() * 0.5));
}

/**
 * The single way back onto the stream — used by the backoff timer, the button, and the events that
 * mean a wait has become pointless. `fresh` marks a signal that justifies a new budget: the user
 * asking, or the network coming back. Without it an exhausted budget stays exhausted.
 */
function retryNow({ fresh = false } = {}) {
    if (!room || source !== null) {
        // Not in a room, or a stream is already live or connecting.
        return;
    }

    if (!fresh && retryTimer === null) {
        return;
    }

    // No-op for a timer that has already fired, which is how we get here in the common case.
    clearTimeout(retryTimer);
    retryTimer = null;

    if (fresh) {
        retryAttempt = 0;
    }

    gaveUp = false;
    setStatus("connecting", "verbinde neu…");
    connect();
}

// ---- Wiring ------------------------------------------------------------------------------------

els.join.addEventListener("submit", (event) => {
    event.preventDefault();
    nickname = els.nickname.value.trim();
    room = els.room.value.trim();
    if (!nickname || !room) {
        return;
    }

    retryAttempt = 0;
    gaveUp = false;

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
            signal: AbortSignal.timeout(SEND_TIMEOUT_MS),
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

els.reconnect.addEventListener("click", () => retryNow({ fresh: true }));

// Both of these mean waiting out the rest of the backoff has stopped making sense. Coming back
// online is new information about the network, so it also revives a budget we had already spent;
// a tab merely being looked at again is not, and only shortens a wait that is already pending.
window.addEventListener("online", () => retryNow({ fresh: true }));

document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
        retryNow();
    }
});

els.chat.addEventListener("scroll", () => {
    if (isNearBottom()) {
        els.jump.hidden = true;
    }
}, { passive: true });

els.leave.addEventListener("click", () => {
    source?.close();
    source = null;
    hasConnected = false;

    // A retry booked before leaving must not reopen the stream behind the join screen. Clearing
    // the room is what makes retryNow() refuse for good, whatever else may still call it.
    clearTimeout(retryTimer);
    retryTimer = null;
    retryAttempt = 0;
    gaveUp = false;
    room = "";

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
