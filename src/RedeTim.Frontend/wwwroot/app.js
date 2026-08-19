"use strict";

const API = "/api";

// KEEP IN SYNC: ChatMessage.DefaultMaxTextLength in RedeTim.Contracts.
const MAX_TEXT_LENGTH = 500;
const COUNTER_FROM = 400;
const COUNTER_WARN = 480;

const GROUP_WINDOW_MS = 5 * 60 * 1000;

const NEAR_BOTTOM_PX = 80;

const THEME_KEY = "redetim.theme";

const MAX_RETRIES = 8;
const RETRY_BASE_MS = 1000;
const RETRY_CAP_MS = 15000;

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
    themeButtons: document.querySelectorAll("[data-theme-option]"),
};

let source = null;
let nickname = "";
let room = "";

let group = null;
let dayKey = "";

let hasConnected = false;

let lastOffset = -1;

let retryTimer = null;
let retryAttempt = 0;
let gaveUp = false;

const timeFormat = new Intl.DateTimeFormat("de-DE", { hour: "2-digit", minute: "2-digit" });
const dateFormat = new Intl.DateTimeFormat("de-DE", { weekday: "long", day: "numeric", month: "long" });
const instantFormat = new Intl.DateTimeFormat("de-DE", { dateStyle: "long", timeStyle: "medium" });

const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

function readStoredTheme() {
    try {
        return localStorage.getItem(THEME_KEY);
    } catch {
        return null;
    }
}

function applyTheme(theme) {
    if (theme === "dark" || theme === "light" || theme === "pretty") {
        document.documentElement.dataset.theme = theme;
    } else {
        delete document.documentElement.dataset.theme;
    }

    const current = resolvedTheme();
    for (const button of els.themeButtons) {
        button.setAttribute("aria-pressed", String(button.dataset.themeOption === current));
    }
}

function resolvedTheme() {
    return document.documentElement.dataset.theme
        ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}

applyTheme(readStoredTheme());

for (const button of els.themeButtons) {
    button.addEventListener("click", () => {
        const theme = button.dataset.themeOption;
        applyTheme(theme);
        try {
            localStorage.setItem(THEME_KEY, theme);
        } catch {
        }
    });
}

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

function scrollToEnd(smooth = false) {
    els.chat.scrollTo({
        top: els.chat.scrollHeight,
        behavior: smooth && !reducedMotion.matches ? "smooth" : "auto",
    });
    els.jump.hidden = true;
}

function addDaySeparator(at) {
    const item = document.createElement("li");
    item.className = "daysep";

    const label = document.createElement("span");
    label.textContent = dayLabel(at);

    item.append(label);
    els.messages.append(item);
}

function startGroup(message, at, own) {
    const item = document.createElement("li");
    item.className = own ? "msg msg--own" : "msg";
    item.style.setProperty("--nick-h", String(hueFor(message.nickname)));

    const avatar = document.createElement("span");
    avatar.className = "msg__avatar";
    avatar.setAttribute("aria-hidden", "true");
    avatar.textContent = (Array.from(message.nickname)[0] ?? "?").toUpperCase();

    const name = document.createElement("span");
    name.className = "msg__name";
    name.textContent = own ? `${message.nickname} (du)` : message.nickname;

    const time = document.createElement("time");
    time.className = "msg__time";
    time.dateTime = at.toISOString();
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
    const at = new Date(message.timestamp);
    if (Number.isNaN(at.getTime())) {
        return;
    }

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
    text.textContent = message.text;
    body.append(text);

    if (follow || own) {
        scrollToEnd();
    } else {
        els.jump.hidden = false;
    }
}

function resetMessages() {
    els.messages.replaceChildren();
    els.empty.hidden = false;
    els.jump.hidden = true;
    group = null;
    dayKey = "";

    lastOffset = -1;
}

function setStatus(state, label) {
    els.status.textContent = label;
    els.status.className = `status status--${state}`;

    const connected = state === "online";
    els.text.disabled = !connected;
    els.sendButton.disabled = !connected;

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

function connect() {
    const stream = new EventSource(`${API}/stream?room=${encodeURIComponent(room)}`);
    source = stream;

    stream.onopen = () => {
        if (source !== stream) {
            return;
        }

        retryAttempt = 0;
        gaveUp = false;
        hasConnected = true;
        setStatus("online", "verbunden");
        els.text.focus();
    };

    stream.onmessage = (event) => {
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
        }
    };

    stream.onerror = () => {
        if (source !== stream) {
            return;
        }

        if (stream.readyState !== EventSource.CLOSED) {
            setStatus("connecting", "verbinde neu…");
            return;
        }

        scheduleReconnect();
    };
}

function scheduleReconnect() {
    source?.close();
    source = null;

    if (retryAttempt >= MAX_RETRIES) {
        gaveUp = true;
        setStatus("offline", "getrennt");
        return;
    }

    const delay = Math.min(RETRY_BASE_MS * 2 ** retryAttempt, RETRY_CAP_MS);
    retryAttempt += 1;

    setStatus("connecting", "verbinde neu…");
    retryTimer = setTimeout(() => retryNow(), delay * (0.75 + Math.random() * 0.5));
}

function retryNow({ fresh = false } = {}) {
    if (!room || source !== null) {
        return;
    }

    if (!fresh && retryTimer === null) {
        return;
    }

    clearTimeout(retryTimer);
    retryTimer = null;

    if (fresh) {
        retryAttempt = 0;
    }

    gaveUp = false;
    setStatus("connecting", "verbinde neu…");
    connect();
}

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
