using RedeTim.Contracts;

namespace RedeTim.Backend;

public readonly record struct ChatRecord(long Offset, ChatMessage Message);
