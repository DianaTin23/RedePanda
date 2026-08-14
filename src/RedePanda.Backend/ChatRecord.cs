using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>A chat message together with the Kafka offset it was read from.</summary>
public readonly record struct ChatRecord(long Offset, ChatMessage Message);
