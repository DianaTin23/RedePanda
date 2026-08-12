using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// A chat message together with the Kafka offset it was read from.
/// <para>
/// The offset stays inside this assembly and never enters the payload: it is written to the SSE
/// <c>id</c> field only. <see cref="RedePanda.Contracts.ChatMessage"/> is the format the console
/// client also parses, and widening it here would have broken that for a value the browser needs
/// but no consumer of the topic does.
/// </para>
/// <para>
/// A room is the Kafka record key (see <see cref="ChatProducer.ProduceAsync"/>), so every message
/// of one room lands on the same partition and the offsets seen by one SSE stream are strictly
/// increasing. That is exactly the property <c>Last-Event-ID</c> needs to resume a dropped
/// connection without repeating or skipping anything.
/// </para>
/// </summary>
public readonly record struct ChatRecord(long Offset, ChatMessage Message);
