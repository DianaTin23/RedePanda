using System.Text.Json.Serialization;

namespace RedePanda_chat_client;

public class Bootstrap
{
    [JsonPropertyName("advertisedHost")]
    public string? AdvertisedHost { get; set; }
    
    [JsonPropertyName("port")]
    public string? Port { get; set; }
}