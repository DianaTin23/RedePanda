using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace RedeTim.Backend.Tests;

// The whole app, minus everything that would reach for a broker: both consumer services are
// unregistered and the presence producer is swapped for a fake. ChatProducer and BrokerReadiness
// stay real -- librdkafka connects lazily, and no test here touches a path that would make them.
//
// The consumers are matched by ImplementationType, so both concrete classes have to stay
// separately registered. See Program.cs.
public sealed class BrokerlessBackend : WebApplicationFactory<Program>
{
    internal FakePresenceProducer Producer { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            foreach (var type in new[] { typeof(ChatConsumerService), typeof(PresenceConsumerService) })
            {
                var descriptor = services.SingleOrDefault(d => d.ImplementationType == type);
                if (descriptor is not null)
                {
                    services.Remove(descriptor);
                }
            }

            services.RemoveAll<IPresenceProducer>();
            services.AddSingleton<IPresenceProducer>(Producer);
        });
    }
}
