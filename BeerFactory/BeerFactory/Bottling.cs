using Hopster;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;

namespace BeerFactory;

public class Bottling
{
    private readonly Client _client;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ConcurrentDictionary<string, BeerTypeBottling> _beerTypes;

    public Bottling(Client client, IHostApplicationLifetime lifetime)
    {
        _client = client;
        _lifetime = lifetime;
        _beerTypes = new ConcurrentDictionary<string, BeerTypeBottling>();
    }

    public async Task BottleReceived(BottleReceived @event)
    {
        var bottle = @event.Bottle;

        Console.WriteLine($"Received bottle {bottle.Id} {bottle.BeerType}");

        var beerType = _beerTypes.GetOrAdd(bottle.BeerType, k =>
        {
            Console.WriteLine($"Received bottle {bottle.Id} {bottle.BeerType}, starting loop");

            var result = new BeerTypeBottling(k, _client, _lifetime);
            result.Start();
            return result;
        });
        await beerType.BottleReceived(@event);
    }
}
