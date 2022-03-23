using Hopster;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;

namespace BeerFactory;

public enum BottlingMode
{
    SplitQueues,
    GlobalQueue,
}

public class Bottling
{
    public static readonly BottlingMode Mode = BottlingMode.SplitQueues;

    private readonly Client _client;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ConcurrentDictionary<string, BeerTypeBottling> _beerTypes;
    private readonly Task _shippingQueueLoop;
    private readonly Task _conveyorBeltMonitorLoop;

    public Bottling(Client client, IHostApplicationLifetime lifetime)
    {
        _client = client;
        _lifetime = lifetime;
        _beerTypes = new ConcurrentDictionary<string, BeerTypeBottling>();
        _shippingQueueLoop = Task.Run(ShippingQueueLoop);
        _conveyorBeltMonitorLoop = Task.Run(ConveryBeltLoop);
    }

    public async Task BottleReceived(BottleReceived @event)
    {
        var bottle = @event.Bottle;

        Console.WriteLine($"Received bottle {bottle.Id} {bottle.BeerType}");

        var beerType = _beerTypes.GetOrAdd(Mode == BottlingMode.SplitQueues ? bottle.BeerType : "Da best beer", k =>
        {
            Console.WriteLine($"Received bottle {bottle.Id} {bottle.BeerType}, starting loop");

            var result = new BeerTypeBottling(k, _client, _lifetime);
            result.Start();
            return result;
        });
        await beerType.BottleReceived(@event);
    }

    async Task ConveryBeltLoop()
    {
        var stoppingToken = _lifetime.ApplicationStopping;

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000);

            var state = await _client.StateAsync();
            if (state == ConveyorBeltState.Crashed || state == ConveyorBeltState.Stopped)
            {
                Console.WriteLine("Conveyor belt crashed, fixing");
                await _client.FixAsync();
            }
        }
    }

    async Task ShippingQueueLoop()
    {
        var stoppingToken = _lifetime.ApplicationStopping;

        while (!stoppingToken.IsCancellationRequested)
        {
            var queues = new List<ShippingQueue>();
            foreach (var kvp in _beerTypes)
                queues.Add(kvp.Value.ShippingQueue);

            var bottles = new List<Bottle>(24);
            foreach (var queue in queues)
            {
                while (queue.TryGetShortExpiryBottle(out var shippableBottle))
                {
                    Console.WriteLine($"Picking short expiry bottle for {shippableBottle.BeerType}");
                    bottles.Add(shippableBottle);
                }
            }

            foreach (var batch in bottles.Where(b => !b.ConsumeBefore.HasValue || b.ConsumeBefore.Value > DateTimeOffset.UtcNow).Chunk(24))
            {
                if (batch.Length == 24)
                {
                    Console.WriteLine($"Shipping case of short expiry bottles");
                    await _client.CaseAsync(new Case
                    {
                        BottleIds = batch.Select(b => b.Id).ToArray(),
                    });
                }
                else
                {
                    Console.WriteLine($"Shipping smaller batch of short expiry bottles, count={batch.Length}");
                    await Task.WhenAll(batch.Select(b => _client.ShipAsync(b.Id)));
                }
            }


            await Task.Delay(1000);
        }

        {
            var queues = new List<ShippingQueue>();
            foreach (var kvp in _beerTypes)
                queues.Add(kvp.Value.ShippingQueue);

            var bottles = new List<Bottle>(24);
            foreach (var queue in queues)
            {
                while (queue.TryGetShortExpiryBottle(out var shippableBottle))
                {
                    Console.WriteLine($"Picking short expiry bottle for {shippableBottle.BeerType}");
                    bottles.Add(shippableBottle);
                }
            }

            foreach (var batch in bottles.Where(b => !b.ConsumeBefore.HasValue || b.ConsumeBefore.Value > DateTimeOffset.UtcNow).Chunk(24))
            {
                if (batch.Length == 24)
                {
                    Console.WriteLine($"EXIT Shipping case of short expiry bottles");
                    await _client.CaseAsync(new Case
                    {
                        BottleIds = batch.Select(b => b.Id).ToArray(),
                    });
                }
                else
                {
                    Console.WriteLine($"EXIT Shipping smaller batch of short expiry bottles, count={batch.Length}");
                    await Task.WhenAll(batch.Select(b => _client.ShipAsync(b.Id)));
                }
            }
        }
    }
}
