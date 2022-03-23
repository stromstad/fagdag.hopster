using Hopster;
using Microsoft.Extensions.Hosting;
using System.Threading.Channels;

namespace BeerFactory;

public sealed class BeerTypeBottling
{
    public string BeerType { get; }

    private readonly Client _client;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly Channel<IBreweryEvent> _channel;
    private readonly FermentationQueue _fermentationQueue;
    private readonly ShippingQueue _shippingQueue;
    private Task? _receiveLoop;
    private Task? _fermentationLoop;

    public BeerTypeBottling(string beerType, Client client, IHostApplicationLifetime lifetime)
    {
        BeerType = beerType;
        _client = client;
        _lifetime = lifetime;
        _channel = Channel.CreateUnbounded<IBreweryEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        _fermentationQueue = new FermentationQueue();
        _shippingQueue = new ShippingQueue();
        _receiveLoop = null;
        _fermentationLoop = null;
    }

    public void Start()
    {
        _receiveLoop = Task.Run(ReceiveLoop);
        _fermentationLoop = Task.Run(FermentationLoop);
    }

    public async Task BottleReceived(BottleReceived @event)
    {
        await _channel.Writer.WriteAsync(@event);
    }

    bool IsOoops(Bottle bottle)
    {
        if (bottle.State == BottleState.Broken)
        {
            Console.WriteLine($"Bottle is broken {bottle.Id}");
            _ = Task.Run(() => _client.RecycleAsync(bottle.Id));
            return true;
        }

        return false;
    }

    async Task ReceiveLoop()
    {
        var stoppingToken = _lifetime.ApplicationStopping;
        while (await _channel.Reader.WaitToReadAsync(stoppingToken))
        {
            await foreach (var @event in _channel.Reader.ReadAllAsync(stoppingToken))
            {
                switch (@event)
                {
                    case BottleReceived e:
                        var bottle = e.Bottle;

                        Console.WriteLine($"Processing received bottle {bottle.Id} {BeerType}");

                        if (IsOoops(bottle)) break;

                        var state = await _client.LevelAsync(BeerType);
                        Console.WriteLine($"{BeerType} has {state} level");
                        if (state < bottle.MaxContent)
                        {
                            Console.WriteLine($"Filling container before filling {bottle.Id} {BeerType}");
                            await _client.FillContainerAsync(BeerType);
                        }

                        var newBottle = await _client.FillbottleAsync(bottle.Id);
                        if (IsOoops(bottle)) break;
                        // check newBottle state

                        Console.WriteLine($"Fermenting bottle {bottle.Id} {BeerType}");
                        await _fermentationQueue.Ferment(newBottle);
                        // state broken -> recycle
                        // state good -> fillbottle -> ferment
                        break;
                }
            }
        }
    }

    async Task FermentationLoop()
    {
        var stoppingToken = _lifetime.ApplicationStopping;
        Bottle? bottle;
        while (!stoppingToken.IsCancellationRequested)
        {
            bottle = await _fermentationQueue.TryGetFermentedBottle();

            if (bottle is null)
            {
                await Task.Delay(1000, stoppingToken);
                continue;
            }

            if (bottle.ConsumeBefore.HasValue && bottle.ConsumeBefore.Value < DateTimeOffset.UtcNow)
            {
                continue;
            }

            Console.WriteLine($"Storing bottle for shipping - queue={_shippingQueue.Count}");
            _shippingQueue.Store(bottle);

            if (_shippingQueue.Count == 24)
            {
                var batch = _shippingQueue.TryGetBottles();
                var shipped = await _client.CaseAsync(new Case()
                {
                    BottleIds = batch.Select(b => b.Id).ToArray(),
                });
                Console.WriteLine($"Shipped case of bottles");
            }

            while (_shippingQueue.ShouldShipFrontBottle)
            {
                var shippableBottle = _shippingQueue.GetBottle();
                await _client.ShipAsync(shippableBottle.Id);
                Console.WriteLine($"Shipped single bottle");
            }
        }
    }
}
