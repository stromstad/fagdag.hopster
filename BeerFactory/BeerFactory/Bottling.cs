using Hopster;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace BeerFactory;

public interface IBreweryEvent
{
}

public sealed record BottleReceived(Bottle Bottle) : IBreweryEvent;

public sealed class FermentationQueue
{
    private readonly PriorityQueue<Bottle, DateTimeOffset> _queue;
    private readonly SemaphoreSlim _lock;

    public FermentationQueue()
    {
        _queue = new PriorityQueue<Bottle, DateTimeOffset>();
        _lock = new SemaphoreSlim(1, 1);
    }

    public async Task Ferment(Bottle bottle)
    {
        if (bottle.FermentationSeconds < 0)
            throw new ArgumentException("Invalid fermentation value");

        await _lock.WaitAsync();
        try
        {
            var finishedFermentingAt = DateTimeOffset.UtcNow.AddSeconds(bottle.FermentationSeconds);
            _queue.Enqueue(bottle, finishedFermentingAt);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<Bottle?> TryGetBottle()
    {

        await _lock.WaitAsync();
        try
        {
            if (_queue.Count == 0)
                return null;
            return _queue.Dequeue();
        }
        finally
        {
            _lock.Release();
        }
    }
}

public sealed class BeerTypeBottling
{
    public string BeerType { get; }

    private readonly Client _client;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly Channel<IBreweryEvent> _channel;
    private readonly FermentationQueue _fermentationQueue;
    private Task? _receiveLoop;

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
        _receiveLoop = null;
    }

    public void Start()
    {
        _receiveLoop = Task.Run(Loop);
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

    async Task Loop()
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
}

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
