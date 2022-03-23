﻿using Hopster;

namespace BeerFactory;

public sealed class ShippingQueue
{
    private readonly PriorityQueue<Bottle, DateTimeOffset> _queue;

    public ShippingQueue()
    {
        _queue = new PriorityQueue<Bottle, DateTimeOffset>();
    }

    public int Count => _queue.Count;

    public bool ShouldShipFrontBottle => _queue.TryPeek(out _, out var priority) &&
        priority < DateTimeOffset.UtcNow.AddSeconds(20);

    public void Store(Bottle bottle)
    {
        var priority = bottle.ConsumeBefore.HasValue ?
            bottle.ConsumeBefore.Value :
            DateTimeOffset.UtcNow.AddDays(30);
        _queue.Enqueue(bottle, priority);
    }

    public Bottle GetBottle() => _queue.Dequeue();

    public IEnumerable<Bottle> TryGetBottles()
    {
        if (_queue.Count == 0)
            return Array.Empty<Bottle>();

        var bottles = new List<Bottle>();
        while (bottles.Count < 24 && _queue.TryDequeue(out var bottle, out var prio))
        {
            bottles.Add(bottle);
        }

        return bottles;
    }
}

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

    public async Task<Bottle?> TryGetFermentedBottle()
    {
        await _lock.WaitAsync();
        try
        {
            if (_queue.Count == 0)
                return null;
            var gotBottle = _queue.TryPeek(out var bottle, out var doneFermentingAt);
            if (!gotBottle)
                return null;

            if (doneFermentingAt > DateTimeOffset.UtcNow)
                return null;

            _queue.Dequeue();
            return bottle!;
        }
        finally
        {
            _lock.Release();
        }
    }
}
