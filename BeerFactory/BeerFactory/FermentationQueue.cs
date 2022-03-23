using Hopster;

namespace BeerFactory;

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
