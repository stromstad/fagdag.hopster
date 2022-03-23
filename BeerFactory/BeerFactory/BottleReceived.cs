using Hopster;

namespace BeerFactory;

public sealed record BottleReceived(Bottle Bottle) : IBreweryEvent;
