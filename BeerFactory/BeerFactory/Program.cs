// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("apikey", "NQju7ZCKxEc7BhWEKwgc7Ll97i4Su7ic6zJx2JyLleo=");

var client = new Hopster.Client("http://hopster.m07039.clients.dev.nrk.no", httpClient);

var bottle = await client.StepAsync();

