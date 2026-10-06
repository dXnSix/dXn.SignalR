// One AppHost serves the whole run, so collections never run in parallel.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
