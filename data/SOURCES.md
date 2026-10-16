# Data sources and permitted use

The current pipeline uses **only synthetic data** from `producer/gamestream_producer/generator.py`. User and game identifiers, genres, events, purchases, and reviews are invented. The generator is original code in this repository; no third-party dataset has been downloaded or redistributed.

The plan proposes [UCSD McAuley Lab's Steam Video Game and Bundle Data](https://cseweb.ucsd.edu/~jmcauley/datasets.html). The source page describes Steam reviews, purchases, plays, recommendations, game metadata, and bundles, but I did not find an explicit license or reuse terms on that page. Until the terms are confirmed, the implementation does not ingest, bundle, or publish that dataset. A future loader should record the exact file URL, checksum, retrieval date, license, transformations, and privacy review here before use. Synthetic events are sufficient for local correctness and systems experiments, but cannot support a claim about real player behavior.
