# Attribution

## Data Sources

### Google Patents Public Data

Patent metadata, classification codes, citation data, and applicant information are sourced from [Google Patents Public Datasets](https://console.cloud.google.com/marketplace/product/google_patents_public_datasets/google-patents-public-data) on BigQuery.

> Google Patents Public Datasets are made available under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
>
> Patent data originates from patent offices worldwide. Google provides this data in a structured, queryable format.

**Tables used:**
- `patents-public-data.patents.publications` — Patent metadata, claims, abstracts
- `patents-public-data.google_patents_research.publications` — 64-dimensional patent embeddings, top terms

### Google Patents Research

Patent embeddings (embedding_v1, 64-dimensional vectors) are from the Google Patents Research dataset:

> Ian Wetherbee, et al. "Google Patents Research Data." Google, 2024.

These embeddings are used for technology clustering, similarity search, and the embedding bridge module.

### GDELT Project

Company news events and sentiment features are derived from the [GDELT Project](https://www.gdeltproject.org/):

> The GDELT Project is supported by Google Jigsaw.
> GDELT data is available under a permissive license for research and commercial use.

**Tables used:**
- `gdelt-bq.gdeltv2.gkg_partitioned` — Global Knowledge Graph
- `gdelt-bq.gdeltv2.events` — GDELT Event records

### Tokyo Stock Exchange

Entity seed data for Japanese firms is based on publicly available listings from the Tokyo Stock Exchange (TSE Prime, Standard, and Growth markets).

## Software Dependencies

This project uses the following open-source libraries:

| Library | License |
|---------|---------|
| [FastMCP](https://github.com/jlowin/fastmcp) | Apache 2.0 |
| [NumPy](https://numpy.org/) | BSD 3-Clause |
| [scikit-learn](https://scikit-learn.org/) | BSD 3-Clause |
| [thefuzz](https://github.com/seatgeek/thefuzz) | MIT |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | BSD 3-Clause |

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

When using this project or its data outputs, please include attribution to the data sources listed above, particularly the CC BY 4.0 attribution for Google Patents Public Data.
