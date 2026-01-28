CONFIG = {
    "sample": {
        "library_id": "Sample1",
        "tissue": "Pancreas",
        "disease": "Pancreatic Cancer",
        "prep": "fresh_frozen",
        "species": "Human",
        "bin_size_um": 8,
    },

    "paths": {
        "data_path": "/path/to/data/human_pancreatic_cancer_FF/binned_outputs/square_008um",
        "output": "perturbations_expanded.parquet",
    },

    "qc": {
        "gene_quantiles": [0.05, 0.10],
        "umi_quantiles": [0.05, 0.10],
    },

    "normalization": {
        "methods": ["log1p"],
    },

    "graph": {
        "n_neighbors": [10, 15, 20],
        "leiden_res": [0.3, 0.5, 0.7, 0.9],
    },

    "spatial": {
        "smoothing_steps": [0, 1, 2],   #NOTE: do not go above 3 for now
    },
}
