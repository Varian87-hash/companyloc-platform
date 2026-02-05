DEFAULT_COMPANIES = ["amazon", "apple", "google", "intel", "meta", "microsoft", "nvidia", "nokia"]

PIPELINE_MODULES = {
    "amazon": "backend.py.pipeline.ingest_amazon",
    "apple": "backend.py.pipeline.ingest_apple",
    "google": "backend.py.pipeline.ingest_google",
    "intel": "backend.py.pipeline.ingest_intel",
    "meta": "backend.py.pipeline.ingest_meta",
    "microsoft": "backend.py.pipeline.ingest_microsoft",
    "nvidia": "backend.py.pipeline.ingest_nvidia",
    "nokia": "backend.py.pipeline.ingest_nokia",
}
