"""Beacon: a FOSS search engine for the Reticulum mesh.

Stage 1 = crawler + registry: discover NomadNet nodes from announces, crawl their
micron pages over Reticulum, and build a Postgres registry + page store (with FTS
ready). Search API/UI, ranking, and analytics build on top.
"""
__version__ = "0.1.0"
