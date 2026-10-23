"""
MI (formerly "Prototype"): Smartcare-based subscriber analysis for CallFlow.

Not a Django app (no models, no migrations, not in INSTALLED_APPS) - it is a
package of DRF views plus a worker, routed from callflow/urls.py:
  /callflow/mi/api/...         new path (async by default)
  /callflow/prototype/api/...  old path, kept for existing callers
"""
