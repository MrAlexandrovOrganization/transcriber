# Offline contract check/export; schema tool is vendored and checksum-pinned.
.PHONY: observability-check observability-export
observability-check:
	python3 tools/observability/observability_package.py check --root "$(CURDIR)"
observability-export: observability-check
	python3 tools/observability/observability_package.py export --root "$(CURDIR)" --output "$(CURDIR)/.observability/package.tar"
check: observability-check
