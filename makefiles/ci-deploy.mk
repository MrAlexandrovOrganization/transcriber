# Standalone bootstrap for the first image-based rollout on an existing VM.
export IMAGE_DEPLOY_LOCK := $(HOME)/.transcriber-images-deploy.lock

.PHONY: ci-deploy
ci-deploy:
	@set -eu; \
	test "$$(git rev-parse --show-toplevel)" = "$$(pwd -P)"; \
	test -n "$${IMAGE_MANIFEST_B64:-}"; \
	test -n "$${GHCR_USER:-}"; \
	test -n "$${GHCR_TOKEN:-}"; \
	test "$${#DEPLOY_SHA}" -eq 40; \
	case "$$DEPLOY_SHA" in *[!0-9a-f]*) exit 1 ;; esac; \
	exec 9>"$(IMAGE_DEPLOY_LOCK)"; \
	flock -w 1800 9; \
	test "$$(git branch --show-current)" = main; \
	test -z "$$(git status --porcelain --untracked-files=all)"; \
	git fetch --quiet origin refs/heads/main; \
	test "$$(git rev-parse FETCH_HEAD)" = "$$DEPLOY_SHA" || { echo 'Stale workflow: main changed' >&2; exit 1; }; \
	git merge --ff-only --quiet "$$DEPLOY_SHA"; \
	test "$$(git rev-parse HEAD)" = "$$DEPLOY_SHA"; \
	DOCKER_CONFIG=$$(mktemp -d); export DOCKER_CONFIG; \
	trap 'rm -rf "$$DOCKER_CONFIG"' EXIT; \
	printf '%s' "$$GHCR_TOKEN" | docker login ghcr.io -u "$$GHCR_USER" --password-stdin; \
	unset GHCR_TOKEN; \
	IMAGE_DEPLOY_LOCKED=1 make -f Makefile image-deploy
