.PHONY: setup lint test clean format format-check

# Setup development environment
setup:
	uv sync

# Format code with ruff
format:
	uv run ruff format src tests

# Check code formatting with ruff
format-check:
	uv run ruff format --check src tests

# Lint with ruff and mypy
lint:
	uv run ruff check src tests
	uv run mypy src tests

# Run tests
test:
	uv run pytest tests/

# Publish
publish:
	@if [ -z "$$UV_PUBLISH_TOKEN" ]; then \
		echo "Error: UV_PUBLISH_TOKEN environment variable is not set"; \
		exit 1; \
	fi
	uv build
	uv publish
	rm -r dist

# Clean up build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/ 
