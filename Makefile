# Use the same Python as detected by PyO3 for build/runtime consistency
PYTHON = /opt/homebrew/Caskroom/mambaforge/base/bin/python

pyenv:
	$(PYTHON) -m venv crates/kameo-snake-testing/python/venv

install:
	crates/kameo-snake-testing/python/venv/bin/pip install -r crates/kameo-snake-testing/python/requirements.txt

clean:
	rm -rf crates/kameo-snake-testing/python/venv

run:
	export RUSTFLAGS="--cfg tokio_unstable"; \
	PYTHON_BIN=crates/kameo-snake-testing/python/venv/bin/python; \
	PYTHONPATH=`$$PYTHON_BIN -c 'import site; print(site.getsitepackages()[0])'`:crates/kameo-snake-testing/python \
	PATH=crates/kameo-snake-testing/python/venv/bin:$$PATH \
	DYLD_LIBRARY_PATH=/opt/homebrew/Caskroom/mambaforge/base/lib:$$DYLD_LIBRARY_PATH \
	PYTHON_GIL=1 \
	RUST_LOG=trace cargo run -p kameo-snake-testing

run-minimal-test-console:
	DYLD_LIBRARY_PATH=crates/kameo-snake-testing/python/mamba_env/lib RUSTFLAGS="--cfg tokio_unstable" cargo run -p pyo3_async_minimal_test

.PHONY: pyenv install clean run run-minimal-test-console 