# Use python3 from PATH for venv and pip
PYTHON ?= python3

pyenv:
	$(PYTHON) -m venv crates/kameo-snake-testing/python/venv

install:
	crates/kameo-snake-testing/python/venv/bin/pip install -r crates/kameo-snake-testing/python/requirements.txt

clean:
	rm -rf crates/kameo-snake-testing/python/venv

run:
	PYTHON_BIN=crates/kameo-snake-testing/python/venv/bin/python; \
	if [ "$(shell uname)" = "Darwin" ]; then \
	  LIBPY_PATH=`$$PYTHON_BIN -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))'`; \
	  LIBPY_DYLIB="$$LIBPY_PATH/libpython3.13.dylib"; \
	  if [ -f "$$LIBPY_DYLIB" ]; then \
	    export DYLD_LIBRARY_PATH="$$LIBPY_PATH:$$DYLD_LIBRARY_PATH"; \
	    echo "[INFO] Using DYLD_LIBRARY_PATH=$$DYLD_LIBRARY_PATH"; \
	  fi; \
	else \
	  export LD_LIBRARY_PATH=$$LD_LIBRARY_PATH; \
	fi; \
	PYTHONPATH=`$$PYTHON_BIN -c 'import site; print(site.getsitepackages()[0])'`:crates/kameo-snake-testing/python \
	PATH=crates/kameo-snake-testing/python/venv/bin:$$PATH \
	PYTHON_GIL=1 \
	cargo run --release -p kameo-snake-testing -- $(ARGS)

pyo3-async-test-venv:
	$(PYTHON) -m venv kameo-pyo3-async-test/python/venv

pyo3-async-test-install: pyo3-async-test-venv
	$(PYTHON) -m pip install --upgrade pip --break-system-packages
	# Add requirements here if needed, e.g.:
	# kameo-pyo3-async-test/python/venv/bin/pip install -r kameo-pyo3-async-test/python/requirements.txt

pyo3-async-test:
	PYTHON_BIN=kameo-pyo3-async-test/python/venv/bin/python; \
	if [ "$(shell uname)" = "Darwin" ]; then \
	  LIBPY_PATH=`$$PYTHON_BIN -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))'`; \
	  LIBPY_DYLIB="$$LIBPY_PATH/libpython3.13.dylib"; \
	  if [ -f "$$LIBPY_DYLIB" ]; then \
	    export DYLD_LIBRARY_PATH="$$LIBPY_PATH:$$DYLD_LIBRARY_PATH"; \
	    echo "[INFO] Using DYLD_LIBRARY_PATH=$$DYLD_LIBRARY_PATH"; \
	  fi; \
	else \
	  export LD_LIBRARY_PATH=$$LD_LIBRARY_PATH; \
	fi; \
	PYTHONPATH=`$$PYTHON_BIN -c 'import site; print(site.getsitepackages()[0])'`:kameo-pyo3-async-test/python \
	PATH=kameo-pyo3-async-test/python/venv/bin:$$PATH \
	PYTHON_GIL=1 \
	cargo run --release -p kameo-pyo3-async-test -- $(ARGS)

.PHONY: pyenv install clean run pyo3-async-test 