PYTHON = /usr/local/bin/python3.13

pyenv:
	$(PYTHON) -m venv crates/kameo-snake-testing/python/venv

install:
	crates/kameo-snake-testing/python/venv/bin/pip install -r crates/kameo-snake-testing/python/requirements.txt

clean:
	rm -rf crates/kameo-snake-testing/python/venv

run:
	PYTHON_BIN=crates/kameo-snake-testing/python/venv/bin/python3.13; \
	PYTHONPATH=`$$PYTHON_BIN -c 'import site; print(site.getsitepackages()[0])'`:crates/kameo-snake-testing/python \
	PATH=crates/kameo-snake-testing/python/venv/bin:$$PATH \
	DYLD_LIBRARY_PATH=/Library/Frameworks/Python.framework/Versions/3.13/lib/:$$DYLD_LIBRARY_PATH \
	PYTHON_GIL=1 \
	cargo run -p kameo-snake-testing

.PHONY: pyenv install clean run 