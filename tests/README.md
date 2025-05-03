Testing
=======

To test python-OBD, you will need to install `pytest` and the `obd` module from your local tree (preferably in a virtualenv) by running:

```bash
pip install pytest
pip install build
python -m build
pip install ./dist/obd-0.7.2.tar.gz
```

To run all basic python-only unit tests, run:

```bash
py.test
```

This directory also contains a set of end-to-end tests that require [obdsim](http://icculus.org/obdgpslogger/obdsim.html) to be running in the background. These tests are skipped by default, but can be activated by passing the `--port` flag.

- Download `obdgpslogger`: https://icculus.org/obdgpslogger/downloads/obdgpslogger-0.16.tar.gz
- Run the following build commands:
  ```bash
  mkdir build
  cd build
  cmake ..
  make obdsim
  ```
- Start `./bin/obdsim`, note the `SimPort name: /dev/pts/<num>` that it creates. Pass this pseudoterminal path as an argument to py.test:
  ```bash
  py.test --port=/dev/pts/<num>
  ```

For more information on pytest with virtualenv, [read more here](https://pytest.org/dev/goodpractises.html)