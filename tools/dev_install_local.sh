rm -rf heptools.egg-info
rm -rf build
pip uninstall -y heptools
pip install --no-deps --no-cache-dir .
