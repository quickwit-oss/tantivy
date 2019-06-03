from setuptools import setup

try:
    from setuptools_rust import Binding, RustExtension
except ImportError:
    print("Please install setuptools-rust package")
    raise SystemExit(1)

setup(
    name="tantivy",
    version="0.9.1",
    rust_extensions=[RustExtension("tantivy.tantivy", binding=Binding.PyO3)],
    packages=["tantivy"],
    zip_safe=False,
)
