#!/usr/bin/env python
"""
jsonb_parse setup script.

Copyright (c) 2021 Daniele Varrazzo <daniele.varrazzo@gmail.com>
"""

import os

from setuptools import setup, Extension
from distutils.command.build_ext import build_ext

VERSION = "0.1.dev0"


class custom_build_ext(build_ext):
    def finalize_options(self) -> None:
        self._setup_ext_build()
        super().finalize_options()

    def _setup_ext_build(self) -> None:
        cythonize = None

        # In the sdist there are not .pyx, only c, so we don't need Cython
        # Otherwise Cython is a requirement and is be used to compile pyx to c
        if os.path.exists("jsonb_parser/_parser.pyx"):
            from Cython.Build import cythonize

        if cythonize is not None:
            for ext in self.distribution.ext_modules:
                for i in range(len(ext.sources)):
                    base, fext = os.path.splitext(ext.sources[i])
                    if fext == ".c" and os.path.exists(base + ".pyx"):
                        ext.sources[i] = base + ".pyx"

            self.distribution.ext_modules = cythonize(
                self.distribution.ext_modules,
                language_level=3,
                compiler_directives={
                    "always_allow_keywords": False,
                },
                annotate=False,  # enable to get an html view of the C module
            )
        else:
            self.distribution.ext_modules = [bext]


bext = Extension(
    "jsonb_parser._parser", ["jsonb_parser/_parser.c"], include_dirs=[]
)

setup(
    version=VERSION,
    ext_modules=[bext],
    cmdclass={"build_ext": custom_build_ext},
    install_requires=[
        (
            "psycopg3 @ git+https://github.com/psycopg/psycopg3.git@68547b8"
            "#subdirectory=psycopg3"
        ),
        (
            "psycopg3-c @ git+https://github.com/psycopg/psycopg3.git@68547b8"
            "#subdirectory=psycopg3_c"
        ),
    ],
)
