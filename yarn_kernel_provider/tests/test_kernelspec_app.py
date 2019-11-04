"""Tests kernel spec installation for use by YarnKernelProvider"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import json
import os
import pytest
import shutil
from tempfile import mkdtemp


@pytest.fixture()
def mock_kernels_dir():
    kernels_dir = mkdtemp(prefix="kernels_")
    orig_data_dir = os.environ.get("JUPYTER_DATA_DIR")
    os.environ["JUPYTER_DATA_DIR"] = kernels_dir
    yield kernels_dir  # provide the fixture value
    shutil.rmtree(kernels_dir)
    if orig_data_dir:
        os.environ["JUPYTER_DATA_DIR"] = orig_data_dir
    else:
        os.environ.pop("JUPYTER_DATA_DIR")


def test_no_opts(script_runner):
    ret = script_runner.run('jupyter-yarn-kernelspec')
    assert ret.success is False
    assert ret.stdout.startswith("No subcommand specified.")
    assert ret.stderr == ''


def test_bad_subcommand(script_runner):
    ret = script_runner.run('jupyter', 'yarn-kernelspec', 'bogus-subcommand')
    assert ret.success is False
    assert ret.stdout.startswith("No subcommand specified.")
    assert ret.stderr == ''


def test_help_all(script_runner):
    ret = script_runner.run('jupyter-yarn-kernelspec', 'install', '--help-all')
    assert ret.success
    assert ret.stdout.startswith("A Jupyter kernel for talking to Spark/Dask within a YARN cluster")
    assert ret.stderr == ''


def test_bad_argument(script_runner):
    ret = script_runner.run('jupyter-yarn-kernelspec', 'install', '--bogus-argument')
    assert ret.success is False
    assert ret.stdout.startswith("A Jupyter kernel for talking to Spark/Dask within a YARN cluster")
    assert "[YKP_SpecInstaller] CRITICAL | Unrecognized flag: \'--bogus-argument\'" in ret.stderr


def test_create_kernelspec(script_runner, mock_kernels_dir):
    my_env = os.environ.copy()
    my_env.update({"JUPYTER_DATA_DIR": mock_kernels_dir})
    ret = script_runner.run('jupyter-yarn-kernelspec', 'install', '--spark_home=/foo/bar',
                            '--yarn_endpoint=http://acme.com:9999', '--user', env=my_env)
    assert ret.success
    assert ret.stderr.startswith("[YKP_SpecInstaller] Installing Yarn Kernel Provider")
    assert ret.stdout == ''

    assert os.path.isdir(os.path.join(mock_kernels_dir, 'kernels', 'yarnkp_spark_python'))
    assert os.path.isfile(os.path.join(mock_kernels_dir, 'kernels', 'yarnkp_spark_python', 'yarnkp_kernel.json'))

    with open(os.path.join(mock_kernels_dir, 'kernels', 'yarnkp_spark_python', 'yarnkp_kernel.json'), "r") as fd:
        kernel_json = json.load(fd)
        assert kernel_json["env"]["SPARK_HOME"] == '/foo/bar'
        assert kernel_json["metadata"]["lifecycle_manager"]["config"]["yarn_endpoint"] == 'http://acme.com:9999'


def test_create_python_kernelspec(script_runner, mock_kernels_dir):
    my_env = os.environ.copy()
    my_env.update({"JUPYTER_DATA_DIR": mock_kernels_dir})
    my_env.update({"SPARK_HOME": "/bar/foo"})
    ret = script_runner.run('jupyter-yarn-kernelspec', 'install', '--display_name="My Python Kernel"',
                            '--kernel_name=my_python_kernel', '--user', env=my_env)
    assert ret.success
    assert ret.stderr.startswith("[YKP_SpecInstaller] Installing Yarn Kernel Provider")
    assert ret.stdout == ''

    assert os.path.isdir(os.path.join(mock_kernels_dir, 'kernels', 'my_python_kernel'))
    assert os.path.isfile(os.path.join(mock_kernels_dir, 'kernels', 'my_python_kernel', 'yarnkp_kernel.json'))

    with open(os.path.join(mock_kernels_dir, 'kernels', 'my_python_kernel', 'yarnkp_kernel.json'), "r") as fd:
        kernel_json = json.load(fd)
        assert kernel_json["display_name"] == 'My Python Kernel'
        assert kernel_json["env"]["SPARK_HOME"] == '/bar/foo'
        assert kernel_json["metadata"]["lifecycle_manager"]["config"]["yarn_endpoint"] is None


def test_create_r_kernelspec(script_runner, mock_kernels_dir):
    my_env = os.environ.copy()
    my_env.update({"JUPYTER_DATA_DIR": mock_kernels_dir})
    my_env.update({"SPARK_HOME": "/bar/foo"})
    ret = script_runner.run('jupyter-yarn-kernelspec', 'install', '--language=R', '--display_name="My R Kernel"',
                            '--kernel_name=my_r_kernel', '--user', env=my_env)
    assert ret.success
    assert ret.stderr.startswith("[YKP_SpecInstaller] Installing Yarn Kernel Provider")
    assert ret.stdout == ''

    assert os.path.isdir(os.path.join(mock_kernels_dir, 'kernels', 'my_r_kernel'))
    assert os.path.isfile(os.path.join(mock_kernels_dir, 'kernels', 'my_r_kernel', 'yarnkp_kernel.json'))

    with open(os.path.join(mock_kernels_dir, 'kernels', 'my_r_kernel', 'yarnkp_kernel.json'), "r") as fd:
        kernel_json = json.load(fd)
        assert kernel_json["display_name"] == 'My R Kernel'
        assert kernel_json["env"]["SPARK_HOME"] == '/bar/foo'
        argv = kernel_json["argv"]
        assert argv[len(argv) - 1] == 'lazy'


def test_create_scala_kernelspec(script_runner, mock_kernels_dir):
    my_env = os.environ.copy()
    my_env.update({"JUPYTER_DATA_DIR": mock_kernels_dir})
    my_env.update({"SPARK_HOME": "/bar/foo"})
    ret = script_runner.run('jupyter-yarn-kernelspec', 'install', '--language=Scala',
                            '--display_name="My Scala Kernel"', '--kernel_name=my_scala_kernel',
                            '--extra_spark_opts=--MyExtraSparkOpts', '--user', env=my_env)
    assert ret.success
    assert ret.stderr.startswith("[YKP_SpecInstaller] Installing Yarn Kernel Provider")
    assert ret.stdout == ''

    assert os.path.isdir(os.path.join(mock_kernels_dir, 'kernels', 'my_scala_kernel'))
    assert os.path.isfile(os.path.join(mock_kernels_dir, 'kernels', 'my_scala_kernel', 'yarnkp_kernel.json'))

    with open(os.path.join(mock_kernels_dir, 'kernels', 'my_scala_kernel', 'yarnkp_kernel.json'), "r") as fd:
        kernel_json = json.load(fd)
        assert kernel_json["display_name"] == 'My Scala Kernel'
        assert '--MyExtraSparkOpts' in kernel_json["env"]["__TOREE_SPARK_OPTS__"]


def test_create_dask_kernelspec(script_runner, mock_kernels_dir):
    my_env = os.environ.copy()
    my_env.update({"JUPYTER_DATA_DIR": mock_kernels_dir})
    my_env.update({"SPARK_HOME": "/bar/dask"})
    ret = script_runner.run('jupyter', 'yarn-kernelspec', 'install', '--language=R',
                            '--dask', '--display_name="My Dask Kernel"', '--kernel_name=my_dask_kernel',
                            '--python_root=/usr/bogus', '--user', env=my_env)
    assert ret.success
    assert "[YKP_SpecInstaller] Installing Yarn Kernel Provider" in ret.stderr
    assert ret.stderr.startswith("[YKP_SpecInstaller] WARNING | Dask support only works with Python")
    assert ret.stdout == ''

    assert os.path.isdir(os.path.join(mock_kernels_dir, 'kernels', 'my_dask_kernel'))
    assert os.path.isfile(os.path.join(mock_kernels_dir, 'kernels', 'my_dask_kernel', 'yarnkp_kernel.json'))

    with open(os.path.join(mock_kernels_dir, 'kernels', 'my_dask_kernel', 'yarnkp_kernel.json'), "r") as fd:
        kernel_json = json.load(fd)
        assert kernel_json["language"] == 'python'
        assert kernel_json["display_name"] == 'My Dask Kernel'
        assert kernel_json["env"]["SPARK_HOME"] == '/bar/dask'
        assert kernel_json["env"]["DASK_YARN_EXE"] == '/usr/bogus/bin/dask-yarn'
        argv = kernel_json["argv"]
        assert argv[len(argv) - 1] == 'none'
