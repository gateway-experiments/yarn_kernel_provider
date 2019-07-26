# Yarn Kernel Provider

__NOTE: This repository is experimental and undergoing frequent changes!__

The Yarn Kernel Provider package provides support necessary for launching Jupyter kernels within YARN clusters.  It adheres to requirements set forth in the [Jupyter Kernel Management](https://github.com/takluyver/jupyter_kernel_mgmt) refactoring for kernel management and discovery. This is accomplished via two classes:

1. [`YarnKernelProvider`](https://github.com/gateway-experiments/yarn_kernel_provider/blob/master/yarn_kernel_provider/provider.py) is invoked by the application to locate and identify specific kernel specificiations (kernelspecs) that manage kernel lifecycles within a YARN cluster.
2. [`YarnKernelLifecycleManager`](https://github.com/gateway-experiments/yarn_kernel_provider/blob/master/yarn_kernel_provider/yarn.py) is instantiated by the [`RemoteKernelManager`](https://github.com/gateway-experiments/remote_kernel_provider/blob/master/remote_kernel_provider/manager.py) to peform the kernel lifecycle management.  It performs post-launch discovery of the application and handles its termination via the [YARN REST API](https://github.com/toidi/hadoop-yarn-api-python-client).

Installation of yarn_kernel_provider also includes a Jupyter application that can be used to create appropriate kernel specifications relative to YARN Spark and Dask.

## Installation
Yarn Kernel Provider is a pip-installable package:
```bash
pip install yarn_kernel_provider
```

##Usage
Because this version of Jupyter kernel management is still in its experimental stages, a [special branch of Notebook](https://github.com/takluyver/notebook/tree/jupyter-kernel-mgmt) is required, which includes the machinery to leverage the new framework.  An installable build of this branch is available as an asset on the [interim-dev release](https://github.com/gateway-experiments/remote_kernel_provider/releases/tag/v0.1-interim-dev) of the Remote Kernel Provider on which Yarn Kernel Provider depends.

### YARN Kernel Specifications
Criteria for discovery of the kernel specification via the `YarnKernelProvider` is that a `yarnkp_kernel.json` file exist in a sub-directory named `kernels` in the Jupyter path hierarchy. 

Such kernel specifications should be initially created using the included Jupyter application`jupyter-yarn-kernelspec` to insure the minimally viable requirements exist.  This application can be used to create specifications for YARN Spark and Dask.  Spark support is available for three languages: Python, Scala and R, while Dask support is available for Python.

To create kernel specifications for use by YarnKernelProvider use `juptyer yarn-kernelspec install`.  Here are it's parameter options, produced using `jupyter yarn-kernelspec install --help`.  All parameters are optional with no parameters yielding a Python-based kernelspec for Spark on the local YARN cluster.  However, locations for SPARK_HOME and Python runtimes may likely require changes if not provided.

```
A Jupyter kernel for talking to Spark/Dask within a YARN cluster

Options
-------

Arguments that take values are actually convenience aliases to full
Configurables, whose aliases are listed on the help line. For more information
on full configurables, see '--help-all'.

--user
    Install to the per-user kernel registry
--sys-prefix
    Install to Python's sys.prefix. Useful in conda/virtual environments.
--dask
    Install kernelspec for Dask YARN.
--debug
    set log level to logging.DEBUG (maximize logging output)
--prefix=<Unicode> (YKP_SpecInstaller.prefix)
    Default: ''
    Specify a prefix to install to, e.g. an env. The kernelspec will be
    installed in PREFIX/share/jupyter/kernels/
--kernel_name=<Unicode> (YKP_SpecInstaller.kernel_name)
    Default: 'yarnkp_spark_python'
    Install the kernel spec into a directory with this name.
--display_name=<Unicode> (YKP_SpecInstaller.display_name)
    Default: 'Spark Python (YARN Cluster)'
    The display name of the kernel - used by user-facing applications.
--yarn_endpoint=<Unicode> (YKP_SpecInstaller.yarn_endpoint)
    Default: None
    The http url specifying the YARN Resource Manager. Note: If this value is
    NOT set, the YARN library will use the files within the local
    HADOOP_CONFIG_DIR to determine the active resource manager.
    (YKP_YARN_ENDPOINT env var)
--alt_yarn_endpoint=<Unicode> (YKP_SpecInstaller.alt_yarn_endpoint)
    Default: None
    The http url specifying the alternate YARN Resource Manager.  This value
    should be set when YARN Resource Managers are configured for high
    availability.  Note: If both YARN endpoints are NOT set, the YARN library
    will use the files within the local HADOOP_CONFIG_DIR to determine the
    active resource manager. (YKP_ALT_YARN_ENDPOINT env var)
--yarn_endpoint_security_enabled=<Bool> (YKP_SpecInstaller.yarn_endpoint_security_enabled)
    Default: False
    Is YARN Kerberos/SPNEGO Security enabled (True/False).
    (YKP_YARN_ENDPOINT_SECURITY_ENABLED env var)
--language=<Unicode> (YKP_SpecInstaller.language)
    Default: 'Python'
    The language of the underlying kernel.  Must be one of 'Python', 'R', or
    'Scala'.  Default = 'Python'.
--python_root=<Unicode> (YKP_SpecInstaller.python_root)
    Default: '/opt/conda'
    Specify where the root of the python installation resides (parent dir of
    bin/python).
--spark_home=<Unicode> (YKP_SpecInstaller.spark_home)
    Default: '/usr/hdp/current/spark2-client'
    Specify where the spark files can be found.
--spark_init_mode=<Unicode> (YKP_SpecInstaller.spark_init_mode)
    Default: 'lazy'
    Spark context initialization mode.  Must be one of 'lazy', 'eager', or
    'none'.  Default = 'lazy'.
--extra_spark_opts=<Unicode> (YKP_SpecInstaller.extra_spark_opts)
    Default: ''
    Specify additional Spark options.
--extra_dask_opts=<Unicode> (YKP_SpecInstaller.extra_dask_opts)
    Default: ''
    Specify additional Dask options.
--log-level=<Enum> (Application.log_level)
    Default: 30
    Choices: (0, 10, 20, 30, 40, 50, 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL')
    Set the log level by value or name.
--config=<Unicode> (JupyterApp.config_file)
    Default: ''
    Full path of a config file.

To see all available configurables, use `--help-all`

Examples
--------

    jupyter-yarn-kernelspec install --language=R --spark_home=/usr/local/spark
    jupyter-yarn-kernelspec install --kernel_name=dask_python --dask --yarn_endpoint=http://foo.bar:8088/ws/v1/cluster
    jupyter-yarn-kernelspec install --language=Scala --spark_init_mode='eager'
``` 
