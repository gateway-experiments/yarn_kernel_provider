# Yarn Kernel Provider
The Yarn Kernel Provider package provides support necessary for launching Jupyter kernels within YARN clusters.  This is accomplished via two classes:

1. [`YarnKernelProvider`](https://github.com/gateway-experiments/yarn_kernel_provider/blob/master/yarn_kernel_provider/provider.py) is invoked by the application to locate and identify specific kernel specificiations (kernelspecs) that manage kernel lifecycles within a YARN cluster.
2. [`YarnClusterProcessProxy`](https://github.com/gateway-experiments/yarn_kernel_provider/blob/master/yarn_kernel_provider/yarn.py) is instantiated by the [`RemoteKernelManager`](https://github.com/gateway-experiments/remote_kernel_provider/blob/master/remote_kernel_provider/manager.py) to peform the kernel lifecycle management.  It performs post-launch discovery of the application and handles its termination via the [YARN REST API](https://github.com/toidi/hadoop-yarn-api-python-client).

## Installation
`YarnKernelProvider` is a pip-installable package:
```bash
pip install yarn_kernel_provider
```

## YARN Kernel Specifications
Criteria for discovery of the kernel specification via the `YarnKernelProvider` is that a `kernel.json` file exist in a sub-directory of `yarn_kernels`.  