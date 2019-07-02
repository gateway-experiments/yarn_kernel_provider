"""Provides support for launching and managing kernels within a YARN cluster."""

from remote_kernel_provider.provider import RemoteKernelProviderBase


class YarnKernelProvider(RemoteKernelProviderBase):

    id = 'yarn'
    kernel_file  = 'yarn_kernel.json'
    expected_process_class = 'yarn_kernel_provider.yarn.YarnClusterProcessProxy'
    supported_process_classes = [
        'enterprise_gateway.services.processproxies.yarn.YarnClusterProcessProxy',
        expected_process_class
    ]
