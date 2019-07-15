"""Provides support for launching and managing kernels within a YARN cluster."""

from remote_kernel_provider.provider import RemoteKernelProviderBase


class YarnKernelProvider(RemoteKernelProviderBase):

    id = 'yarn'
    kernel_file = 'yarn_kernel.json'
    lifecycle_manager_classes = ['yarn_kernel_provider.yarn.YarnKernelLifecycleManager']
