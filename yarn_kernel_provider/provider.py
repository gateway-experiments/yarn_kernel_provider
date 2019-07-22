"""Provides support for launching and managing kernels within a YARN cluster."""

from remote_kernel_provider.provider import RemoteKernelProviderBase


class YarnKernelProvider(RemoteKernelProviderBase):

    id = 'yarnkp'
    kernel_file = 'yarnkp_kernel.json'
    lifecycle_manager_classes = ['yarn_kernel_provider.yarn.YarnKernelLifecycleManager']
