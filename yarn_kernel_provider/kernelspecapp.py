"""Installs kernel specs for use by YarnKernelProvider"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import os
import os.path
import json
import sys

from distutils import dir_util
from os import listdir
from string import Template
from traitlets.config.application import Application
from jupyter_core.application import (
    JupyterApp, base_flags, base_aliases
)
from traitlets import Instance, Dict, Unicode, Bool, default
from jupyter_kernel_mgmt.kernelspec import KernelSpec, KernelSpecManager
from remote_kernel_provider import spec_utils

from .provider import YarnKernelProvider
from . import __version__

KERNEL_JSON = "yarnkp_kernel.json"
PYTHON = 'python'
DEFAULT_LANGUAGE = PYTHON
SUPPORTED_LANGUAGES = [PYTHON, 'scala', 'r']
DEFAULT_KERNEL_NAMES = {
    DEFAULT_LANGUAGE: 'yarnkp_spark_python',
    'scala': 'yarnkp_spark_scala',
    'r': 'yarnkp_spark_r',
    'dask': 'yarnkp_dask_python'}
DEFAULT_DISPLAY_NAMES = {
    DEFAULT_LANGUAGE: 'Spark Python (YARN Cluster)',
    'scala': 'Spark Scala (YARN Cluster)',
    'r': 'Spark R (YARN Cluster)',
    'dask': 'Dask Python (YARN Cluster)'}
DEFAULT_INIT_MODE = 'lazy'
SPARK_INIT_MODES = [DEFAULT_INIT_MODE, 'eager', 'none']


class YKP_SpecInstaller(JupyterApp):
    """CLI for extension management."""
    name = u'jupyter-yarn-kernelspec'
    description = u'A Jupyter kernel for talking to Spark/Dask within a YARN cluster'
    examples = '''
    jupyter-yarn-kernelspec install --language=R --spark_home=/usr/local/spark
    jupyter-yarn-kernelspec install --kernel_name=dask_python --dask --yarn_endpoint=http://foo.bar:8088/ws/v1/cluster
    jupyter-yarn-kernelspec install --language=Scala --spark_init_mode='eager'
    '''
    kernel_spec_manager = Instance(KernelSpecManager)

    def _kernel_spec_manager_default(self):
        return KernelSpecManager(kernel_file=YarnKernelProvider.kernel_file)

    source_dir = Unicode()
    staging_dir = Unicode()
    template_dir = Unicode()

    kernel_name = Unicode(DEFAULT_KERNEL_NAMES[DEFAULT_LANGUAGE], config=True,
                          help='Install the kernel spec into a directory with this name.')

    display_name = Unicode(DEFAULT_DISPLAY_NAMES[DEFAULT_LANGUAGE], config=True,
                           help='The display name of the kernel - used by user-facing applications.')

    # Yarn endpoint
    yarn_endpoint_env = 'YKP_YARN_ENDPOINT'
    yarn_endpoint = Unicode(None, config=True, allow_none=True,
                            help="""The http url specifying the YARN Resource Manager. Note: If this value is NOT set,
                            the YARN library will use the files within the local HADOOP_CONFIG_DIR to determine the
                            active resource manager. (YKP_YARN_ENDPOINT env var)""")

    @default('yarn_endpoint')
    def yarn_endpoint_default(self):
        return os.getenv(self.yarn_endpoint_env)

    # Alt Yarn endpoint
    alt_yarn_endpoint_env = 'YKP_ALT_YARN_ENDPOINT'
    alt_yarn_endpoint = Unicode(None, config=True, allow_none=True,
                                help="""The http url specifying the alternate YARN Resource Manager.  This value should
                                be set when YARN Resource Managers are configured for high availability.  Note: If both
                                YARN endpoints are NOT set, the YARN library will use the files within the local
                                HADOOP_CONFIG_DIR to determine the active resource manager.
                                (YKP_ALT_YARN_ENDPOINT env var)""")

    @default('alt_yarn_endpoint')
    def alt_yarn_endpoint_default(self):
        return os.getenv(self.alt_yarn_endpoint_env)

    yarn_endpoint_security_enabled_env = 'YKP_YARN_ENDPOINT_SECURITY_ENABLED'
    yarn_endpoint_security_enabled_default_value = False
    yarn_endpoint_security_enabled = Bool(yarn_endpoint_security_enabled_default_value, config=True,
                                          help="""Is YARN Kerberos/SPNEGO Security enabled (True/False).
                                          (YKP_YARN_ENDPOINT_SECURITY_ENABLED env var)""")

    @default('yarn_endpoint_security_enabled')
    def yarn_endpoint_security_enabled_default(self):
        return bool(os.getenv(self.yarn_endpoint_security_enabled_env,
                              self.yarn_endpoint_security_enabled_default_value))

    language = Unicode('Python', config=True,
                       help="The language of the underlying kernel.  Must be one of 'Python', 'R', or "
                            "'Scala'.  Default = 'Python'.")

    python_root = Unicode('/opt/conda', config=True,
                          help="Specify where the root of the python installation resides (parent dir of bin/python).")

    spark_home = Unicode(os.getenv("SPARK_HOME", '/usr/hdp/current/spark2-client'), config=True,
                         help="Specify where the spark files can be found.")

    spark_init_mode = Unicode(DEFAULT_INIT_MODE, config=True,
                              help="Spark context initialization mode.  Must be one of 'lazy', 'eager', "
                                   "or 'none'.  Default = 'lazy'.")

    extra_spark_opts = Unicode('', config=True, help="Specify additional Spark options.")

    extra_dask_opts = Unicode('', config=True, help="Specify additional Dask options.")

    # Flags
    user = Bool(False, config=True,
                help="Try to install the kernel spec to the per-user directory instead of the system "
                     "or environment directory.")

    prefix = Unicode('', config=True,
                     help="Specify a prefix to install to, e.g. an env. The kernelspec will be "
                          "installed in PREFIX/share/jupyter/kernels/")

    dask = Bool(False, config=True, help="Kernelspec will be configured for Dask YARN.")

    aliases = {
        'prefix': 'YKP_SpecInstaller.prefix',
        'kernel_name': 'YKP_SpecInstaller.kernel_name',
        'display_name': 'YKP_SpecInstaller.display_name',
        'yarn_endpoint': 'YKP_SpecInstaller.yarn_endpoint',
        'alt_yarn_endpoint': 'YKP_SpecInstaller.alt_yarn_endpoint',
        'yarn_endpoint_security_enabled': 'YKP_SpecInstaller.yarn_endpoint_security_enabled',
        'language': 'YKP_SpecInstaller.language',
        'python_root': 'YKP_SpecInstaller.python_root',
        'spark_home': 'YKP_SpecInstaller.spark_home',
        'spark_init_mode': 'YKP_SpecInstaller.spark_init_mode',
        'extra_spark_opts': 'YKP_SpecInstaller.extra_spark_opts',
        'extra_dask_opts': 'YKP_SpecInstaller.extra_dask_opts',
    }
    aliases.update(base_aliases)

    flags = {'user': ({'YKP_SpecInstaller': {'user': True}},
                      "Install to the per-user kernel registry"),
             'sys-prefix': ({'YKP_SpecInstaller': {'prefix': sys.prefix}},
                            "Install to Python's sys.prefix. Useful in conda/virtual environments."),
             'dask': ({'YKP_SpecInstaller': {'dask': True}},
                      "Install kernelspec for Dask YARN."),
             'debug': base_flags['debug'], }

    def parse_command_line(self, argv=None):
        super(YKP_SpecInstaller, self).parse_command_line(argv=argv)

    def start(self):
        # validate parameters, ensure values are present
        self._validate_parameters()

        # create staging dir
        self.staging_dir = spec_utils.create_staging_directory()

        # copy files from installed area to staging dir
        self.source_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'kernelspecs', self.template_dir))
        dir_util.copy_tree(src=self.source_dir, dst=self.staging_dir)
        spec_utils.copy_kernelspec_files(self.staging_dir, launcher_type=self.language, resource_type=self.language)

        # install to destination
        self.log.info("Installing Yarn Kernel Provider kernel specification for '{}'".format(self.display_name))
        install_dir = self.kernel_spec_manager.install_kernel_spec(self.staging_dir,
                                                                   kernel_name=self.kernel_name,
                                                                   user=self.user,
                                                                   prefix=self.prefix)

        # apply template values at destination (since one of the values is the destination directory)
        self._finalize_kernel_json(install_dir)

    def _finalize_kernel_json(self, location):
        """Apply substitutions to the kernel.json string, update a kernel spec using these values,
           then write to the target kernel.json file.
        """
        subs = self._get_substitutions(location)
        kernel_json_str = ''
        with open(os.path.join(location, KERNEL_JSON)) as f:
            for line in f:
                line = line.split('#', 1)[0]
                kernel_json_str = kernel_json_str + line
            f.close()
        post_subs = Template(kernel_json_str).safe_substitute(subs)
        kernel_json = json.loads(post_subs)

        # Instantiate default KernelSpec, then update with the substitutions.  This allows for new fields
        # to be added that we might not yet know about.
        kernel_spec = KernelSpec().to_dict()
        kernel_spec.update(kernel_json)

        kernel_json_file = os.path.join(location, KERNEL_JSON)
        self.log.debug("Finalizing kernel json file for kernel: '{}'".format(self.display_name))
        with open(kernel_json_file, 'w+') as f:
            json.dump(kernel_spec, f, indent=2)

    def _validate_parameters(self):
        if self.user and self.prefix:
            self._log_and_exit("Can't specify both user and prefix. Please choose one or the other.")

        entered_language = self.language
        self.language = self.language.lower()
        if self.language not in SUPPORTED_LANGUAGES:
            self._log_and_exit("Language '{}' is not in the set of supported languages: {}".
                               format(entered_language, SUPPORTED_LANGUAGES))

        if self.dask:
            if self.language != PYTHON:
                self.log.warning("Dask support only works with Python, changing language from {} to Python.".
                                 format(entered_language))
                self.language = PYTHON
            # if kernel and display names are still defaulted, silently change to dask defaults
            if self.kernel_name == DEFAULT_KERNEL_NAMES[DEFAULT_LANGUAGE]:
                self.kernel_name = DEFAULT_KERNEL_NAMES['dask']
            if self.display_name == DEFAULT_DISPLAY_NAMES[DEFAULT_LANGUAGE]:
                self.display_name = DEFAULT_DISPLAY_NAMES['dask']

            self.template_dir = DEFAULT_KERNEL_NAMES['dask']
            self.spark_init_mode = 'none'
            if len(self.extra_spark_opts) > 0:
                self.log.warning("--extra_spark_opts will be ignored for Dask-based kernelspecs.")
                self.extra_spark_opts = ''
        else:
            # if kernel and display names are still defaulted, silently change to language defaults
            if self.kernel_name == DEFAULT_KERNEL_NAMES[DEFAULT_LANGUAGE]:
                self.kernel_name = DEFAULT_KERNEL_NAMES[self.language]
            if self.display_name == DEFAULT_DISPLAY_NAMES[DEFAULT_LANGUAGE]:
                self.display_name = DEFAULT_DISPLAY_NAMES[self.language]

            self.template_dir = DEFAULT_KERNEL_NAMES[self.language]
            if len(self.extra_dask_opts) > 0:
                self.log.warning("--extra_dask_opts will be ignored for Spark-based kernelspecs.")
                self.extra_dask_opts = ''

        self.spark_init_mode = self.spark_init_mode.lower()
        if self.spark_init_mode not in SPARK_INIT_MODES:
            self._log_and_exit("Spark initialization mode '{}' is not in the set of supported "
                               "initialization modes: {}".format(self.spark_init_mode, SPARK_INIT_MODES))

        # sanitize kernel_name
        self.kernel_name = self.kernel_name.replace(' ', '_')

    def _get_substitutions(self, install_dir):

        substitutions = dict()
        substitutions['spark_home'] = self.spark_home
        substitutions['yarn_endpoint'] = '"{}"'.format(self.yarn_endpoint) if self.yarn_endpoint is not None else 'null'
        substitutions['alt_yarn_endpoint'] = \
            '"{}"'.format(self.alt_yarn_endpoint) if self.alt_yarn_endpoint is not None else 'null'
        substitutions['yarn_endpoint_security_enabled'] = str(self.yarn_endpoint_security_enabled).lower()
        substitutions['extra_spark_opts'] = self.extra_spark_opts
        substitutions['extra_dask_opts'] = self.extra_dask_opts
        substitutions['spark_init_mode'] = self.spark_init_mode
        substitutions['python_root'] = self.python_root
        substitutions['display_name'] = self.display_name
        substitutions['install_dir'] = install_dir
        substitutions['py4j_path'] = ''

        # If this is a python kernel, attempt to get the path to the py4j file.
        if self.language == PYTHON and not self.dask:
            try:
                python_lib_contents = listdir("{0}/python/lib".format(self.spark_home))
                py4j_zip = list(filter(lambda filename: "py4j" in filename, python_lib_contents))[0]

                # This is always a sub-element of a path, so let's prefix with semi-colon
                substitutions['py4j_path'] = ":{0}/python/lib/{1}".format(self.spark_home, py4j_zip)
            except OSError:
                self.log.warn('Unable to find py4j, installing without PySpark support.')
        return substitutions

    def _log_and_exit(self, msg, exit_status=1):
        self.log.error(msg)
        self.exit(exit_status)


class YarnKernelProviderApp(Application):
    version = __version__
    name = 'jupyter yarn-kernelspec'
    description = '''Application used to create kernelspecs for use on Hadoop YARN clusters

    \tYarn Kernel Provider Version: {}
    '''.format(__version__)
    examples = '''
    jupyter yarn-kernelspec install - Installs the kernel as a Jupyter Kernel.
    '''

    subcommands = Dict({
        'install': (YKP_SpecInstaller, YKP_SpecInstaller.description.splitlines()[0]),
    })

    aliases = {}
    flags = {}

    def start(self):
        if self.subapp is None:
            print('No subcommand specified. Must specify one of: {}'.format(list(self.subcommands)))
            print()
            self.print_description()
            self.print_subcommands()
            self.exit(1)
        else:
            return self.subapp.start()


if __name__ == '__main__':
    YarnKernelProviderApp.launch_instance()
