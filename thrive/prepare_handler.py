import os
import zipfile
import logging
import platform
from thrive.thrive_handler import ThriveHandler, ThriveHandlerException
from thrive.utils import logkv

logger = logging.getLogger(__name__)


class PrepareHandlerException(ThriveHandlerException):
    pass


class PrepareHandler(ThriveHandler):

    @staticmethod
    def zip26(_resources_file, _outdir, _tozip):
        """
        Creation of zip artifact for Python2.6

        @type _resources_file: str
        @param _resources_file: Name of the output zip artifact

        @type _outdir: str
        @param _outdir: Directory location of the zip artifact

        @type _tozip: list
        @param _tozip: List of files to zip into the artifactls

        @rtype: None
        @return: None

        @exception: Exception
        """
        try:
            zf = zipfile.ZipFile(_resources_file, "w")
            for _file in _tozip:
                _fpath = os.path.join(_outdir, _file)
                logkv(logger, {"msg": "Zipping", "file": _file}, "info")
                zf.write(_fpath, os.path.basename(_fpath))
        except Exception as ex:
            logkv(logger, {"msg": "Error creating resources file",
                           "resources_file": _resources_file,
                           "error": ex}, "error")
            raise PrepareHandlerException
        finally:
            zf.close()

    @staticmethod
    def zip27(_resources_file, _outdir, _tozip):
        """
        Creation of zip artifact for Python2.7

        @type _resources_file: str
        @param _resources_file: Name of the output zip artifact

        @type _outdir: str
        @param _outdir: Directory location of the zip artifact

        @type _tozip: list
        @param _tozip: List of files to zip into the artifact

        @rtype: None
        @return: None

        @exception: Exception
        """

        with zipfile.ZipFile(_resources_file, "w") as zf:
            for _file in _tozip:
                _fpath = os.path.join(_outdir, _file)
                logkv(logger, {"msg": "Zipping", "file": _file}, "info")
                zf.write(_fpath, os.path.basename(_fpath))

    def assemble(self):
        """
        Assemble artifacts for feeding the setup phase

        @rtype: None
        @return: None
        """

        # Check if dataset path folder exists
        if not os.path.exists(self.get_config("nfs_dataset_path")):
            os.makedirs(self.get_config("nfs_dataset_path"))

        # Create list of all resources
        ob_dir = os.path.join(self.get_config("nfs_root"),
                              "onboarding")
        ob_resources = list(os.walk(ob_dir))[1:]

        print ob_dir
        print ob_resources
        # Zip all resources
        for odir, _, files in ob_resources:
            tozip = [f for f in files if not f.endswith(".zip")]
            dataset = os.path.basename(odir)
            # resources_file = os.path.join(odir,
            #                               "%s-%s.zip" % (dataset, "resources"))
            resources_file = os.path.join(self.get_config("nfs_dataset_path"),
                                          "%s-%s.zip" % (dataset, "resources"))
            try:
                logkv(logger, {"msg": "Creating resources file",
                               "resources_file": resources_file}, "info")

                # Context managers for ZipFile are available for Python >=2.7. Since
                # we want to be backward compatible with Python2.6, we have to switch
                # based on Python version
                PYTHON_VERSION = platform.python_version()[:3]
                if PYTHON_VERSION == "2.6":
                    PrepareHandler.zip26(resources_file, odir, tozip)
                elif PYTHON_VERSION == "2.7":
                    PrepareHandler.zip27(resources_file, odir, tozip)
                else:
                    raise PrepareHandlerException("Unsupported Python major version")
            except Exception as ex:
                logkv(logger, {"msg": "Error creating resources file",
                               "resources_file": resources_file,
                               "error": ex}, "error")
                raise PrepareHandlerException

    def execute(self):
        """
        Top level execute method for PrepareHandler

        @rtype: None
        @return: None
        """
        self.assemble()


