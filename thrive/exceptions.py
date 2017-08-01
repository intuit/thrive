import inspect
import os


class ThriveBaseException(Exception):
    """
    Base class for all Thrive exceptions. Defines the get message method that
    returns a more informative exception trace through inspection.
    """
    def info(self):
        """
        @rtype: dict
        @return: Exception name, raiser function and raiser module
        """
        sr = super(ThriveBaseException, self).__repr__()
        try:
            callframe = inspect.trace()[-1]
            origin_funcname = callframe[3]
            origin_module = os.path.basename(callframe[1])
        except IndexError:
            origin_funcname = origin_module = ""

        return {"exception": sr, "function": origin_funcname, "module": origin_module}

    def __repr__(self):
        return ",".join(["%s=%s" % (k, v) for k, v in self.info().items()])

    def __str__(self):
        return self.__repr__()


class ThriveHandlerException(ThriveBaseException):
    pass


class ThriveManagerException(ThriveBaseException):
    pass


class HdfsManagerException(ThriveManagerException):
    pass


class HiveManagerException(ThriveManagerException):
    pass


class MetadataManagerException(ThriveManagerException):
    pass


class NewRelicManagerException(ThriveManagerException):
    pass


class OozieManagerException(ThriveManagerException):
    pass


class SplunkManagerException(ThriveManagerException):
    pass


class VerticaManagerException(ThriveManagerException):
    pass


class LoadHandlerException(ThriveHandlerException):
    pass


class SetupHandlerException(ThriveHandlerException):
    pass


class CleanupHandlerException(ThriveHandlerException):
    pass


class ReplayHandlerException(ThriveHandlerException):
    pass


class RollbackHandlerException(ThriveHandlerException):
    pass


class ConfigLoaderException(ThriveBaseException):
    pass

class SplunkAlertException(ThriveManagerException):
    pass






