"""bcbio-nextgen-vm base exception handling."""


class BCBioException(Exception):
    """Base bcbio-nextgen-vm exception

    To correctly use this class, inherit from it and define
    a `template` property.

    That `template` will be formated using the keyword arguments
    provided to the constructor.

    Example:
    ::
        class InvalidCluster(BCBioException):

            template = "Cluster %(cluser_name)r is not defined in %(config)r."


        raise InvalidCluster(cluser_name="Cluster name",
                             config="cluster.config")
    """

    template = "An unknown exception occurred."

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs
        if not message:
            try:
                message = self.template % kwargs
            except TypeError:
                # Something went wrong during message formatting.
                # Probably kwargs doesn't match a variable in the message.
                message = ("Message: %(template)s. Extra info: %(kwargs)s" %
                           {"template": self.template, "kwargs": kwargs})
                # TODO(alexandrucoman): Log the issue and the kwargs

        super(BCBioException, self).__init__(message)


class NotFound(BCBioException):

    """The required object is not available in container."""

    template = "The %(object)r was not found in %(container)s."
