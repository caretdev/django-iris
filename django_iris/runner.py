from django.conf import settings
from django.test.runner import DiscoverRunner
from testcontainers.iris import IRISContainer


class IRISDiscoverRunner(DiscoverRunner):
    _iris_container: IRISContainer
    _iris_user: str
    _iris_password: str
    _iris_namespace: str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        iris_image = getattr(
            settings, "IRIS_IMAGE", "intersystemsdc/iris-community:latest"
        )
        iris_key = getattr(settings, "IRIS_KEY", None)
        self._iris_user = getattr(settings, "IRIS_USERNAME", "DJANGO")
        self._iris_password = getattr(settings, "IRIS_PASSWORD", "django")
        iris_name = getattr(settings, "IRIS_NAMESPACE", "USER")
        extra = {}
        if iris_key:
            extra["license_key"] = iris_key

        self._iris_container = IRISContainer(
            image=iris_image,
            username=self._iris_user,
            password=self._iris_password,
            namespace=iris_name,
            **extra
        )

    def _setup_container(self):
        self._iris_container.start()

        databases = getattr(settings, "DATABASES", ["default"])

        for database in databases:
            settings.DATABASES[database][
                "HOST"
            ] = self._iris_container.get_container_host_ip()
            settings.DATABASES[database]["PORT"] = int(
                self._iris_container.get_exposed_port(1972)
            )
            settings.DATABASES[database]["USER"] = self._iris_user
            settings.DATABASES[database]["PASSWORD"] = self._iris_password

    def _teardown_container(self):
        self._iris_container.stop()

    def setup_databases(self, **kwargs):
        self._setup_container()
        return super().setup_databases(**kwargs)

    def teardown_databases(self, old_config, **kwargs):
        super().teardown_databases(old_config, **kwargs)
        self._teardown_container()
