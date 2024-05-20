from django.conf import settings
from django.test.runner import DiscoverRunner
from testcontainers.iris import IRISContainer


class IRISDiscoverRunner(DiscoverRunner):
    _iris_container: IRISContainer

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        postgresql_image = getattr(
            settings, "IRIS_IMAGE", "intersystemsdc/iris-community:latest"
        )
        postgresql_user = getattr(settings, "IRIS_USERNAME", "_SYSTEM")
        postgresql_password = getattr(settings, "IRIS_PASSWORD", "SYS")
        postgresql_name = getattr(settings, "IRIS_NAMESPACE", "USER")

        self._iris_container = IRISContainer(
            image=postgresql_image,
            username=postgresql_user,
            password=postgresql_password,
            namespace=postgresql_name,
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

    def _teardown_container(self):
        self._iris_container.stop()

    def setup_databases(self, **kwargs):
        self._setup_container()
        return super().setup_databases(**kwargs)

    def teardown_databases(self, old_config, **kwargs):
        super().teardown_databases(old_config, **kwargs)
        self._teardown_container()
