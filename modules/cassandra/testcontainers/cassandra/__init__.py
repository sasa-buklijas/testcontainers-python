from testcontainers.core.config import MAX_TRIES
from testcontainers.core.generic import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


class CassandraContainer(DockerContainer):
    """
    Cassandra database container.

    Example
    -------
    .. doctest::

        >>> from testcontainers.cassandra import CassandraContainer

        >>> with CassandraContainer() as cassandra:
        ...     cluster = cassandra.get_cluster()
        ...     with cluster.connect() as session:
        ...          result = session.execute(
        ...             "CREATE KEYSPACE keyspace1 WITH replication = "
        ...             "{'class': 'SimpleStrategy', 'replication_factor': '1'};")
    """

    def __init__(self, image="cassandra:latest", ports_to_expose=(9042,)):
        super().__init__(image)
        self.ports_to_expose = ports_to_expose
        self.with_exposed_ports(*self.ports_to_expose)

        self.with_env("CASSANDRA_SNITCH", "GossipingPropertyFileSnitch")
        self.with_env("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
        self.with_env("JVM_OPTS",
                      "-Dcassandra.skip_wait_for_gossip_to_settle=0 "
                      "-Dcassandra.initial_token=0")
        self.with_env("HEAP_NEWSIZE", "128M")
        self.with_env("MAX_HEAP_SIZE", "1024M")
        self.with_env("CASSANDRA_DC", "datacenter1")

    def start(self):
        super().start()
        wait_for_logs(self, predicate="Starting listening for CQL clients", timeout=MAX_TRIES)
        return self

    def get_cluster(self, **kwargs):
        from cassandra.cluster import Cluster

        container = self.get_wrapped_container()
        container.reload()
        hostname = self.get_container_host_ip()
        port = self.get_exposed_port(9042)
        return Cluster(contact_points=[hostname], port=port, **kwargs)
