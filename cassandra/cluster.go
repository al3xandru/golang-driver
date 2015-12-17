package cassandra

// #cgo LDFLAGS: -L/usr/local/lib -lcassandra
// #cgo CFLAGS: -I/usr/local/include
// #include <stdlib.h>
// #include <cassandra.h>
import "C"
import (
	"strings"
	"unsafe"
)

type Cluster struct {
	cptr *C.struct_CassCluster_
}

func NewCluster(contactPoints ...string) *Cluster {
	cluster := new(Cluster)
	cluster.cptr = C.cass_cluster_new()
	cContactPoints := C.CString(strings.Join(contactPoints, ","))
	defer C.free(unsafe.Pointer(cContactPoints))
	C.cass_cluster_set_contact_points(cluster.cptr, cContactPoints)

	return cluster
}

func (cluster *Cluster) SetProtocolVersion(version uint8) {
	if version < 1 {
		panic("protocol version must be > 1")
	}
	C.cass_cluster_set_protocol_version(cluster.cptr, C.int(version))
}

// Disable retrieving and updating schema metadata.
// When disabled, the cluster initialization is faster, but this
// also disables the token-aware routing of requests
func (cluster *Cluster) SetUseSchemaMetadata(flag bool) {
	if flag {
		C.cass_cluster_set_use_schema(cluster.cptr, C.cass_bool_t(1))
	} else {
		C.cass_cluster_set_use_schema(cluster.cptr, C.cass_bool_t(0))

	}
}

func (cluster *Cluster) SetConnectionTimeout(timeout uint) {
	C.cass_cluster_set_connect_timeout(cluster.cptr, C.uint(timeout))
}

func (cluster *Cluster) SetRequestTimeout(timeout uint) {
	C.cass_cluster_set_request_timeout(cluster.cptr,
		C.uint(timeout))
}

func (cluster *Cluster) SetConnectionOptions(opts connectionOptions) error {
	if opts.ConnectionTimeout != unsetValue {
		C.cass_cluster_set_connect_timeout(cluster.cptr, C.uint(opts.ConnectionTimeout))
	}
	if opts.HeartbeatInterval != unsetValue {
		C.cass_cluster_set_connection_heartbeat_interval(cluster.cptr,
			C.uint(opts.HeartbeatInterval))
	}
	if opts.ReconnectionWaitTime != unsetValue {
		C.cass_cluster_set_reconnect_wait_time(cluster.cptr,
			C.uint(opts.ReconnectionWaitTime))
	}
	if opts.ConnectionIdleTimeout != unsetValue {
		C.cass_cluster_set_connection_idle_timeout(cluster.cptr,
			C.uint(opts.ConnectionIdleTimeout))
	}
	if opts.TcpNoDelay {
		C.cass_cluster_set_tcp_nodelay(cluster.cptr,
			C.cass_bool_t(1))
	} else {
		C.cass_cluster_set_tcp_nodelay(cluster.cptr,
			C.cass_bool_t(0))
	}
	if opts.TcpKeepAlive {
		C.cass_cluster_set_tcp_keepalive(cluster.cptr,
			C.cass_bool_t(1), C.uint(opts.TcpKeepAliveDelay))
	}
	var cerr C.CassError = C.CASS_OK
	if opts.CoreConnectionsPerHost != unsetValue {
		cerr = C.cass_cluster_set_core_connections_per_host(cluster.cptr,
			C.uint(opts.CoreConnectionsPerHost))
	}
	if opts.MaxConnectionsPerHost != unsetValue {
		cerr = C.cass_cluster_set_max_connections_per_host(cluster.cptr,
			C.uint(opts.MaxConnectionsPerHost))
	}
	if opts.MaxConcurrentConnectionCreation != unsetValue {
		cerr = C.cass_cluster_set_max_concurrent_creation(cluster.cptr,
			C.uint(opts.MaxConcurrentConnectionCreation))
	}
	if opts.ParallelIOThreads != unsetValue {
		cerr = C.cass_cluster_set_num_threads_io(cluster.cptr,
			C.uint(opts.ParallelIOThreads))
	}
	if opts.WriteBytesHighWatermark != unsetValue {
		cerr = C.cass_cluster_set_write_bytes_high_water_mark(cluster.cptr,
			C.uint(opts.WriteBytesHighWatermark))
	}
	if opts.WriteBytesLowWatermark != unsetValue {
		cerr = C.cass_cluster_set_write_bytes_high_water_mark(cluster.cptr,
			C.uint(opts.WriteBytesLowWatermark))
	}
	if cerr != C.CASS_OK {
		return newError(cerr)
	}
	return nil
}

func (cluster *Cluster) SetQueueOptions(opts queueOptions) error {
	var cerr C.CassError = C.CASS_OK
	if opts.MaxEvents != unsetValue {
		cerr = C.cass_cluster_set_queue_size_event(cluster.cptr,
			C.uint(opts.MaxEvents))
	}
	if opts.MaxIO != unsetValue {
		cerr = C.cass_cluster_set_queue_size_io(cluster.cptr,
			C.uint(opts.MaxIO))
	}
	// if opts.MaxLog != unsetValue {
	// 	cerr = C.cass_cluster_set_queue_size_log(cluster.cptr,
	// 		C.uint(opts.MaxLog))
	// }
	if cerr != C.CASS_OK {
		return newError(cerr)
	}
	return nil
}

func (cluster *Cluster) SetRequestOptions(opts requestOptions) error {
	if opts.RequestTimeout != unsetValue {
		C.cass_cluster_set_request_timeout(cluster.cptr,
			C.uint(opts.RequestTimeout))
	}
	var cerr C.CassError = C.CASS_OK
	if opts.MaxConcurrentRequests != unsetValue {
		cerr = C.cass_cluster_set_max_concurrent_requests_threshold(cluster.cptr,
			C.uint(opts.MaxConcurrentRequests))
	}
	if opts.MaxRequestsPerFlush != unsetValue {
		cerr = C.cass_cluster_set_max_requests_per_flush(cluster.cptr,
			C.uint(opts.MaxRequestsPerFlush))
	}
	if opts.PendingRequestsHighWatermark != unsetValue {
		cerr = C.cass_cluster_set_pending_requests_high_water_mark(cluster.cptr,
			C.uint(opts.PendingRequestsHighWatermark))
	}
	if opts.PendingRequestsLowWatermark != unsetValue {
		cerr = C.cass_cluster_set_pending_requests_low_water_mark(cluster.cptr,
			C.uint(opts.PendingRequestsLowWatermark))
	}
	if cerr != C.CASS_OK {
		return newError(cerr)
	}
	return nil
}

func (cluster *Cluster) Close() {
	C.cass_cluster_free(cluster.cptr)
	cluster.cptr = nil
}

func (cluster *Cluster) Connect() (*Session, error) {
	session := new(Session)
	session.cptr = C.cass_session_new()

	future := async(func() *C.struct_CassFuture_ {
		return C.cass_session_connect(session.cptr, cluster.cptr)
	})
	defer future.Close()

	if err := future.Error(); err != nil {
		return nil, err
	}
	return session, nil
}

type connectionOptions struct {
	ConnectionTimeout               uint
	HeartbeatInterval               uint
	ReconnectionWaitTime            uint
	ConnectionIdleTimeout           uint
	CoreConnectionsPerHost          uint
	MaxConnectionsPerHost           uint
	MaxConcurrentConnectionCreation uint
	TcpKeepAlive                    bool
	TcpKeepAliveDelay               uint
	TcpNoDelay                      bool
	ParallelIOThreads               uint
	WriteBytesHighWatermark         uint
	WriteBytesLowWatermark          uint
}

const unsetValue = ^uint(0)

// Configure only the settings that are of interest. The
// rest will use the defaults.
func NewConnectionOptions() connectionOptions {
	return connectionOptions{unsetValue, unsetValue, unsetValue, unsetValue,
		unsetValue, unsetValue, unsetValue,
		false, unsetValue, false,
		unsetValue,
		unsetValue, unsetValue}
}

type requestOptions struct {
	RequestTimeout               uint
	MaxConcurrentRequests        uint
	MaxRequestsPerFlush          uint
	PendingRequestsHighWatermark uint
	PendingRequestsLowWatermark  uint
}

func NewRequestOptions() requestOptions {
	return requestOptions{unsetValue, unsetValue, unsetValue,
		unsetValue, unsetValue}
}

type queueOptions struct {
	MaxEvents uint
	MaxIO     uint
	MaxLog    uint
}

func NewQueueOptions() queueOptions {
	return queueOptions{unsetValue, unsetValue, unsetValue}
}
