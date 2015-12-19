package cassandra_test

import (
	"golang-driver/cassandra"
	"testing"
	"time"
)

func TestClusterConfiguration(t *testing.T) {
	connectionPoints := []string{"127.0.0.1"}
	cluster := cassandra.NewCluster(connectionPoints...)
	defer cluster.Close()

	cluster.SetProtocolVersion(4)
	cluster.SetConnectionTimeout(10 * time.Second)
	cluster.SetRequestTimeout(10 * time.Second)

	if err := setConnectionOptions(cluster); err != nil {
		t.Error(err)
	}

	if err := setRequestOptions(cluster); err != nil {
		t.Error(err)
	}

	if err := setClusterQueueOptions(cluster); err != nil {
		t.Error(err)
	}

	session, err := cluster.Connect()
	if err != nil {
		t.Error(err)
	}
	defer session.Close()
}

func setConnectionOptions(cluster *cassandra.Cluster) error {
	opts := cassandra.NewConnectionOptions()
	opts.HeartbeatInterval = 60 // seconds

	return cluster.SetConnectionOptions(opts)
}

func setRequestOptions(cluster *cassandra.Cluster) error {
	opts := cassandra.NewRequestOptions()
	opts.MaxRequestsPerFlush = 128

	return cluster.SetRequestOptions(opts)
}

func setClusterQueueOptions(cluster *cassandra.Cluster) error {
	opts := cassandra.NewQueueOptions()
	opts.MaxLog = 8192

	return cluster.SetQueueOptions(opts)
}
