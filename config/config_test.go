package config

import (
	"bytes"
	"gopkg.in/yaml.v2"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	var testCases = []struct {
		name string
		file string
		cfg  Config
	}{
		{
			"full description",
			"testdata/full.yml",
			Config{
				Caches: []Cache{
					{
						Name:      "longterm",
						Dir:       "/path/to/longterm/cachedir",
						MaxSize:   ByteSize(100 << 30),
						Expire:    Duration(time.Hour),
						GraceTime: Duration(20 * time.Second),
					},
					{
						Name:    "shortterm",
						Dir:     "/path/to/shortterm/cachedir",
						MaxSize: ByteSize(100 << 20),
						Expire:  Duration(10 * time.Second),
					},
				},
				Server: Server{
					HTTP: HTTP{
						ListenAddr: ":9090",
						TimeoutCfg: TimeoutCfg{
							ReadTimeout:  Duration(5 * time.Minute),
							WriteTimeout: Duration(10 * time.Minute),
							IdleTimeout:  Duration(20 * time.Minute),
						},
					},
				},
				LogDebug: true,

				Clusters: []Cluster{
					{
						Name:  "first cluster",
						Nodes: []string{"127.0.0.1:8123", "shard2:8123"},
						KillQueryUser: KillQueryUser{
							Name:     "default",
							Password: "***",
						},
						ClusterUsers: []ClusterUser{
							{
								Name:                 "web",
								Password:             "password",
								MaxConcurrentQueries: 4,
								MaxExecutionTime:     Duration(time.Minute),
							},
						},
						HeartBeatInterval: Duration(time.Minute),
					},
					{
						Name: "second cluster",
						Replicas: []Replica{
							{
								Name:  "replica1",
								Nodes: []string{"127.0.1.1:8443", "127.0.1.2:8443"},
							},
							{
								Name:  "replica2",
								Nodes: []string{"127.0.2.1:8443", "127.0.2.2:8443"},
							},
						},
						ClusterUsers: []ClusterUser{
							{
								Name:                 "default",
								MaxConcurrentQueries: 4,
								MaxExecutionTime:     Duration(time.Minute),
							},
							{
								Name:                 "web",
								ReqPerMin:            10,
								MaxConcurrentQueries: 4,
								MaxExecutionTime:     Duration(10 * time.Second),
								MaxQueueSize:         50,
								MaxQueueTime:         Duration(70 * time.Second),
							},
						},
						HeartBeatInterval: Duration(5 * time.Second),
					},
				},

				ParamGroups: []ParamGroup{
					{
						Name: "cron-job",
						Params: []Param{
							{
								Key:   "max_memory_usage",
								Value: "40000000000",
							},
							{
								Key:   "max_bytes_before_external_group_by",
								Value: "20000000000",
							},
						},
					},
					{
						Name: "web",
						Params: []Param{
							{
								Key:   "max_memory_usage",
								Value: "5000000000",
							},
							{
								Key:   "max_columns_to_read",
								Value: "30",
							},
							{
								Key:   "max_execution_time",
								Value: "30",
							},
						},
					},
				},

				Users: []User{
					{
						Name:         "web",
						Password:     "****",
						ToCluster:    "first cluster",
						ToUser:       "web",
						DenyHTTP:     true,
						AllowCORS:    true,
						ReqPerMin:    4,
						MaxQueueSize: 100,
						MaxQueueTime: Duration(35 * time.Second),
						Cache:        "longterm",
						Params:       "web",
					},
					{
						Name:                 "default",
						ToCluster:            "second cluster",
						ToUser:               "default",
						MaxConcurrentQueries: 4,
						MaxExecutionTime:     Duration(time.Minute),
					},
				},
			},
		},
		{
			"default values",
			"testdata/default_values.yml",
			Config{
				Server: Server{
					HTTP: HTTP{
						ListenAddr: ":8080",
						TimeoutCfg: TimeoutCfg{
							ReadTimeout:  Duration(time.Minute),
							WriteTimeout: Duration(time.Minute),
							IdleTimeout:  Duration(10 * time.Minute),
						},
					},
				},
				Clusters: []Cluster{
					{
						Name:  "cluster",
						Nodes: []string{"127.0.0.1:8123"},
						ClusterUsers: []ClusterUser{
							{
								Name: "default",
							},
						},
						HeartBeatInterval: Duration(5 * time.Second),
					},
				},
				Users: []User{
					{
						Name:      "default",
						ToCluster: "cluster",
						ToUser:    "default",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := LoadFile(tc.file)
			if err != nil {
				t.Fatalf("Error parsing %s: %s", tc.file, err)
			}
			got, err := yaml.Marshal(c)
			if err != nil {
				t.Fatalf("%s", err)
			}
			exp, err := yaml.Marshal(&tc.cfg)
			if err != nil {
				t.Fatalf("%s", err)
			}
			if !bytes.Equal(got, exp) {
				t.Fatalf("unexpected config result: \ngot\n\n%s\n expected\n\n%s", got, exp)
			}
		})
	}
}

func TestBadConfig(t *testing.T) {
	var testCases = []struct {
		name  string
		file  string
		error string
	}{
		{
			"no file",
			"testdata/nofile.yml",
			"open testdata/nofile.yml: no such file or directory",
		},
		{
			"extra fields",
			"testdata/bad.extra_fields.yml",
			"unknown fields in cluster \"second cluster\": unknown_field",
		},
		{
			"empty users",
			"testdata/bad.empty_users.yml",
			"`users` must contain at least 1 user",
		},
		{
			"empty nodes",
			"testdata/bad.empty_nodes.yml",
			"either `cluster.nodes` or `cluster.replicas` must be set for \"second cluster\"",
		},
		{
			"empty replica nodes",
			"testdata/bad.empty_replica_nodes.yml",
			"`replica.nodes` cannot be empty for \"bar\"",
		},
		{
			"nodes and replicas",
			"testdata/bad.nodes_and_replicas.yml",
			"`cluster.nodes` cannot be simultaneously set with `cluster.replicas` for \"second cluster\"",
		},
		{
			"max queue size and time on user",
			"testdata/bad.queue_size_time_user.yml",
			"`max_queue_size` must be set if `max_queue_time` is set for \"default\"",
		},
		{
			"max queue size and time on cluster_user",
			"testdata/bad.queue_size_time_cluster_user.yml",
			"`max_queue_size` must be set if `max_queue_time` is set for \"default\"",
		},
		{
			"cache max size",
			"testdata/bad.cache_max_size.yml",
			"cannot parse byte size \"-10B\": it must be positive float followed by optional units. For example, 1.5Gb, 3T",
		},
		{
			"empty param group name",
			"testdata/bad.param_groups.name.yml",
			"`param_group.name` must be specified",
		},
		{
			"empty param group params",
			"testdata/bad.param_groups.params.yml",
			"`param_group.params` must contain at least one param",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFile(tc.file)
			if err == nil {
				t.Fatalf("error expected")
			}
			if err.Error() != tc.error {
				t.Fatalf("expected: %q; got: %q", tc.error, err)
			}
		})
	}
}

func TestExamples(t *testing.T) {
	var testCases = []struct {
		name string
		file string
	}{
		{
			"simple",
			"examples/simple.yml",
		},
		{
			"spread inserts",
			"examples/spread.inserts.yml",
		},
		{
			"spread selects",
			"examples/spread.selects.yml",
		},
		{
			"combined",
			"examples/combined.yml",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFile(tc.file)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestParseDuration(t *testing.T) {
	var testCases = []struct {
		value    string
		expected time.Duration
	}{
		{
			"10ns",
			time.Duration(10),
		},
		{
			"20µs",
			20 * time.Microsecond,
		},
		{
			"30ms",
			30 * time.Millisecond,
		},
		{
			"40s",
			40 * time.Second,
		},
		{
			"50m",
			50 * time.Minute,
		},
		{
			"60h",
			60 * time.Hour,
		},
		{
			"75d",
			75 * 24 * time.Hour,
		},
		{
			"80w",
			80 * 7 * 24 * time.Hour,
		},
	}
	for _, tc := range testCases {
		v, err := parseDuration(tc.value)
		if err != nil {
			t.Fatalf("unexpected duration conversion error: %s", err)
		}
		got := time.Duration(v)
		if got != tc.expected {
			t.Fatalf("unexpected value - got: %v; expected: %v", got, tc.expected)
		}
		if v.String() != tc.value {
			t.Fatalf("unexpected toString conversion - got: %q; expected: %q", v, tc.value)
		}
	}
}

func TestParseDurationNegative(t *testing.T) {
	var testCases = []struct {
		value, error string
	}{
		{
			"10",
			"not a valid duration string: \"10\"",
		},
		{
			"20ks",
			"not a valid duration string: \"20ks\"",
		},
		{
			"30Ms",
			"not a valid duration string: \"30Ms\"",
		},
		{
			"40 ms",
			"not a valid duration string: \"40 ms\"",
		},
		{
			"50y",
			"not a valid duration string: \"50y\"",
		},
		{
			"1.5h",
			"not a valid duration string: \"1.5h\"",
		},
	}
	for _, tc := range testCases {
		_, err := parseDuration(tc.value)
		if err == nil {
			t.Fatalf("expected to get parse error; got: nil")
		}
		if err.Error() != tc.error {
			t.Fatalf("unexpected error - got: %q; expected: %q", err, tc.error)
		}
	}
}

func TestConfigTimeouts(t *testing.T) {
	var testCases = []struct {
		name        string
		file        string
		expectedCfg TimeoutCfg
	}{
		{
			"default",
			"testdata/default_values.yml",
			TimeoutCfg{
				ReadTimeout:  Duration(time.Minute),
				WriteTimeout: Duration(time.Minute),
				IdleTimeout:  Duration(10 * time.Minute),
			},
		},
		{
			"defined",
			"testdata/timeouts.defined.yml",
			TimeoutCfg{
				ReadTimeout:  Duration(time.Minute),
				WriteTimeout: Duration(time.Hour),
				IdleTimeout:  Duration(24 * time.Hour),
			},
		},
		{
			"calculated write 1",
			"testdata/timeouts.write.calculated.yml",
			TimeoutCfg{
				ReadTimeout: Duration(time.Minute),
				// 10 + 1 minute
				WriteTimeout: Duration(11 * 60 * time.Second),
				IdleTimeout:  Duration(10 * time.Minute),
			},
		},
		{
			"calculated write 2",
			"testdata/timeouts.write.calculated2.yml",
			TimeoutCfg{
				ReadTimeout: Duration(time.Minute),
				// 20 + 1 minute
				WriteTimeout: Duration(21 * 60 * time.Second),
				IdleTimeout:  Duration(10 * time.Minute),
			},
		},
		{
			"calculated write 3",
			"testdata/timeouts.write.calculated3.yml",
			TimeoutCfg{
				ReadTimeout: Duration(time.Minute),
				// 50 + 1 minute
				WriteTimeout: Duration(51 * 60 * time.Second),
				IdleTimeout:  Duration(10 * time.Minute),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadFile(tc.file)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			got := cfg.Server.HTTP.TimeoutCfg
			if got.ReadTimeout != tc.expectedCfg.ReadTimeout {
				t.Fatalf("got ReadTimeout %v; expected to have: %v", got.ReadTimeout, tc.expectedCfg.ReadTimeout)
			}
			if got.WriteTimeout != tc.expectedCfg.WriteTimeout {
				t.Fatalf("got WriteTimeout %v; expected to have: %v", got.WriteTimeout, tc.expectedCfg.WriteTimeout)
			}
			if got.IdleTimeout != tc.expectedCfg.IdleTimeout {
				t.Fatalf("got IdleTimeout %v; expected to have: %v", got.IdleTimeout, tc.expectedCfg.IdleTimeout)
			}
		})
	}
}
