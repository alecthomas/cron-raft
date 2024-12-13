package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/hashicorp/cronexpr"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"golang.org/x/sync/errgroup"
)

var cli struct {
	Peers   []string `help:"List of Raft peers." default:"127.0.0.1:63001,127.0.0.1:63002,127.0.0.1:63003"`
	DataDir string   `help:"Raft data directory." default:"data"`
	Shards  uint64   `help:"Number of Raft shards." default:"64"`

	ID uint64 `arg:"" help:"Node ID." required:"" placeholder:"ID"`
}

func main() {
	kctx := kong.Parse(&cli)
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	// logger.GetLogger("logdb").SetLevel(logger.ERROR)
	// logger.GetLogger("config").SetLevel(logger.ERROR)
	// logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	initialMembers := make(map[uint64]string)
	for idx, v := range cli.Peers {
		initialMembers[uint64(idx+1)] = v
	}
	nodeAddr := initialMembers[cli.ID]
	rc := config.Config{
		ReplicaID:          cli.ID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	dataDir := filepath.Join(cli.DataDir, fmt.Sprintf("node%d", cli.ID))
	nhc := config.NodeHostConfig{
		WALDir:         dataDir,
		NodeHostDir:    dataDir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	kctx.FatalIfErrorf(err)
	defer nh.Close()

	// Create the Raft groups.
	for shard := range cli.Shards {
		rc.ShardID = shard
		err := nh.StartReplica(initialMembers, false, NewCronStateMachine(nh), rc)
		kctx.FatalIfErrorf(err)
	}

	jobs := make(chan CronJob, 16)

	wg, ctx := errgroup.WithContext(ctx)
	// Read "<verb> <schedule>" from stdin.
	wg.Go(func() error {
		defer close(jobs)
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				return err
			}
			if s == "exit\n" {
				return fmt.Errorf("exit")
			}
			fields := strings.Fields(s)
			if len(fields) < 2 {
				fmt.Println("invalid input: <verb> <schedule> expected")
				continue
			}
			schedule, err := cronexpr.Parse(strings.Join(fields[1:], " "))
			if err != nil {
				fmt.Println("invalid schedule:", err)
				continue
			}
			h := fnv.New64a()
			h.Write([]byte(s))
			job := CronJob{
				Shard:          h.Sum64() % cli.Shards,
				ScheduleString: strings.Join(fields[1:], " "),
				Verb:           fields[0],
				Schedule:       schedule,
			}
			jobs <- job
		}
	})
	wg.Go(func() error {
		sessions := make([]*client.Session, cli.Shards)
		fmt.Println("Processing cron jobs...")
		for {
			select {
			case <-ctx.Done():
				nh.Close()
				return ctx.Err()

			case job := <-jobs:
				cronJob, err := json.Marshal(job)
				if err != nil {
					fmt.Println("failed to marshal job:", err)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				session := sessions[job.Shard]
				if session == nil {
					session = nh.GetNoOPSession(job.Shard)
					if err != nil {
						return fmt.Errorf("failed to get session for shard %d: %w", job.Shard, err)
					}
					sessions[job.Shard] = session
				}
				_, err = nh.SyncPropose(ctx, session, cronJob)
				if err != nil {
					return fmt.Errorf("failed to propose job: %w", err)
				}
				cancel()
			}
		}
	})
	wg.Wait()
}

type CronJob struct {
	Shard          uint64               `json:"shard"`
	Verb           string               `json:"verb"`
	ScheduleString string               `json:"schedule"`
	Schedule       *cronexpr.Expression `json:"-"`
}

func (c *CronJob) UnmarshalJSON(data []byte) error {
	type Alias CronJob
	var alias Alias
	var err error
	if err = json.Unmarshal(data, &alias); err != nil {
		return err
	}
	if alias.Schedule, err = cronexpr.Parse(alias.ScheduleString); err != nil {
		return err
	}
	*c = CronJob(alias)
	return nil
}

func NewCronStateMachine(nh *dragonboat.NodeHost) func(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return func(clusterID, nodeID uint64) sm.IStateMachine {
		return &CronStateMachine{
			id: nodeID,
			nh: nh,
		}
	}
}

// CronStateMachine executes a cron job.
type CronStateMachine struct {
	id uint64
	nh *dragonboat.NodeHost
}

func (c *CronStateMachine) Close() error {
	panic("unimplemented")
}

func (c *CronStateMachine) Lookup(interface{}) (interface{}, error) {
	panic("unimplemented")
}

func (c *CronStateMachine) RecoverFromSnapshot(io.Reader, []sm.SnapshotFile, <-chan struct{}) error {
	panic("unimplemented")
}

func (c *CronStateMachine) SaveSnapshot(io.Writer, sm.ISnapshotFileCollection, <-chan struct{}) error {
	panic("unimplemented")
}

func (c *CronStateMachine) Update(entry sm.Entry) (sm.Result, error) {
	var job CronJob
	if err := json.Unmarshal(entry.Cmd, &job); err != nil {
		return sm.Result{}, err
	}
	leader, _, _, err := c.nh.GetLeaderID(job.Shard)
	if err != nil {
		return sm.Result{}, err
	}
	if leader == c.id {
		fmt.Printf("Executing job: %s\n", entry.Cmd)
	}
	return sm.Result{Value: uint64(len(entry.Cmd))}, nil
}
