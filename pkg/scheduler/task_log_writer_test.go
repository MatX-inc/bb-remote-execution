package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type taskLogSink func(p []byte) (int, error)

func (f taskLogSink) Write(p []byte) (int, error) { return f(p) }

// getTaskLogLinesLost returns the number of lines that the writers of
// this process have lost so far.
func getTaskLogLinesLost(t *testing.T) float64 {
	registry := prometheus.NewRegistry()
	registry.MustRegister(inMemoryBuildQueueTaskLogLinesLostTotal)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	return metricFamilies[0].GetMetric()[0].GetCounter().GetValue()
}

// Lines are written as they come, not only once the context is done,
// and Run() does not return before it is.
func TestTaskLogWriterRun(t *testing.T) {
	written := make(chan string, 1)
	writer := NewTaskLogWriter(taskLogSink(func(p []byte) (int, error) {
		written <- string(p)
		return len(p), nil
	}), 1)
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		writer.Run(ctx)
		close(returned)
	}()

	linesLost := getTaskLogLinesLost(t)
	for _, ev := range []string{"queued", "finished"} {
		writer.Write(&TaskLogLine{Seq: 1, Ev: ev})
		select {
		case data := <-written:
			require.Equal(t, `{"seq":1,"boot":0,"t":"","ev":"`+ev+`"}`+"\n", data)
		case <-time.After(10 * time.Second):
			t.Fatal("Line was not written")
		}
	}
	select {
	case <-returned:
		t.Fatal("Run() returned before its context was done")
	case <-time.After(taskLogDrainTimeout + 100*time.Millisecond):
	}
	cancel()
	<-returned
	require.Equal(t, linesLost, getTaskLogLinesLost(t))
}

// At shutdown Run() waits for a sink that is slow to take the lines
// that are still buffered, and no longer.
func TestTaskLogWriterSlowSink(t *testing.T) {
	written := 0
	writer := NewTaskLogWriter(taskLogSink(func(p []byte) (int, error) {
		time.Sleep(taskLogDrainTimeout / 100)
		written++
		return len(p), nil
	}), 4)
	for seq := uint64(1); seq <= 4; seq++ {
		writer.Write(&TaskLogLine{Seq: seq})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	writer.Run(ctx)
	require.Equal(t, 4, written)
	require.Less(t, time.Since(start), taskLogDrainTimeout)
}

// Lines that do not fit in the buffer, that are not JSON or that the
// sink fails to write are counted as lost. The writer carries on.
func TestTaskLogWriterLinesLost(t *testing.T) {
	var out []string
	writer := NewTaskLogWriter(taskLogSink(func(p []byte) (int, error) {
		if len(out) == 1 {
			out = append(out, "")
			return 0, errors.New("Disk on fire")
		}
		out = append(out, string(p))
		return len(p), nil
	}), 4)
	linesLost := getTaskLogLinesLost(t)
	writer.Write(&TaskLogLine{Seq: 1})
	writer.Write(&TaskLogLine{Seq: 2})
	writer.Write(&TaskLogLine{Seq: 3, Worker: json.RawMessage("{")})
	writer.Write(&TaskLogLine{Seq: 4})
	require.Equal(t, linesLost, getTaskLogLinesLost(t))
	writer.Write(&TaskLogLine{Seq: 5})
	require.Equal(t, linesLost+1, getTaskLogLinesLost(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer.Run(ctx)
	require.Equal(t, []string{
		`{"seq":1,"boot":0,"t":"","ev":""}` + "\n",
		"",
		`{"seq":4,"boot":0,"t":"","ev":""}` + "\n",
	}, out)
	require.Equal(t, linesLost+3, getTaskLogLinesLost(t))
}
