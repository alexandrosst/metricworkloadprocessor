// package: github.com/yourorg/metricworkloadprocessor
package metricworkloadprocessor

import (
	"context"
	"math/rand"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

const typeStr = "metricworkload"

// Only per-item mode is supported for realism
type Config struct {
	LatencyMS       int `mapstructure:"latency_ms"`
	LatencyJitterMS int `mapstructure:"latency_jitter_ms"`
	CPUus           int `mapstructure:"cpu_us"`
	CPUJitterPct    int `mapstructure:"cpu_jitter_pct"`
	MemMiB          int `mapstructure:"mem_mib"`
	MemSlices       int `mapstructure:"mem_slices"`

	// item mode shaping:
	PerItemProb   float64 `mapstructure:"per_item_prob"`   // 0..1, chance to cost each datapoint
	ItemGroupSize int     `mapstructure:"item_group_size"` // cost per N datapoints (e.g., 50)

	// scaling:
	ScaleWithWorkload bool `mapstructure:"scale_with_workload"` // if true, scale CPU/mem with batch size
}

func createDefaultConfig() component.Config {
	return &Config{
		MemSlices:     4,
		PerItemProb:   1.0,
		ItemGroupSize: 1,
	}
}

type processorState struct {
	cfg   *Config
	alloc [][]byte
}

func (ps *processorState) ensureMem(memMiB int) {
	want := memMiB * 1024 * 1024
	if want <= 0 {
		ps.alloc = nil
		return
	}
	if ps.cfg.MemSlices <= 0 {
		ps.cfg.MemSlices = 1
	}
	sz := want / ps.cfg.MemSlices
	if sz <= 0 {
		sz = want
	}
	// Allocate if needed
	if len(ps.alloc) != ps.cfg.MemSlices || (len(ps.alloc) > 0 && len(ps.alloc[0]) != sz) {
		ps.alloc = make([][]byte, ps.cfg.MemSlices)
		for i := range ps.alloc {
			b := make([]byte, sz)
			ps.alloc[i] = b
		}
	}
	// Periodically re-touch memory to simulate ongoing usage
	for i := range ps.alloc {
		b := ps.alloc[i]
		for j := 0; j < len(b); j += 4096 {
			b[j] = byte(j)
		}
	}
}

func jitterAbs(base, jitter int) int {
	if base <= 0 && jitter <= 0 {
		return 0
	}
	return max(0, base+rand.Intn(2*jitter+1)-jitter)
}
func jitterPct(base, pct int) int {
	if base <= 0 || pct <= 0 {
		return base
	}
	d := base * pct / 100
	return max(0, base+rand.Intn(2*d+1)-d)
}

func sleepMS(ms int) {
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func burnCPU(us int) {
	if us <= 0 {
		return
	}
	deadline := time.Now().Add(time.Duration(us) * time.Microsecond)
	x := 1.0
	for time.Now().Before(deadline) {
		x = x*1.0000001 + 0.0000003
	}
	_ = x
}

func (ps *processorState) injectOnce(multiplier int) {
	memMiB := ps.cfg.MemMiB
	cpuUs := ps.cfg.CPUus
	if ps.cfg.ScaleWithWorkload && multiplier > 0 {
		memMiB = ps.cfg.MemMiB * multiplier
		cpuUs = ps.cfg.CPUus * multiplier
	}
	ps.ensureMem(memMiB)
	lat := jitterAbs(ps.cfg.LatencyMS, ps.cfg.LatencyJitterMS)
	sleepMS(lat)
	cpu := jitterPct(cpuUs, ps.cfg.CPUJitterPct)
	burnCPU(cpu)
}

func (ps *processorState) injectPerItems(count int) {
	if count <= 0 {
		return
	}
	group := ps.cfg.ItemGroupSize
	if group < 1 {
		group = 1
	}
	n := (count + group - 1) / group
	for i := 0; i < n; i++ {
		if rand.Float64() > ps.cfg.PerItemProb {
			continue
		}
		multiplier := group
		if i == n-1 {
			// Last group may be smaller
			multiplier = count - (n-1)*group
		}
		ps.injectOnce(multiplier)
	}
}

type metricsProc struct {
	ps   *processorState
	next consumer.Metrics
}

func (mp *metricsProc) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (mp *metricsProc) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	count := int(md.DataPointCount())
	mp.ps.injectPerItems(count)
	return mp.next.ConsumeMetrics(ctx, md)
}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithMetrics(createMetrics, component.StabilityLevelDevelopment),
	)
}

func createMetrics(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	ps := &processorState{cfg: cfg.(*Config)}
	return &metricsProc{ps: ps, next: next}, nil
}

// Start implements the processor.Metrics interface.
func (mp *metricsProc) Start(ctx context.Context, host component.Host) error {
	// No startup logic needed for this processor.
	return nil
}

// Shutdown implements the processor.Metrics interface.
func (mp *metricsProc) Shutdown(ctx context.Context) error {
	// No resources to clean up in this processor.
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
