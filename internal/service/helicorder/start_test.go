package helicorder

import (
	"testing"
	"time"

	"github.com/anyshake/observer/internal/hardware/explorer"
	"github.com/bclswl0827/heligo"
)

func TestAppendChannelPlotData(t *testing.T) {
	t.Parallel()

	const timestamp = int64(1_700_000_000_000)
	prefix := []heligo.PlotData{{Time: time.UnixMilli(timestamp - 1), Value: 99}}
	channels := []explorer.ChannelData{
		{ChannelCode: "HNE", Data: []int32{40, 50, 60}},
		{ChannelCode: "HNZ", Data: []int32{10, 20, 30}},
	}

	got := appendChannelPlotData(prefix, 3, timestamp, channels, "HNZ")
	if len(got) != 4 {
		t.Fatalf("expected prefix plus 3 samples, got %d entries", len(got))
	}
	if got[0] != prefix[0] {
		t.Fatalf("prefix changed: got %+v, want %+v", got[0], prefix[0])
	}

	wantValues := []float64{10, 20, 30}
	wantOffsets := []int64{0, 333, 666}
	for i := range wantValues {
		if got[i+1].Value != wantValues[i] {
			t.Errorf("sample %d value = %v, want %v", i, got[i+1].Value, wantValues[i])
		}
		wantTime := time.UnixMilli(timestamp + wantOffsets[i])
		if !got[i+1].Time.Equal(wantTime) {
			t.Errorf("sample %d time = %v, want %v", i, got[i+1].Time, wantTime)
		}
	}
}

func TestAppendChannelPlotDataSkipsMissingChannel(t *testing.T) {
	t.Parallel()

	prefix := []heligo.PlotData{{Value: 1}}
	got := appendChannelPlotData(prefix, 2, 0, []explorer.ChannelData{
		{ChannelCode: "HNZ", Data: []int32{10, 20}},
	}, "HNN")

	if len(got) != len(prefix) {
		t.Fatalf("missing channel appended %d entries, want %d", len(got), len(prefix))
	}
}

func TestAppendChannelPlotDataHandlesPartialRecord(t *testing.T) {
	t.Parallel()

	got := appendChannelPlotData(nil, 100, 0, []explorer.ChannelData{
		{ChannelCode: "HNZ", Data: []int32{10, 20}},
	}, "HNZ")

	if len(got) != 2 {
		t.Fatalf("partial record produced %d entries, want 2", len(got))
	}
}

func TestAppendChannelPlotDataSkipsInvalidSampleRate(t *testing.T) {
	t.Parallel()

	got := appendChannelPlotData(nil, 0, 0, []explorer.ChannelData{
		{ChannelCode: "HNZ", Data: []int32{10}},
	}, "HNZ")

	if len(got) != 0 {
		t.Fatalf("invalid sample rate produced %d entries", len(got))
	}
}

func BenchmarkAppendChannelPlotData(b *testing.B) {
	channels := []explorer.ChannelData{
		{ChannelCode: "HNE", Data: make([]int32, 100)},
		{ChannelCode: "HNN", Data: make([]int32, 100)},
		{ChannelCode: "HNZ", Data: make([]int32, 100)},
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = appendChannelPlotData(nil, 100, 1_700_000_000_000, channels, "HNZ")
	}
}
