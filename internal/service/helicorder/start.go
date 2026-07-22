package helicorder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/anyshake/observer/config"
	"github.com/anyshake/observer/internal/dao/action"
	"github.com/anyshake/observer/internal/hardware/explorer"
	"github.com/anyshake/observer/pkg/logger"
	"github.com/bclswl0827/heligo"
	"github.com/samber/lo"
)

// Keep the number of concurrently retained plot spans small. Each worker can
// hold the source samples, normalized samples and rendered points for a span.
const plotWorkers = 2

type provider struct {
	actionHandler *action.Handler
	queryCache    queryCache

	stationCode  string
	networkCode  string
	locationCode string

	channelCode string
}

func (d *provider) GetPlotName() string { return "AnyShake Observer" }
func (d *provider) GetStation() string  { return d.stationCode }
func (d *provider) GetNetwork() string  { return d.networkCode }
func (d *provider) GetChannel() string  { return d.channelCode }
func (d *provider) GetLocation() string { return d.locationCode }
func (d *provider) GetPlotData(startTime, endTime time.Time) ([]heligo.PlotData, error) {
	startTimestamp := startTime.Add(-time.Second)
	endTimestamp := endTime.Add(time.Second)
	cacheKey := queryCacheKey{
		StartUnixMilli: startTimestamp.UnixMilli(),
		EndUnixMilli:   endTimestamp.UnixMilli(),
	}

	if d.queryCache != nil {
		var plotData []heligo.PlotData
		found, err := d.queryCache.Read(cacheKey, d.channelCode, func(sampleRate int, timestamp int64, samples []int32) {
			plotData = appendChannelSamples(plotData, sampleRate, timestamp, samples)
		})
		if err != nil {
			logger.GetLogger(ID).Warnf("failed to read waveform cache for timestamp %d, falling back to database: %v", cacheKey.StartUnixMilli, err)
		} else if found {
			return plotData, nil
		}
	}

	records, err := d.actionHandler.SeisRecordsQuery(startTimestamp, endTimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to query seismic waveform records: %w", err)
	}

	var plotData []heligo.PlotData
	var cacheWriter queryCacheWriter
	if d.queryCache != nil {
		cacheWriter, err = d.queryCache.NewWriter(cacheKey)
		if err != nil {
			logger.GetLogger(ID).Warnf("failed to create waveform cache for timestamp %d: %v", cacheKey.StartUnixMilli, err)
		}
	}
	for _, record := range records {
		_, sampleRate, channelData, err := record.Decode()
		if err != nil {
			if cacheWriter != nil {
				cacheWriter.Abort()
			}
			return nil, fmt.Errorf("failed to decode seismic waveform record on timestamp %d: %w", record.RecordTime, err)
		}
		plotData = appendChannelPlotData(plotData, sampleRate, record.RecordTime, channelData, d.channelCode)
		if cacheWriter != nil {
			if err := cacheWriter.Write(queryCacheData{
				SampleRate:  sampleRate,
				Timestamp:   record.RecordTime,
				ChannelData: channelData,
			}); err != nil {
				logger.GetLogger(ID).Warnf("failed to write waveform cache for timestamp %d: %v", cacheKey.StartUnixMilli, err)
				cacheWriter.Abort()
				cacheWriter = nil
			}
		}
	}

	if cacheWriter != nil {
		if err := cacheWriter.Commit(); err != nil {
			logger.GetLogger(ID).Warnf("failed to commit waveform cache for timestamp %d: %v", cacheKey.StartUnixMilli, err)
		}
	}

	return plotData, nil
}

func appendChannelPlotData(
	plotData []heligo.PlotData,
	sampleRate int,
	timestamp int64,
	channelData []explorer.ChannelData,
	channelCode string,
) []heligo.PlotData {
	if sampleRate <= 0 {
		return plotData
	}

	var samples []int32
	for i := range channelData {
		if channelData[i].ChannelCode == channelCode {
			samples = channelData[i].Data
			break
		}
	}
	if len(samples) == 0 {
		return plotData
	}
	return appendChannelSamples(plotData, sampleRate, timestamp, samples)
}

func appendChannelSamples(plotData []heligo.PlotData, sampleRate int, timestamp int64, samples []int32) []heligo.PlotData {
	if sampleRate <= 0 || len(samples) == 0 {
		return plotData
	}

	// A partial record is still useful. Limiting the count also avoids a panic
	// when malformed input declares a sample rate larger than its data slice.
	sampleCount := min(sampleRate, len(samples))
	start := len(plotData)
	plotData = append(plotData, make([]heligo.PlotData, sampleCount)...)
	for i := range sampleCount {
		timeOffset := int64(i * 1000 / sampleRate)
		plotData[start+i] = heligo.PlotData{
			Time:  time.UnixMilli(timestamp + timeOffset),
			Value: float64(samples[i]),
		}
	}

	return plotData
}

func (d *provider) setChannelCode(channelCode string) {
	d.channelCode = channelCode
}

func (s *HelicorderServiceImpl) handleInterrupt(timer *time.Timer) {
	if !timer.Stop() {
		<-timer.C
	}
	s.wg.Done()
}

func (s *HelicorderServiceImpl) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ctx.Err() != nil {
		s.ctx, s.cancelFn = context.WithCancel(context.Background())
	}

	channelCodes, err := (&config.StationChannelCodesConfigConstraintImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		logger.GetLogger(ID).Errorf("failed to get station channel codes: %v", err)
		return err
	}

	logger.GetLogger(ID).Infof("generated helicorder images will be saved to %s", s.filePath)
	logger.GetLogger(ID).Infof("helicorder waveform cache storage: %s", s.cacheStorage)
	if s.cacheStorage == CACHE_STORAGE_DISK {
		logger.GetLogger(ID).Infof("helicorder waveform cache path: %s", s.cachePath)
	}

	go func() {
		timer := time.NewTimer(time.Minute)

		s.status.SetStartedAt(s.timeSource.Now())
		s.status.SetIsRunning(true)
		defer func() {
			if r := recover(); r != nil {
				logger.GetLogger(ID).Errorf("service unexpectly crashed, recovered from panic: %v\n%s", r, debug.Stack())
				s.handleInterrupt(timer)
				_ = s.Stop()
			}
		}()

		for {
			select {
			case <-s.ctx.Done():
				s.handleInterrupt(timer)
				return
			case <-timer.C:
				// Subtract one minute to avoid date rollover
				currentTime := s.timeSource.Now().Add(-time.Minute)

				hardwareConfig := s.hardwareDev.GetConfig()
				availableChannelCode := hardwareConfig.GetChannelCodes()

				for channelIdx, channelCode := range channelCodes.([]string) {
					if !lo.Contains(availableChannelCode, channelCode) {
						logger.GetLogger(ID).Infof("skipping channel %s: not available in current hardware configuration", channelCode)
						continue
					}

					// Discard channels which scale factor is zero or undefined
					if channelIdx >= len(s.scaleFactors) || s.scaleFactors[channelIdx] == 0 {
						logger.GetLogger(ID).Warnf("skipping channel %s: scale factor is zero or undefined", channelCode)
						continue
					}
					scaleFactor := s.scaleFactors[channelIdx]

					helicorderCtx, err := heligo.New(&s.dataProvider, 24*time.Hour, time.Duration(s.timeSpan)*time.Minute)
					if err != nil {
						logger.GetLogger(ID).Errorf("failed to create helicorder context: %v", err)
						continue
					}

					// Update current channel code
					s.dataProvider.setChannelCode(channelCode)
					logger.GetLogger(ID).Infof("start plotting helicorder for channel %s", channelCode)

					if err = helicorderCtx.Plot(currentTime, plotWorkers, s.spanSamples, scaleFactor, s.lineWidth, nil); err != nil {
						logger.GetLogger(ID).Errorf("failed to plot helicorder for %s: %v", channelCode, err)
						continue
					}

					dateDir := filepath.Join(s.filePath, currentTime.UTC().Format("2006-01-02"))
					filePath := filepath.Join(dateDir, s.getHelicorderFileName(currentTime, channelCode))

					if err := os.MkdirAll(dateDir, 0755); err != nil {
						logger.GetLogger(ID).Errorf("failed to create directory %s: %v", dateDir, err)
						continue
					}

					if err = helicorderCtx.Save(s.imageSize, filePath); err != nil {
						logger.GetLogger(ID).Errorf("failed to save helicorder for %s: %v", channelCode, err)
						continue
					}

					logger.GetLogger(ID).Infof("helicorder for %s has been saved to %s", channelCode, filePath)
				}

				if s.dataProvider.queryCache != nil {
					if err := s.dataProvider.queryCache.Clear(); err != nil {
						logger.GetLogger(ID).Warnf("failed to clear waveform cache: %v", err)
					}
				}

				// Plotting has a deliberately short-lived, comparatively large working set.
				// Return it to the OS after the hourly batch has completed.
				debug.FreeOSMemory()

				if s.lifeCycle > 0 {
					endTime := currentTime.Add(time.Duration(-s.lifeCycle) * time.Hour * 24)
					if err := s.cleanupHelicorderFiles(endTime); err != nil {
						logger.GetLogger(ID).Errorf("failed to purge expired helicorder files: %v", err)
					}
				}

				timer.Reset(s.getDurationToNextTime(s.timeSource.Now()))
			}
		}
	}()

	s.wg.Add(1)
	return nil
}

func (s *HelicorderServiceImpl) getDurationToNextTime(currentTime time.Time) time.Duration {
	timsSpanMinute := int(time.Hour.Minutes())
	currentMinute := currentTime.Minute()
	// Minutes to next time span
	nextQuarter := (currentMinute/timsSpanMinute + 1) * timsSpanMinute % 60
	nextTime := time.Date(
		currentTime.Year(),
		currentTime.Month(),
		currentTime.Day(),
		currentTime.Hour(),
		nextQuarter,
		0, // Reset seconds
		0,
		currentTime.Location(),
	)
	if nextQuarter <= currentMinute {
		nextTime = nextTime.Add(time.Hour)
	}
	return nextTime.Sub(currentTime)
}

func (m *HelicorderServiceImpl) getHelicorderFileName(tm time.Time, channelCode string) string {
	return fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s",
		m.dataProvider.networkCode, m.dataProvider.stationCode, m.dataProvider.locationCode, channelCode,
		tm.UTC().Format("2006"),
		tm.UTC().Format("002"),
		m.imageFormat,
	)
}

func (s *HelicorderServiceImpl) cleanupHelicorderFiles(until time.Time) error {
	err := filepath.Walk(s.filePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			dirTime := info.ModTime()
			if dirTime.Before(until) {
				err := os.RemoveAll(path)
				if err != nil {
					return fmt.Errorf("failed to remove directory: %w", err)
				}
			}
		}

		return nil
	})

	return err
}
