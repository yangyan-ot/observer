package helicorder

import (
	"fmt"

	"github.com/anyshake/observer/config"
)

func (s *HelicorderServiceImpl) Init() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, con := range s.GetConfigConstraint() {
		if err := con.Init(s.dataProvider.actionHandler); err != nil {
			return fmt.Errorf("failed to initlize config constraint for service %s, namespace %s, key %s: %w", ID, con.GetNamespace(), con.GetKey(), err)
		}
	}

	stationCode, err := (&config.StationStationCodeConfigConstraintImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return err
	}
	s.dataProvider.stationCode = stationCode.(string)

	networkCode, err := (&config.StationNetworkCodeConfigConstraintImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return err
	}
	s.dataProvider.networkCode = networkCode.(string)

	locationCode, err := (&config.StationLocationCodeConfigConstraintImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return err
	}
	s.dataProvider.locationCode = locationCode.(string)

	filePath, err := (&helicorderConfigFilePathImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder config file path: %w", err)
	}
	s.filePath = filePath.(string)

	cacheStorage, err := (&helicorderConfigCacheStorageImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder cache storage: %w", err)
	}
	s.cacheStorage = cacheStorage.(string)

	cachePath, err := (&helicorderConfigCachePathImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder cache path: %w", err)
	}
	s.cachePath = cachePath.(string)

	queryCache, err := newQueryCache(s.cacheStorage, s.cachePath)
	if err != nil {
		return fmt.Errorf("failed to initialize helicorder cache: %w", err)
	}
	if queryCache != nil {
		if err := queryCache.Clear(); err != nil {
			return fmt.Errorf("failed to clear stale helicorder cache: %w", err)
		}
	}
	s.dataProvider.queryCache = queryCache

	imageFormat, err := (&helicorderConfigImageFormatImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder image format: %w", err)
	}
	s.imageFormat = imageFormat.(string)

	timeSpan, err := (&helicorderConfigTimeSpanImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder time span: %w", err)
	}
	s.timeSpan = timeSpan.(int)

	spanSamples, err := (&helicorderConfigSpanSamplesImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder span samples: %w", err)
	}
	s.spanSamples = spanSamples.(int)

	imageSize, err := (&helicorderConfigImageSizeImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder image size: %w", err)
	}
	s.imageSize = imageSize.(int)

	lineWidth, err := (&helicorderConfigLineWidthImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder line width: %w", err)
	}
	s.lineWidth = lineWidth.(float64)

	scaleFactors, err := (&helicorderConfigScaleFactorsImpl{}).Get(s.dataProvider.actionHandler)
	if err != nil {
		return fmt.Errorf("failed to get helicorder waveform scale factor: %w", err)
	}
	s.scaleFactors = scaleFactors.([]float64)

	return nil
}
