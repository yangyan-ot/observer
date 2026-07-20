package winston

func (s *WinstonServiceImpl) GetDescription() string {
	return "A simple built-in Winston server for third-party clients (e.g. Earthworm). Public exposure is not recommended, as its polling model can cause high CPU usage."
}
