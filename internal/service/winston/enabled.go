package winston

func (s *WinstonServiceImpl) IsEnabled() bool {
	enable, err := (&winstonConfigEnabledImpl{}).Get(s.actionHandler)
	if err != nil {
		return false
	}
	if en, ok := enable.(bool); !ok || !en {
		return false
	}
	return true
}
