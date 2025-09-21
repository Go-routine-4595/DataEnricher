package domain

import (
	"encoding/json"
	"errors"
)

type Message struct {
	SourceTopic string          `json:"source_topic"`
	DeviceID    string          `json:"device_id"`
	Data        json.RawMessage `json:"data"`
}

type EnrichedMessage struct {
	SourceTopic string          `json:"source_topic"`
	DeviceID    string          `json:"device_id"`
	SiteCode    string          `json:"site_code"`
	Data        json.RawMessage `json:"data"`
	RegistryRaw json.RawMessage `json:"registry"`
	DataModel   string
}

func (e *EnrichedMessage) UnmarshalJSON(data []byte) error {
	var (
		m   EnrichedMessage
		err error
	)
	err = json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	err = m.getSiteCode()
	if err != nil {
		return err
	}
	err = m.getDataModel()
	if err != nil {
		return err
	}
	*e = m
	return nil
}

func (e *EnrichedMessage) getSiteCode() error {
	var (
		Registry map[string]interface{}
		err      error
	)

	err = json.Unmarshal(e.RegistryRaw, &Registry)
	if err != nil {
		return err
	}
	if _, ok := Registry["siteCode"]; !ok {
		return errors.New("siteCode not found")
	}
	siteCode, _ := Registry["siteCode"].(string)
	e.SiteCode = siteCode

	return nil
}

func (e *EnrichedMessage) getDataModel() error {
	var (
		Registry map[string]interface{}
		err      error
	)
	err = json.Unmarshal(e.RegistryRaw, &Registry)
	if err != nil {
		return err
	}
	if _, ok := Registry["dataModel"]; !ok {
		return errors.New("dataModel not found")
	}
	dataModel, _ := Registry["dataModel"].(string)
	e.DataModel = dataModel
	return nil
}

func (e *EnrichedMessage) Byte() ([]byte, error) {
	b, err := json.Marshal(e)
	return b, err
}
