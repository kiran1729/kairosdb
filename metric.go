// Copyright (c) 2014 ZeroStack Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package kairosdb

import (
  "encoding/json"
  "fmt"
)

// DataPoint represents a measurement. Contains the time when measurement
// occurred and its value.
type DataPoint struct {
  // time (in milliseconds from epoch) when the measurement occured.
  TimeMS int64
  // Value can be a float, long or string type.
  Value interface{}
}

// MarshalJSON serializes the DataPoint to JSON.
func (dp *DataPoint) MarshalJSON() ([]byte, error) {
  dt := []interface{}{dp.TimeMS, dp.Value}
  return json.Marshal(dt)
}

// UnmarshalJSON deserializes a datapoint given Array format. This is how
// Kairosb returns a datapoint.
func (dp *DataPoint) UnmarshalJSON(data []byte) error {
  var dpArray []interface{}
  err := json.Unmarshal(data, &dpArray)
  if err != nil {
    return err
  }
  if len(dpArray) != 2 {
    return fmt.Errorf("kairosdb: error deserializing data point: %s",
      string(data))
  }
  dp.TimeMS = int64(dpArray[0].(float64))
  dp.Value = dpArray[1]
  return nil
}

// Metric contains measurements or datapoints.
type Metric struct {
  Name       string            `json:"name"`
  Tags       map[string]string `json:"tags"`
  Typ        string            `json:"-"`
  DataPoints []*DataPoint      `json:"datapoints"`
}

// NewMetric returns an instance of Metric type given name and type.
func NewMetric(name string, typ string) (*Metric, error) {
  if name == "" {
    return nil, fmt.Errorf("kairosdb: metric name can not be empty")
  }
  if typ != "float" && typ != "long" && typ != "string" && typ != "int" &&
    typ != "uint" && typ != "bool" {
    return nil, fmt.Errorf("kairosdb: unsupported data type: %s", typ)
  }
  return &Metric{
    Name:       name,
    Typ:        typ,
    Tags:       make(map[string]string),
    DataPoints: []*DataPoint{},
  }, nil
}

// AddTag adds tag with name and value to the Tag map of the metric.
func (m *Metric) AddTag(name string, value string) *Metric {
  if name != "" && value != "" {
    m.Tags[name] = value
  }
  return m
}

// AddTags adds tags to the Tag map of the metric.
func (m *Metric) AddTags(tags map[string]string) *Metric {
  if tags == nil {
    return m
  }
  for k, v := range tags {
    m.Tags[k] = v
  }
  return m
}

// AddDataPoint appends a data point.
func (m *Metric) AddDataPoint(timeMS int64, value interface{}) *Metric {
  m.DataPoints = append(m.DataPoints,
    &DataPoint{
      TimeMS: timeMS,
      Value:  value,
    })
  return m
}
