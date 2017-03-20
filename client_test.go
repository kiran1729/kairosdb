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
// +build kairosdb

package kairosdb

import (
  "flag"
  "strings"
  "testing"
  "time"

  "github.com/stretchr/testify/assert"
  "github.com/zerostackinc/util"
)

var (
  kairosdbURL = flag.String("kairosdb_url", "http://localhost:8080",
    "Kairosdb server URL")
)

// TestQueryMetric test does the following
// deletes existing metric data
// insert a few datapoints for a metric
// verifies if they are inserted successfully
// deletes the inserted metrics
// verifies the metrics are indeed deleted
func TestQueryMetric(t *testing.T) {
  c, err := NewHTTPClient(*kairosdbURL, nil)
  assert.Nil(t, err)

  metricName := "test-counter"

  deleteMetricData(t, c, metricName)

  m, err := NewMetric("test-counter", "long")
  assert.NoError(t, err)
  m = m.AddTag("tag1", "val1")
  numDatapoints := 10
  for i := 0; i < numDatapoints; i++ {
    timeStamp := util.GetNowUTCUnixMS() + int64(i)
    m = m.AddDataPoint(timeStamp, 10)
  }
  assert.Nil(t, err)

  err = c.PushMetrics([]*Metric{m}, 5*time.Second)
  assert.Nil(t, err)

  // test Metrics() method
  metricNames, err := c.Metrics(5 * time.Second)
  assert.NoError(t, err)
  assert.True(t, util.SliceContainsStr(metricNames, metricName))

  q := NewQuery().SetStartRelative(1, Hours)

  qm, err := NewQueryMetric(metricName)
  assert.Nil(t, err)
  assert.NotNil(t, qm)

  avgAg, err := NewAggregator("avg", 50, Milliseconds)
  assert.Nil(t, err)

  minAg, err := NewAggregator("min", 50, Milliseconds)
  assert.Nil(t, err)

  qm.SetLimit(4000).
    SetOrder(ASC).
    AddAggregator(avgAg).
    AddAggregator(minAg).
    AddTagFilter("tag1", []string{"val1"})

  q.AddMetric(qm)

  timeout := time.NewTimer(5 * time.Second)
  ticker := time.NewTicker(100 * time.Millisecond)

  done := false
  for !done {
    select {
    case <-timeout.C:
      assert.Fail(t, "timed out verifying metrics insertion")
      done = true
      break
    case <-ticker.C:
      res, err := c.Query(q, 1*time.Second)
      assert.Nil(t, err)
      assert.NotNil(t, res)
      if len(res.Queries) > 0 && res.Queries[0].SampleSize == numDatapoints {
        done = true
      }
    }
  }
  ticker.Stop()

  deleteMetricData(t, c, metricName)

  timeout = time.NewTimer(5 * time.Second)
  ticker = time.NewTicker(100 * time.Millisecond)

  done = false
  for !done {
    select {
    case <-timeout.C:
      assert.Fail(t, "timed out verifying metrics deletion")
      done = true
      break
    case <-ticker.C:
      res, err := c.Query(q, 1*time.Second)
      assert.Nil(t, err)
      assert.NotNil(t, res)
      if len(res.Queries) > 0 && res.Queries[0].SampleSize == 0 {
        done = true
      }
    }
  }
  ticker.Stop()
}

func deleteMetricData(t *testing.T, c *HTTPClient, metricName string) {
  // delete Metric
  q := NewQuery().SetStartAbsoluteMS(1)
  qm, err := NewQueryMetric(metricName)
  assert.Nil(t, err)
  assert.NotNil(t, qm)
  // deleteQM.AddTagFilter("tag1", []string{"val1"})
  q.AddMetric(qm)

  err = c.Delete(q, 5*time.Second)
  assert.NoError(t, err)
}
