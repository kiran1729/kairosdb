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
  "testing"
  "time"

  "github.com/golang/glog"
  "github.com/stretchr/testify/assert"
  "github.com/zerostackinc/util"
)

func TestQueryWithAbsoluteTime(t *testing.T) {
  q := NewQuery()
  q.SetStartAbsoluteMS(util.GetNowUTCUnix() * 1000).
    SetEndAbsoluteMS(util.GetNowUTCUnix() * 1000)
  b, err := json.Marshal(q)
  assert.Nil(t, err)
  glog.Infof("query JSON: %s", string(b))
}

func TestQueryWithRelativeTime(t *testing.T) {
  q := NewQuery()
  q.SetStartRelative(10, Milliseconds).SetEndRelative(20, Hours).
    SetCacheTime(2 * time.Second)
  b, err := json.Marshal(q)
  assert.Nil(t, err)
  glog.Infof("query JSON: %s", string(b))
}

func TestQueryWithMetric(t *testing.T) {
  q := NewQuery().SetStartRelative(10, Hours).
    SetEndRelative(20, Minutes).
    SetCacheTime(2 * time.Second)

  qm, err := NewQueryMetric("dummy-metric")
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
    AddTagFilter("tag1", []string{"val1", "val2"}).
    AddTagFilter("tag2", []string{"val3"}).
    AddGrouper(NewTagGrouper([]string{"m1", "m2"})).
    AddGrouper(NewTimeGrouper(1, Days, 7)).
    AddGrouper(NewValueGrouper(1000))

  q.AddMetric(qm)

  b, err := json.Marshal(q)
  assert.Nil(t, err)
  glog.Infof("query JSON: %s", string(b))
}
