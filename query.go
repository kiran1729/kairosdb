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
  "time"
)

// This files defines Metric query requests related objects. For more details,
// refer to http://kairosdb.github.io/kairosdocs/restapi/QueryMetrics.html

// TimeUnit represent a unit of time e.g. weeks, days, month etc.
type TimeUnit int

// Constants for all the TimeUnit types.
const (
  Unknown TimeUnit = iota
  Milliseconds
  Seconds
  Minutes
  Hours
  Days
  Weeks
  Months
  Years
)

// String returns text representation for a TimeUnit.
func (tm TimeUnit) String() string {
  switch tm {
  case Milliseconds:
    return "milliseconds"
  case Seconds:
    return "seconds"
  case Minutes:
    return "minutes"
  case Hours:
    return "hours"
  case Days:
    return "days"
  case Weeks:
    return "weeks"
  case Months:
    return "months"
  case Years:
    return "years"
  default:
    return "unknown"
  }
}

// GetTimeUnit returns TimeUnit object corresponding to a unit in string.
func GetTimeUnit(unit string) TimeUnit {
  switch unit {
  case "milliseconds":
    return Milliseconds
  case "seconds":
    return Seconds
  case "minutes":
    return Minutes
  case "hours":
    return Hours
  case "days":
    return Days
  case "weeks":
    return Weeks
  case "months":
    return Months
  case "years":
    return Years
  default:
    return Unknown
  }
}

// MarshalJSON returns Marshalled JSON for given TimeUnit.
func (tm *TimeUnit) MarshalJSON() ([]byte, error) {
  return json.Marshal(tm.String())
}

// SortOrder represents sorting order type to be used in Kairosdb queries.
type SortOrder int

// Constants for all the sort order types.
const (
  UNKNOWN SortOrder = iota
  ASC
  DESC
)

// String returns text representation for a SortOrder.
func (o SortOrder) String() string {
  switch o {
  case ASC:
    return "asc"
  case DESC:
    return "desc"
  default:
    return "unknown"
  }
}

// MarshalJSON returns Marshalled JSON for given SortOrder.
func (o SortOrder) MarshalJSON() ([]byte, error) {
  return json.Marshal(o.String())
}

// RelativeTime represents relative time type.
// You can specify a time in relative terms as well. For ex,
// 2 weeks ago, can be represented by unit "weeks" and value 2.
type RelativeTime struct {
  Value int64    `json:"value"`
  Unit  TimeUnit `json:"unit"`
}

// Query represents a query for Kairosdb.
type Query struct {
  startAbsoluteMS int64 // time in millis
  endAbsoluteMS   int64 // time in millis
  startRelative   *RelativeTime
  endRelative     *RelativeTime
  cacheTime       time.Duration
  metrics         []*QueryMetric
}

// NewQuery returns a Query object.
func NewQuery() *Query {
  return &Query{metrics: []*QueryMetric{}}
}

// SetStartAbsoluteMS sets absolute start time.
func (q *Query) SetStartAbsoluteMS(t int64) *Query {
  q.startAbsoluteMS = t
  return q
}

// GetStartAbsoluteMS returns the absolute start time.
func (q *Query) GetStartAbsoluteMS() int64 {
  return q.startAbsoluteMS
}

// SetEndAbsoluteMS sets absolute end time.
func (q *Query) SetEndAbsoluteMS(t int64) *Query {
  q.endAbsoluteMS = t
  return q
}

// GetEndAbsoluteMS returns the absolute end time.
func (q *Query) GetEndAbsoluteMS() int64 {
  return q.endAbsoluteMS
}

// SetCacheTime sets cache time.
func (q *Query) SetCacheTime(t time.Duration) *Query {
  q.cacheTime = t
  return q
}

// SetStartRelative sets startRelative time.
func (q *Query) SetStartRelative(val int64, unit TimeUnit) *Query {
  q.startRelative = &RelativeTime{val, unit}
  return q
}

// SetEndRelative sets endRelative time.
func (q *Query) SetEndRelative(val int64, unit TimeUnit) *Query {
  q.endRelative = &RelativeTime{val, unit}
  return q
}

// AddMetric adds given metric to the given Query.
func (q *Query) AddMetric(qm *QueryMetric) *Query {
  q.metrics = append(q.metrics, qm)
  return q
}

// Metrics is a function that returns the underlying slice. To be used for
// testing only.
func (q *Query) Metrics() []*QueryMetric {
  return q.metrics
}

// MarshalJSON returns marshaled JSON for given Query.
func (q *Query) MarshalJSON() ([]byte, error) {
  qq := map[string]interface{}{}

  if q.startAbsoluteMS > 0 {
    qq["start_absolute"] = q.startAbsoluteMS
  }
  if q.startRelative != nil {
    qq["start_relative"] = q.startRelative
  }

  if q.endAbsoluteMS > 0 {
    qq["end_absolute"] = q.endAbsoluteMS
  }

  if q.endRelative != nil {
    qq["end_relative"] = q.endRelative
  }

  if q.cacheTime > 0 {
    qq["cache_time"] = int64(q.cacheTime.Seconds())
  }

  qq["metrics"] = q.metrics

  return json.Marshal(qq)
}

// QueryMetric represents Metric item to be used by Query object.
type QueryMetric struct {
  Order       SortOrder
  Limit       int
  Name        string
  Aggregators []*Aggregator
  TagFilters  map[string][]string
  Groupers    []Grouper
}

// NewQueryMetric returns an instance of QueryMetric.
func NewQueryMetric(name string) (qm *QueryMetric, err error) {
  if name == "" {
    err = fmt.Errorf("kairosdb: empty name not allowed")
    return
  }
  qm = &QueryMetric{
    Order:       UNKNOWN,
    Name:        name,
    Aggregators: []*Aggregator{},
    TagFilters:  map[string][]string{},
    Groupers:    []Grouper{},
  }
  return
}

// SetLimit sets limit for result set of given metric.
func (qm *QueryMetric) SetLimit(l int) *QueryMetric {
  qm.Limit = l
  return qm
}

// SetOrder sets sort order for the result set of given metric.
func (qm *QueryMetric) SetOrder(o SortOrder) *QueryMetric {
  qm.Order = o
  return qm
}

// AddAggregator adds given aggregator to aggregator list of QueryMetric.
func (qm *QueryMetric) AddAggregator(ag *Aggregator) *QueryMetric {
  qm.Aggregators = append(qm.Aggregators, ag)
  return qm
}

// AddGrouper adds given Grouper to grouper list of QueryMetric.
func (qm *QueryMetric) AddGrouper(g Grouper) *QueryMetric {
  qm.Groupers = append(qm.Groupers, g)
  return qm
}

// AddTagFilter adds tag filters to the list of TagFilters of QueryMetric.
func (qm *QueryMetric) AddTagFilter(tagName string,
  values []string) *QueryMetric {
  qm.TagFilters[tagName] = values
  return qm
}

// MarshalJSON returns Marshaled JSON of given QueryMetric.
func (qm *QueryMetric) MarshalJSON() ([]byte, error) {
  queryMap := map[string]interface{}{
    "name": qm.Name,
  }

  if qm.Order != UNKNOWN {
    queryMap["order"] = qm.Order
  }

  if qm.Limit > 0 {
    queryMap["limit"] = qm.Limit
  }

  if len(qm.Aggregators) > 0 {
    queryMap["aggregators"] = qm.Aggregators
  }

  if len(qm.Groupers) > 0 {
    queryMap["group_by"] = qm.Groupers
  }

  if len(qm.TagFilters) > 0 {
    queryMap["tags"] = qm.TagFilters
  }
  return json.Marshal(queryMap)
}

// Grouper represents Grouper type.
type Grouper interface{}

// TagGrouper represents a Grouper of type TagGrouper. This is used
// to specify grouping by tags.
type TagGrouper struct {
  Name string   `json:"name"`
  Tags []string `json:"tags"`
}

// NewTagGrouper returns a TagGrouper given a list of tags.
func NewTagGrouper(tags []string) *TagGrouper {
  return &TagGrouper{Name: "tag", Tags: tags}
}

// TimeGrouper represents a TimeGrouper. This is used to group
// results on the basis of type.
type TimeGrouper struct {
  Name       string    `json:"name"`
  Range      RangeSize `json:"range_size"`
  GroupCount int       `json:"group_count"`
}

// RangeSize represents a Range of time e.g. 1 day, 2 weeks.
type RangeSize struct {
  Value int      `json:"value"`
  Unit  TimeUnit `json:"unit"`
}

// NewTimeGrouper returns a TimeGrouper.
func NewTimeGrouper(rangeValue int, rangeUnit TimeUnit,
  groupCount int) *TimeGrouper {
  return &TimeGrouper{
    Name: "time",
    Range: RangeSize{
      Value: rangeValue,
      Unit:  rangeUnit,
    },
    GroupCount: groupCount,
  }
}

// ValueGrouper represents a ValueGrouper type which is used to define
// grouping by value. For ex. group value between 0-9, 10-19 and so on.
type ValueGrouper struct {
  Name      string `json:"name"`
  RangeSize int    `json:"range_size"`
}

// NewValueGrouper returns a ValueGrouper given a range size.
func NewValueGrouper(rangeSize int) *ValueGrouper {
  return &ValueGrouper{Name: "value", RangeSize: rangeSize}
}

// Aggregator represents an Aggregator type.
type Aggregator struct {
  Name          string   `json:"name"`
  Sampling      Sampling `json:"sampling"`
  AlignSampling bool     `json:"align_sampling"`
}

// Sampling represents a sampling interval to be used by Aggegator.
type Sampling struct {
  Value int64    `json:"value"`
  Unit  TimeUnit `json:"unit"`
}

// NewAggregator returns an Aggregator given name, value and unit. Name
// represents type of aggrgator like "avg", "sum", "min" etc.
func NewAggregator(name string, value int64, unit TimeUnit) (
  ag *Aggregator, err error) {

  if value <= 0 {
    err = fmt.Errorf("kairosdb: negative value passed, val: %d", value)
    return
  }

  if name == "" {
    err = fmt.Errorf("kairosdb: aggregator name can not be empty")
    return
  }

  ag = &Aggregator{
    Name:     name,
    Sampling: Sampling{Value: value, Unit: unit},
  }
  return
}
