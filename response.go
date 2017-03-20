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

// QueryResponse represents result of Querying of Kairosdb.
type QueryResponse struct {
  Queries []QueryResult `json:"queries"`
}

// QueryResult represents result for one QueryMetric specified in Query.
type QueryResult struct {
  SampleSize int      `json:"sample_size"`
  Results    []Result `json:"results"`
}

// Result represent GroupResult of a Metric.
type Result struct {
  // TODO: Groupers is a slice of different type groupers
  // Still figuring out the best way to extract it.
  Groupers []interface{}       `json:"group_by"`
  Name     string              `json:"name"`
  Values   []DataPoint         `json:"values"`
  Tags     map[string][]string `json:"tags"`
}

// MergeFrom merges the results from "other" onto the current QueryResponse.
func (qr *QueryResponse) MergeFrom(other *QueryResponse) {
  qr.Queries = append(qr.Queries, other.Queries...)
}
