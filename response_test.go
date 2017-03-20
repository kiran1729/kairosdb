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

  "github.com/stretchr/testify/assert"
)

// Test to validate Query Response deserialization.
func TestDeserializeQueryResponse(t *testing.T) {
  var payload = `
{
  "queries":[
    {
      "sample_size":10,
      "results":[{
        "name":"kairos-db-test-counter",
        "group_by":[
        {
          "name":"type",
          "type":"number"
        }
        ],
        "tags":{"tag1":["val1"]},
        "values":[
        [1418790504000,10],
        [1418790507000,10],
        [1418790509000,10],
        [1418811092000,10],
        [1418811936000,10],
        [1418811948000,10],
        [1418813113000,10],
        [1418814692000,10],
        [1418831275000,10],
        [1418836136000,10]
        ]
      }]
      }
  ]
}
`
  var qr QueryResponse
  err := json.Unmarshal([]byte(payload), &qr)
  assert.Nil(t, err)
  assert.True(t, len(qr.Queries) == 1)
  assert.Equal(t, qr.Queries[0].SampleSize, 10)
  assert.Equal(t, qr.Queries[0].Results[0].Values[0].Value.(float64),
    float64(10))
}
