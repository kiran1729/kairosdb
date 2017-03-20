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
  "bytes"
  "encoding/json"
  "fmt"
  "io"
  "io/ioutil"
  "net/http"
  "strings"
  "time"

  "github.com/golang/glog"
)

// Client interface defines the Kairosdb interface. Kairosdb has HTTP and Telnet
// endpoints to interact with.
type Client interface {

  // PushMetrics sends given metrics to KairosDB server.
  PushMetrics(metrics []*Metric, timeout time.Duration) error

  // Metrics returns list of metric names saved in DB.
  Metrics(timeout time.Duration) ([]string, error)

  // Delete deletes datapoints filtered by given query.
  Delete(query *Query, timeout time.Duration) error

  // Query sends a query to KairosDB server and retrieves the result.
  // TODO: replace interface{} with QueryResponse type.
  Query(query *Query, timeout time.Duration) (*QueryResponse, error)
}

// HTTPClient implements KairosDB Client interface using HTTP endpoints of
// KairosDB server.
type HTTPClient struct {
  backend Backend
  url     string
}

// NewHTTPClient returns an instance of HTTPClient.
func NewHTTPClient(url string, backend Backend) (*HTTPClient, error) {
  b := backend
  if b == nil {
    b = newDefaultBackend()
  }
  return &HTTPClient{url: url, backend: b}, nil
}

// PushMetrics sends given metrics to KairosDB server.
func (c *HTTPClient) PushMetrics(metrics []*Metric,
  timeout time.Duration) error {

  if len(metrics) == 0 {
    return fmt.Errorf("kairosdb: metrics missing")
  }
  payload, err := json.Marshal(metrics)
  if err != nil {
    return err
  }
  glog.V(4).Infof("pushing metric: %s", string(payload))
  reader := ioutil.NopCloser(bytes.NewReader(payload))
  err = c.backend.Call("POST", c.url+"/api/v1/datapoints", reader, timeout,
    http.StatusNoContent, nil)
  if err != nil {
    // TODO: client prints too many of these before system comes up.
    // Need to figure out a more intelligent suppression mechanism. Also, let
    // caller print this rather than print it here. Leaving Error for
    // non-connection errors to catch field name and tags issues.
    if strings.Contains(err.Error(), "connection refused") {
      glog.V(1).Infof("error connecting to KairosDB: %v", err)
    } else {
      glog.Errorf("error pushing metrics payload: %v :: %v", string(payload), err)
    }
    return err
  }
  return nil
}

// Query sends a query to KairosDB server and retrieves the result.
func (c *HTTPClient) Query(q *Query, timeout time.Duration) (
  res *QueryResponse, err error) {

  if q == nil {
    err = fmt.Errorf("kairosdb: nil query passed")
    return
  }
  payload, err := json.Marshal(q)
  if err != nil {
    return
  }

  res = &QueryResponse{}
  glog.V(3).Infof("querying metric: %s", string(payload))
  reader := ioutil.NopCloser(bytes.NewReader(payload))
  err = c.backend.Call("POST", c.url+"/api/v1/datapoints/query", reader,
    timeout, http.StatusOK, res)
  if err != nil {
    return
  }

  glog.V(3).Infof("response from query: %+v", res)
  return
}

// Delete deletes datapoints matching given query.
func (c *HTTPClient) Delete(q *Query, timeout time.Duration) error {
  if q == nil {
    return fmt.Errorf("kairosdb: nil query passed")
  }
  payload, err := json.Marshal(q)
  if err != nil {
    return fmt.Errorf("error unmarshalling query:%v :: %v", q, err)
  }

  glog.Infof("deleting datapoints payload: %v", string(payload))
  reader := ioutil.NopCloser(bytes.NewReader(payload))
  err = c.backend.Call("POST", c.url+"/api/v1/datapoints/delete", reader,
    timeout, http.StatusNoContent, nil)
  if err != nil {
    return fmt.Errorf("error deleting datapoints for query: %v :: %v",
      string(payload), err)
  }

  glog.Infof("datapoints deleted successfully for query: %+v", q)
  return nil
}

// Metrics returns list of metric names stored in DB.
func (c *HTTPClient) Metrics(timeout time.Duration) ([]string, error) {
  // temporary struct for parsing JSON response
  var respData struct {
    Names []string `json:"results"`
  }

  err := c.backend.Call("GET", c.url+"/api/v1/metricnames", nil,
    timeout, http.StatusOK, &respData)
  if err != nil {
    return nil, err
  }

  glog.V(3).Infof("metric names: %+v", respData.Names)
  return respData.Names, nil
}

// Backend interface defines an abstraction for invoking Kairosdb API
// over HTTP. A mock implementation of Backend will be used during
// the unit testing.
type Backend interface {
  Call(method string, url string, r io.Reader, timeout time.Duration,
    expStatus int, resp interface{}) error
}

// defaultBackend implements Backend interface for Kairosdb HTTP API.
type defaultBackend struct{}

func newDefaultBackend() *defaultBackend {
  return &defaultBackend{}
}

func (b *defaultBackend) Call(method string, url string, r io.Reader,
  timeout time.Duration, expStatus int, obj interface{}) error {

  req, err := b.newRequest(method, url, r)
  if err != nil {
    return err
  }

  cl := &http.Client{Timeout: timeout}
  res, err := cl.Do(req)
  if err != nil {
    return err
  }
  defer res.Body.Close()

  resBody, err := ioutil.ReadAll(res.Body)
  if err != nil {
    return err
  }

  if res.StatusCode != expStatus {
    return fmt.Errorf("kairosdb: status :%d does not match expected status: %d"+
      " with respBody: %v", res.StatusCode, expStatus, string(resBody))
  }

  if obj != nil {
    glog.V(2).Infof("response body: %s", string(resBody))
    return json.Unmarshal(resBody, obj)
  }
  return nil
}

func (b *defaultBackend) newRequest(method string, url string,
  r io.Reader) (*http.Request, error) {

  req, err := http.NewRequest(method, url, r)
  if err != nil {
    return nil, err
  }
  req.Header.Set("Content-Type", "application/json")
  return req, nil
}
