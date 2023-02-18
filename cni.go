/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package cni

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	cnilibrary "github.com/containernetworking/cni/libcni"
	"github.com/containernetworking/cni/pkg/invoke"
	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/containernetworking/cni/pkg/version"
)

type CNI interface {
	// Setup setup the network for the namespace
	Setup(ctx context.Context, id string, path string, opts ...NamespaceOpts) (*Result, error)
	// SetupSerially sets up each of the network interfaces for the namespace in serial
	SetupSerially(ctx context.Context, id string, path string, opts ...NamespaceOpts) (*Result, error)
	// Remove tears down the network of the namespace.
	Remove(ctx context.Context, id string, path string, opts ...NamespaceOpts) error
	// Check checks if the network is still in desired state
	Check(ctx context.Context, id string, path string, opts ...NamespaceOpts) error
	// Load loads the cni network config
	Load(opts ...Opt) error
	// Status checks the status of the cni initialization
	Status() error
	// GetConfig returns a copy of the CNI plugin configurations as parsed by CNI
	GetConfig() *ConfigResult
	// CreateNetwork creates and adds a network config
	CreateNetwork(cfg []byte, ifName string) (Network, error)
	// DeleteNetwork deletes a network config
	DeleteNetwork(name string) error
	// Network returns a network config identified by name
	GetNetwork(name string) (Network, error)
	// ListNetworks returns all networks
	ListNetworks() []Network
	// ListAttachments returns attachments for the specified container
	// In case of empty id, all attachments will be returned
	ListAttachments(id string) []Attachment
	// GetResult returns the cached result of namespace setups
	// This routine can be used to retrieve the updated result
	// after networks are dynamically attached/detached
	GetResult(id string) (*Result, error)
}

// Network defines the interfaces of a Network definition.
type Network interface {
	// Name returns the name of the network
	Name() string
	// Config returns the network config
	Config() *ConfNetwork
	// Attach the network to a container
	Attach(ctx context.Context, id string, path string, opts ...NamespaceOpts) (Attachment, error)
}

// Attachment is the operation of applying a network configuration to a container
type Attachment interface {
	// Network returns the name of the network that this attachment is created for
	Network() string
	// Container returns the id of the container
	Container() string
	// IfName returns the interface name within the container
	IfName() string
	// NetNS returns the path of network namespace
	NetNS() string
	// Remove the attachment
	Remove(ctx context.Context) error
	// Check whether the current status of the attachment is as expected
	Check(ctx context.Context) error
}

type ConfigResult struct {
	PluginDirs       []string
	PluginConfDir    string
	PluginMaxConfNum int
	Prefix           string
	Networks         []*ConfNetwork
}

type ConfNetwork struct {
	Config *NetworkConfList
	IFName string
}

// NetworkConfList is a source bytes to string version of cnilibrary.NetworkConfigList
type NetworkConfList struct {
	Name       string
	CNIVersion string
	Plugins    []*NetworkConf
	Source     string
}

// NetworkConf is a source bytes to string conversion of cnilibrary.NetworkConfig
type NetworkConf struct {
	Network *types.NetConf
	Source  string
}

type libcni struct {
	config

	cniConfig    cnilibrary.CNI
	networkCount int // minimum network plugin configurations needed to initialize cni
	networks     []*network
	sync.RWMutex
}

func defaultCNIConfig() *libcni {
	return &libcni{
		config: config{
			pluginDirs:       []string{DefaultCNIDir},
			pluginConfDir:    DefaultNetDir,
			pluginMaxConfNum: DefaultMaxConfNum,
			prefix:           DefaultPrefix,
		},
		cniConfig: cnilibrary.NewCNIConfig(
			[]string{
				DefaultCNIDir,
			},
			&invoke.DefaultExec{
				RawExec:       &invoke.RawExec{Stderr: os.Stderr},
				PluginDecoder: version.PluginDecoder{},
			},
		),
		networkCount: 1,
	}
}

// New creates a new libcni instance.
func New(config ...Opt) (CNI, error) {
	cni := defaultCNIConfig()
	var err error
	for _, c := range config {
		if err = c(cni); err != nil {
			return nil, err
		}
	}
	return cni, nil
}

// Load loads the latest config from cni config files.
func (c *libcni) Load(opts ...Opt) error {
	var err error
	c.Lock()
	defer c.Unlock()
	// Reset the networks on a load operation to ensure
	// config happens on a clean slate
	c.reset()

	for _, o := range opts {
		if err = o(c); err != nil {
			return fmt.Errorf("cni config load failed: %v: %w", err, ErrLoad)
		}
	}
	return nil
}

// Status returns the status of CNI initialization.
func (c *libcni) Status() error {
	c.RLock()
	defer c.RUnlock()
	if len(c.networks) < c.networkCount {
		return ErrCNINotInitialized
	}
	return nil
}

// Networks returns all the configured networks.
// NOTE: Caller MUST NOT modify anything in the returned array.
func (c *libcni) ListNetworks() []Network {
	c.RLock()
	defer c.RUnlock()
	var rs []Network
	for _, n := range c.networks {
		rs = append(rs, n)
	}
	return rs
}

// Setup setups the network in the namespace and returns a Result
func (c *libcni) Setup(ctx context.Context, id string, path string, opts ...NamespaceOpts) (*Result, error) {
	if err := c.Status(); err != nil {
		return nil, err
	}
	ns, err := newNamespace(id, path, opts...)
	if err != nil {
		return nil, err
	}
	result, err := c.attachNetworks(ctx, ns)
	if err != nil {
		return nil, err
	}
	return c.createResult(result)
}

// SetupSerially setups the network in the namespace and returns a Result
func (c *libcni) SetupSerially(ctx context.Context, id string, path string, opts ...NamespaceOpts) (*Result, error) {
	if err := c.Status(); err != nil {
		return nil, err
	}
	ns, err := newNamespace(id, path, opts...)
	if err != nil {
		return nil, err
	}
	result, err := c.attachNetworksSerially(ctx, ns)
	if err != nil {
		return nil, err
	}
	return c.createResult(result)
}

func (c *libcni) attachNetworksSerially(ctx context.Context, ns *Namespace) ([]*current.Result, error) {
	var results []*current.Result
	for _, net := range c.ListNetworks() {
		network := net.(*network)
		r, err := network.attach(ctx, ns)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
}

type asynchAttachResult struct {
	index int
	res   *current.Result
	err   error
}

func asynchAttach(ctx context.Context, index int, n *network, ns *Namespace, wg *sync.WaitGroup, rc chan asynchAttachResult) {
	defer wg.Done()
	r, err := n.attach(ctx, ns)
	rc <- asynchAttachResult{index: index, res: r, err: err}
}

func (c *libcni) attachNetworks(ctx context.Context, ns *Namespace) ([]*current.Result, error) {
	var wg sync.WaitGroup
	var firstError error
	networks := c.ListNetworks()
	results := make([]*current.Result, len(networks))
	rc := make(chan asynchAttachResult)

	for i, net := range networks {
		network := net.(*network)
		wg.Add(1)
		go asynchAttach(ctx, i, network, ns, &wg, rc)
	}

	for range networks {
		rs := <-rc
		if rs.err != nil && firstError == nil {
			firstError = rs.err
		}
		results[rs.index] = rs.res
	}
	wg.Wait()

	return results, firstError
}

// Remove removes the network config from the namespace
func (c *libcni) Remove(ctx context.Context, id string, path string, opts ...NamespaceOpts) error {
	if err := c.Status(); err != nil {
		return err
	}
	ns, err := newNamespace(id, path, opts...)
	if err != nil {
		return err
	}
	// TODO Consider using cached attachments because network list may be updated already
	for _, net := range c.ListNetworks() {
		network := net.(*network)
		if err := network.detach(ctx, ns); err != nil {
			// Based on CNI spec v0.7.0, empty network namespace is allowed to
			// do best effort cleanup. However, it is not handled consistently
			// right now:
			// https://github.com/containernetworking/plugins/issues/210
			// TODO(random-liu): Remove the error handling when the issue is
			// fixed and the CNI spec v0.6.0 support is deprecated.
			// NOTE(claudiub): Some CNIs could return a "not found" error, which could mean that
			// it was already deleted.
			if (path == "" && strings.Contains(err.Error(), "no such file or directory")) || strings.Contains(err.Error(), "not found") {
				continue
			}
			return err
		}
	}
	return nil
}

// Check checks if the network is still in desired state
func (c *libcni) Check(ctx context.Context, id string, path string, opts ...NamespaceOpts) error {
	if err := c.Status(); err != nil {
		return err
	}
	ns, err := newNamespace(id, path, opts...)
	if err != nil {
		return err
	}
	// TODO Consider using cached attachments because network list may be updated already
	for _, net := range c.ListNetworks() {
		network := net.(*network)
		err := network.check(ctx, ns)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetConfig returns a copy of the CNI plugin configurations as parsed by CNI
func (c *libcni) GetConfig() *ConfigResult {
	c.RLock()
	defer c.RUnlock()
	r := &ConfigResult{
		PluginDirs:       c.config.pluginDirs,
		PluginConfDir:    c.config.pluginConfDir,
		PluginMaxConfNum: c.config.pluginMaxConfNum,
		Prefix:           c.config.prefix,
	}
	for _, network := range c.networks {
		r.Networks = append(r.Networks, network.Config())
	}
	return r
}

func (c *libcni) reset() {
	c.networks = nil
}

func (c *libcni) CreateNetwork(cfg []byte, ifName string) (Network, error) {
	conflist, err := cnilibrary.ConfListFromBytes(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse conflist, %w", ErrInvalidConfig)
	}
	c.Lock()
	defer c.Unlock()
	for _, n := range c.networks {
		if n.Name() == conflist.Name {
			return nil, fmt.Errorf("network %q already exists, %w", n.Name(), ErrAlreadyExists)
		}
		if n.ifName == ifName {
			return nil, fmt.Errorf("network %q already has interface %q, %w", n.Name(), ifName, ErrAlreadyExists)
		}
	}
	n := newNetwork(c.cniConfig, conflist, ifName)
	c.networks = append(c.networks, n)
	return n, nil
}

func (c *libcni) DeleteNetwork(name string) error {
	c.Lock()
	defer c.Unlock()
	i := len(c.networks)
	for k, n := range c.networks {
		if n.Name() == name {
			i = k
			break
		}
	}
	if i == len(c.networks) {
		return fmt.Errorf("network %q does not exist, %w", name, ErrNotFound)
	}
	c.networks = append(c.networks[:i], c.networks[i+1:]...)
	return nil
}

func (c *libcni) GetNetwork(name string) (Network, error) {
	c.RLock()
	defer c.RUnlock()
	for _, n := range c.networks {
		if n.Name() == name {
			return n, nil
		}
	}
	return nil, fmt.Errorf("network %q does not exist, %w", name, ErrNotFound)
}

func (c *libcni) ListAttachments(id string) []Attachment {
	var rs []Attachment
	list, err := getCachedAttachments(id)
	if err != nil {
		return rs
	}
	for _, i := range list {
		rs = append(rs, newAttachment(i, c.cniConfig, nil))
	}
	return rs
}

func (c *libcni) GetResult(id string) (*Result, error) {
	list, err := getCachedAttachments(id)
	if err != nil {
		return nil, err
	}
	var rs []*current.Result
	for _, i := range list {
		cfg, err := cnilibrary.ConfListFromBytes(i.Config)
		if err != nil {
			return nil, err
		}
		rt := &cnilibrary.RuntimeConf{
			ContainerID:    i.ContainerID,
			NetNS:          i.NetNS,
			IfName:         i.IfName,
			Args:           i.CniArgs,
			CapabilityArgs: i.CapabilityArgs,
		}
		r, err := c.cniConfig.GetNetworkListCachedResult(cfg, rt)
		if err != nil {
			return nil, err
		}
		cr, err := current.NewResultFromResult(r)
		if err != nil {
			return nil, err
		}
		rs = append(rs, cr)
	}
	return c.createResult(rs)
}

// The following will be added to CNI
func getCachedAttachments(containerID string) ([]*NetworkAttachment, error) {
	dirPath := filepath.Join(cnilibrary.CacheDir, "results")
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 0, len(entries))
	for _, e := range entries {
		fileNames = append(fileNames, e.Name())
	}
	sort.Strings(fileNames)

	attachments := []*NetworkAttachment{}
	for _, fname := range fileNames {
		if len(containerID) > 0 {
			part := fmt.Sprintf("-%s-", containerID)
			pos := strings.Index(fname, part)
			if pos <= 0 || pos+len(part) >= len(fname) {
				continue
			}
		}

		cacheFile := filepath.Join(dirPath, fname)
		bytes, err := os.ReadFile(cacheFile)
		if err != nil {
			continue
		}

		cachedInfo := struct {
			Kind           string                 `json:"kind"`
			Config         []byte                 `json:"config"`
			IfName         string                 `json:"ifName"`
			ContainerID    string                 `json:"containerID"`
			NetworkName    string                 `json:"networkName"`
			NetNS          string                 `json:"netns,omitempty"`
			CniArgs        [][2]string            `json:"cniArgs,omitempty"`
			CapabilityArgs map[string]interface{} `json:"capabilityArgs,omitempty"`
		}{}

		if err := json.Unmarshal(bytes, &cachedInfo); err != nil {
			continue
		}
		if cachedInfo.Kind != cnilibrary.CNICacheV1 {
			continue
		}
		if len(containerID) > 0 && cachedInfo.ContainerID != containerID {
			continue
		}
		if cachedInfo.IfName == "" || cachedInfo.NetworkName == "" {
			continue
		}

		attachments = append(attachments, &NetworkAttachment{
			ContainerID:    cachedInfo.ContainerID,
			Network:        cachedInfo.NetworkName,
			IfName:         cachedInfo.IfName,
			Config:         cachedInfo.Config,
			NetNS:          cachedInfo.NetNS,
			CniArgs:        cachedInfo.CniArgs,
			CapabilityArgs: cachedInfo.CapabilityArgs,
		})
	}
	return attachments, nil
}
