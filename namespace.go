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

	cnilibrary "github.com/containernetworking/cni/libcni"
	current "github.com/containernetworking/cni/pkg/types/100"
)

type network struct {
	cni    cnilibrary.CNI
	config *cnilibrary.NetworkConfigList
	ifName string
}

var _ Network = (*network)(nil)

func newNetwork(cni cnilibrary.CNI, conflist *cnilibrary.NetworkConfigList, ifName string) *network {
	return &network{
		cni:    cni,
		config: conflist,
		ifName: ifName,
	}
}

func (n *network) Name() string {
	return n.config.Name
}

func (n *network) Config() *ConfNetwork {
	conf := &NetworkConfList{
		Name:       n.config.Name,
		CNIVersion: n.config.CNIVersion,
		Source:     string(n.config.Bytes),
	}
	for _, plugin := range n.config.Plugins {
		conf.Plugins = append(conf.Plugins, &NetworkConf{
			Network: plugin.Network,
			Source:  string(plugin.Bytes),
		})
	}
	return &ConfNetwork{
		Config: conf,
		IFName: n.ifName,
	}
}

func (n *network) Attach(ctx context.Context, id string, path string, opts ...NamespaceOpts) (Attachment, error) {
	ns, err := newNamespace(id, path, opts...)
	if err != nil {
		return nil, err
	}
	rt := ns.config(n.ifName)
	cr, err := n.attach(ctx, ns)
	if err != nil {
		return nil, err
	}
	return newAttachment(&NetworkAttachment{
		ContainerID:    rt.ContainerID,
		Network:        n.Name(),
		IfName:         rt.IfName,
		Config:         n.config.Bytes,
		NetNS:          rt.NetNS,
		CniArgs:        rt.Args,
		CapabilityArgs: rt.CapabilityArgs,
	}, n.cni, cr), nil
}

func (n *network) attach(ctx context.Context, ns *Namespace) (*current.Result, error) {
	rt := ns.config(n.ifName)
	r, err := n.cni.AddNetworkList(ctx, n.config, rt)
	if err != nil {
		return nil, err
	}
	cr, err := current.NewResultFromResult(r)
	if err != nil {
		return nil, err
	}
	return cr, nil
}

func (n *network) detach(ctx context.Context, ns *Namespace) error {
	return n.cni.DelNetworkList(ctx, n.config, ns.config(n.ifName))
}

func (n *network) check(ctx context.Context, ns *Namespace) error {
	return n.cni.CheckNetworkList(ctx, n.config, ns.config(n.ifName))
}

// TODO the following will be in the upcoming CNI release
type NetworkAttachment struct {
	ContainerID    string
	Network        string
	IfName         string
	Config         []byte
	NetNS          string
	CniArgs        [][2]string
	CapabilityArgs map[string]interface{}
}

type attachment struct {
	NetworkAttachment
	cni    cnilibrary.CNI
	result *current.Result
}

var _ Attachment = (*attachment)(nil)

func newAttachment(info *NetworkAttachment, cni cnilibrary.CNI, result *current.Result) *attachment {
	return &attachment{
		NetworkAttachment: *info,
		cni:               cni,
		result:            result,
	}
}

func (a *attachment) Network() string {
	return a.NetworkAttachment.Network
}

func (a *attachment) Container() string {
	return a.NetworkAttachment.ContainerID
}

func (a *attachment) IfName() string {
	return a.NetworkAttachment.IfName
}

func (a *attachment) NetNS() string {
	return a.NetworkAttachment.NetNS
}

func (a *attachment) Remove(ctx context.Context) error {
	rt := &cnilibrary.RuntimeConf{
		ContainerID:    a.Container(),
		NetNS:          a.NetNS(),
		IfName:         a.IfName(),
		Args:           a.CniArgs,
		CapabilityArgs: a.CapabilityArgs,
	}
	conflist, err := cnilibrary.ConfListFromBytes(a.Config)
	if err != nil {
		return err
	}
	return a.cni.DelNetworkList(ctx, conflist, rt)
}

func (a *attachment) Check(ctx context.Context) error {
	rt := &cnilibrary.RuntimeConf{
		ContainerID:    a.Container(),
		NetNS:          a.NetNS(),
		IfName:         a.IfName(),
		Args:           a.CniArgs,
		CapabilityArgs: a.CapabilityArgs,
	}
	conflist, err := cnilibrary.ConfListFromBytes(a.Config)
	if err != nil {
		return err
	}
	return a.cni.CheckNetworkList(ctx, conflist, rt)
}

type Namespace struct {
	id             string
	path           string
	capabilityArgs map[string]interface{}
	args           map[string]string
}

func newNamespace(id, path string, opts ...NamespaceOpts) (*Namespace, error) {
	ns := &Namespace{
		id:             id,
		path:           path,
		capabilityArgs: make(map[string]interface{}),
		args:           make(map[string]string),
	}
	for _, o := range opts {
		if err := o(ns); err != nil {
			return nil, err
		}
	}
	return ns, nil
}

func (ns *Namespace) config(ifName string) *cnilibrary.RuntimeConf {
	c := &cnilibrary.RuntimeConf{
		ContainerID: ns.id,
		NetNS:       ns.path,
		IfName:      ifName,
	}
	for k, v := range ns.args {
		c.Args = append(c.Args, [2]string{k, v})
	}
	c.CapabilityArgs = ns.capabilityArgs
	return c
}
