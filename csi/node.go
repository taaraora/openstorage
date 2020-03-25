/*
Package csi is CSI driver interface for OSD
Copyright 2017 Portworx

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
package csi

import (
	"fmt"
	"os"
	"strings"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/util"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mountutil "k8s.io/kubernetes/pkg/util/mount"
)

func (s *OsdCsiServer) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest,
) (*csi.NodeGetInfoResponse, error) {

	clus, err := s.cluster.Enumerate()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unable to Enumerate cluster: %s", err)
	}

	result := &csi.NodeGetInfoResponse{
		NodeId: clus.NodeId,
	}

	return result, nil
}

// NodePublishVolume is a CSI API call which mounts the volume on the specified
// target path on the node.
//
// TODO: Support READ ONLY Mounts
//
func (s *OsdCsiServer) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
) (*csi.NodePublishVolumeResponse, error) {

	logrus.Debugf("NodePublishVolume req[%#v]", req)

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}
	if req.GetVolumeCapability() == nil || req.GetVolumeCapability().GetAccessMode() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume access mode must be provided")
	}

	// Get grpc connection
	conn, err := s.getConn()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Unable to connect to SDK server: %v", err)
	}

	// Get secret if any was passed
	ctx = s.setupContextWithToken(ctx, req.GetSecrets())

	// Check if block device
	driverType := s.driver.Type()
	if driverType != api.DriverType_DRIVER_TYPE_BLOCK &&
		req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Trying to attach as block a non block device")
	}

	// Gather volume attributes
	spec, _, _, err := s.specHandler.SpecFromOpts(req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Invalid volume attributes: %#v",
			req.GetVolumeContext())
	}

	// Get volume encryption info from req.Secrets
	driverOpts := s.addEncryptionInfoToLabels(make(map[string]string), req.GetSecrets())

	// prepare for mount/attaching
	mounts := api.NewOpenStorageMountAttachClient(conn)
	opts := &api.SdkVolumeAttachOptions{
		SecretName: spec.GetPassphrase(),
	}
	 var attachResp *api.SdkVolumeAttachResponse
	if driverType == api.DriverType_DRIVER_TYPE_BLOCK {
		if attachResp, err = mounts.Attach(ctx, &api.SdkVolumeAttachRequest{
			VolumeId:      req.GetVolumeId(),
			Options:       opts,
			DriverOptions: driverOpts,
		}); err != nil {
			return nil, err
		}
	}

	targetPath := req.GetTargetPath()
	isBlockAccessType := false
	if req.GetVolumeCapability().GetBlock() != nil {
		isBlockAccessType = true
	}

	// ensureTargetLocation verifies target location and creates the one if it doesn't exist
	if err = ensureTargetLocation(targetPath, isBlockAccessType); err != nil {
		return nil, status.Errorf(
			codes.Aborted,
			"Failed to ensure target location %s: %s",
			targetPath,
			err.Error())
	}

	logrus.Debugf("NodePublishVolume is block[%v]", isBlockAccessType)
	//check that it is not mounted
	// TODO: should we put this as a new method in OpenStorageMountAttach service?
	mounter := mountutil.New("")
	if notMount, _ := mounter.IsNotMountPoint(targetPath); !notMount {
		logrus.Debugf("NodePublishVolume notMount[%v]", notMount)
		mountedDeviceName, err := mounter.GetDeviceNameFromMount(targetPath, "")
		if err != nil {
			return nil, status.Errorf(
				codes.Aborted,
				"Failed to mount target location %s: %s",
				targetPath,
				"failed to get device name for target location which is already mounted")
		}
		logrus.Debugf("NodePublishVolume mountedDeviceName[%v]", mountedDeviceName)

		if strings.Contains(mountedDeviceName, req.VolumeId){
			logrus.Debugf("NodePublishVolume is already mounted[%v]", req.VolumeId)
			return &csi.NodePublishVolumeResponse{}, nil
		}
		return nil, status.Errorf(
			codes.Aborted,
			"Failed to mount target location %s: %s",
			targetPath,
			fmt.Sprintf("target location is already mounted with device %s", mountedDeviceName))
	}

	//TODO: check attachResp is nil
	if isBlockAccessType {
		if err = mounter.Mount(attachResp.DevicePath, targetPath, "", []string{"bind"}); err != nil {
			return nil, status.Errorf(
				codes.Aborted,
				"Failed to mount target location %s: %s",
				targetPath,
				fmt.Sprintf("failed to mount device %s", attachResp.DevicePath))
		}

		return &csi.NodePublishVolumeResponse{}, nil
	}

	// for volumes with mount access type just mount volume onto the path
	if _, err := mounts.Mount(ctx, &api.SdkVolumeMountRequest{
		VolumeId:  req.GetVolumeId(),
		MountPath: targetPath,
		Options:   opts,
	}); err != nil {
		return nil, err
	}

	logrus.Infof("Volume %s mounted on %s",
		req.GetVolumeId(),
		targetPath)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume is a CSI API call which unmounts the volume.
func (s *OsdCsiServer) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {

	logrus.Debugf("NodeUnPublishVolume req[%#v]", req)

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume id must be provided")
	}
	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path must be provided")
	}

	// Get volume information
	vol, err := util.VolumeFromName(s.driver, req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Volume id %s not found: %s",
			req.GetVolumeId(),
			err.Error())
	}

	if !vol.IsAttached() {
		logrus.Infof("Volume %s was already unmounted", req.GetVolumeId())
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Get information about the target since the request does not
	// tell us if it is for block or mount point.
	// https://github.com/container-storage-interface/spec/issues/285
	fileInfo, err := os.Lstat(req.GetTargetPath())
	if err != nil && os.IsNotExist(err) {
		// For idempotency, return that there is nothing to unmount
		logrus.Infof("NodeUnpublishVolume on target path %s but it does "+
			"not exist, returning there is nothing to do", req.GetTargetPath())
		return &csi.NodeUnpublishVolumeResponse{}, nil
	} else if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Unknown error while verifying target location %s: %s",
			req.GetTargetPath(),
			err.Error())
	}

	// Check if it is block or not
	if fileInfo.Mode()&os.ModeSymlink != 0 {
		// If block, we just need to remove the link.
		os.Remove(req.GetTargetPath())
	} else {
		if !fileInfo.IsDir() {
			return nil, status.Errorf(
				codes.NotFound,
				"Target location %s is not a directory", req.GetTargetPath())
		}

		// UnMount volume onto the path
		if err = s.driver.Unmount(req.GetVolumeId(), req.GetTargetPath(), nil); err != nil {
			logrus.Infof("Unable to unmount volume %s onto %s: %s",
				req.GetVolumeId(),
				req.GetTargetPath(),
				err.Error())
		}
	}

	if s.driver.Type() == api.DriverType_DRIVER_TYPE_BLOCK {
		if err = s.driver.Detach(req.GetVolumeId(), nil); err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Unable to detach volume: %s",
				err.Error())
		}
	}

	logrus.Infof("Volume %s unmounted", req.GetVolumeId())

	return &csi.NodeUnpublishVolumeResponse{}, nil
}


// NodeGetCapabilities is a CSI API function which seems to be setup for
// future patches
func (s *OsdCsiServer) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {

	logrus.Debugf("NodeGetCapabilities req[%#v]", req)

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_UNKNOWN,
					},
				},
			},
		},
	}, nil
}

func ensureTargetLocation(targetPath string, isBlock bool) error {
	_, err := os.Lstat(targetPath)
	if os.IsNotExist(err) {
		if isBlock {
			if err := makeFile(targetPath); err != nil {
				return fmt.Errorf("cannot create target location %s for block volume", targetPath)
			}
			return nil
		}

		if err := makeDir(targetPath); err != nil {
			return fmt.Errorf("cannot create target location %s for mount volume", targetPath)
		}

		return nil
	}

	return errors.Wrap(err, "unknown error while verifying target location")
}

func makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	defer f.Close()
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}
