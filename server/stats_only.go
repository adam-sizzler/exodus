package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"exodus-node/proto"
)

const statsOnlyMessage = "stats-only node: user management and task APIs are disabled for sing-box"

const (
	taskOperationDeployConfig       = "deploy_config"
	taskOperationNodePluginExecutor = "node_plugin_executor"
)

func (s *NodeServer) ListUsers(ctx context.Context, req *proto.ListUsersRequest) (*proto.ListUsersResponse, error) {
	_ = ctx
	_ = req
	return nil, status.Error(codes.Unimplemented, statsOnlyMessage)
}

func (s *NodeServer) AddUsers(ctx context.Context, req *proto.AddUsersRequest) (*proto.OperationResponse, error) {
	if req == nil {
		return &proto.OperationResponse{
			Status: &rpcstatus.Status{Code: int32(codes.InvalidArgument), Message: "request is nil"},
		}, nil
	}
	items := make([]SyncUserItem, 0, len(req.Usernames))
	for _, username := range req.Usernames {
		tag := req.InboundTag
		var tags []string
		if tag != "" {
			tags = []string{tag}
		}
		items = append(items, SyncUserItem{
			Action:      "add",
			Identifier:  username,
			Username:    username,
			InboundTags: tags,
		})
	}
	payloadBytes, _ := json.Marshal(SyncUsersTaskPayload{Users: items})
	st, err := s.HandleSyncUsers(ctx, "add_users", payloadBytes)
	if err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Status:    st,
		Usernames: req.Usernames,
	}, nil
}

func (s *NodeServer) DeleteUsers(ctx context.Context, req *proto.DeleteUsersRequest) (*proto.OperationResponse, error) {
	if req == nil {
		return &proto.OperationResponse{
			Status: &rpcstatus.Status{Code: int32(codes.InvalidArgument), Message: "request is nil"},
		}, nil
	}
	items := make([]SyncUserItem, 0, len(req.Usernames))
	for _, username := range req.Usernames {
		tag := req.InboundTag
		var tags []string
		if tag != "" {
			tags = []string{tag}
		}
		items = append(items, SyncUserItem{
			Action:      "delete",
			Identifier:  username,
			Username:    username,
			InboundTags: tags,
		})
	}
	payloadBytes, _ := json.Marshal(SyncUsersTaskPayload{Users: items})
	st, err := s.HandleSyncUsers(ctx, "delete_users", payloadBytes)
	if err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Status:    st,
		Usernames: req.Usernames,
	}, nil
}

func (s *NodeServer) SetUserEnabled(ctx context.Context, req *proto.SetUserEnabledRequest) (*proto.OperationResponse, error) {
	if req == nil {
		return &proto.OperationResponse{
			Status: &rpcstatus.Status{Code: int32(codes.InvalidArgument), Message: "request is nil"},
		}, nil
	}
	action := "add"
	if !req.Enabled {
		action = "disable"
	}
	items := make([]SyncUserItem, 0, len(req.Usernames))
	for _, username := range req.Usernames {
		items = append(items, SyncUserItem{
			Action:     action,
			Identifier: username,
			Username:   username,
		})
	}
	payloadBytes, _ := json.Marshal(SyncUsersTaskPayload{Users: items})
	st, err := s.HandleSyncUsers(ctx, action, payloadBytes)
	if err != nil {
		return nil, err
	}
	return &proto.OperationResponse{
		Status:    st,
		Usernames: req.Usernames,
	}, nil
}

func (s *NodeServer) SubmitTask(ctx context.Context, task *proto.NodeTask) (*rpcstatus.Status, error) {
	_ = ctx
	if task == nil {
		return &rpcstatus.Status{
			Code:    int32(codes.InvalidArgument),
			Message: "task is nil",
		}, nil
	}
	s.Cfg.LoggerFor("NodeService").Debug("SubmitTask received", "task_id", task.TaskId, "operation", task.Operation, "payload_bytes", len(task.Payload))

	taskPayload, err := decompressGzipIfNeeded(task.Payload)
	if err != nil {
		s.Cfg.LoggerFor("NodeService").Warn("Failed to decompress task payload", "task_id", task.TaskId, "error", err)
		return &rpcstatus.Status{
			Code:    int32(codes.InvalidArgument),
			Message: fmt.Sprintf("decompress task payload: %v", err),
		}, nil
	}

	switch task.Operation {
	case taskOperationDeployConfig:
		var payload DeployConfigTaskPayload
		if err := json.Unmarshal(taskPayload, &payload); err != nil {
			s.Cfg.LoggerFor("SingboxService").Warn("Invalid deploy_config payload", "task_id", task.TaskId, "error", err)
			return &rpcstatus.Status{
				Code:    int32(codes.InvalidArgument),
				Message: fmt.Sprintf("invalid deploy_config payload: %v", err),
			}, nil
		}
		if len(payload.Config) == 0 && len(payload.SingboxConfig) == 0 {
			payload.Config = append(json.RawMessage(nil), task.Payload...)
		}

		summary, err := s.DeployConfig(ctx, payload)
		if err != nil {
			s.Cfg.LoggerFor("SingboxService").Error("Failed to start Sing-box: " + err.Error())
			return &rpcstatus.Status{
				Code:    int32(codes.FailedPrecondition),
				Message: err.Error(),
			}, nil
		}

		message := fmt.Sprintf(
			"success: config_path=%s listen=%s inbounds=%d outbounds=%d users=%d restarted=%t force_restart=%t config_changed=%t haproxy_users_changed=%t core_ready=%t core_process_before=%s core_process_after=%s",
			summary.ConfigPath,
			summary.Listen,
			summary.Inbounds,
			summary.Outbounds,
			summary.Users,
			summary.Restarted,
			summary.ForceRestart,
			summary.ConfigChanged,
			summary.HaproxyUsersChanged,
			summary.CoreReady,
			summary.CoreProcessBefore,
			summary.CoreProcessAfter,
		)
		if summary.ReloadError != "" {
			message += fmt.Sprintf(" reload_error=%q", summary.ReloadError)
		}

		return &rpcstatus.Status{
			Code:    int32(codes.OK),
			Message: message,
		}, nil
	case taskOperationNodePluginExecutor:
		accepted, err := ExecuteNodePluginCommand(task.Payload)
		if err != nil {
			s.Cfg.LoggerFor("PluginService").Warn("Node plugin executor command failed", "task_id", task.TaskId, "error", err)
			return &rpcstatus.Status{
				Code:    int32(codes.FailedPrecondition),
				Message: err.Error(),
			}, nil
		}
		return &rpcstatus.Status{
			Code:    int32(codes.OK),
			Message: fmt.Sprintf("success: accepted=%t", accepted),
		}, nil

	case taskOperationGeocheck:
		outputJSON, err := ExecuteGeocheck(ctx, taskPayload)
		if err != nil {
			s.Cfg.LoggerFor("GeocheckService").Warn("Geocheck failed", "task_id", task.TaskId, "error", err)
			return &rpcstatus.Status{
				Code:    int32(codes.Internal),
				Message: err.Error(),
			}, nil
		}
		return &rpcstatus.Status{
			Code:    int32(codes.OK),
			Message: outputJSON,
		}, nil

	case "sync_users", "add_users", "delete_users":
		return s.HandleSyncUsers(ctx, task.Operation, taskPayload)

	default:
		s.Cfg.LoggerFor("NodeService").Warn("Unsupported task operation", "task_id", task.TaskId, "operation", task.Operation)
		return &rpcstatus.Status{
			Code:    int32(codes.Unimplemented),
			Message: fmt.Sprintf("%s (operation=%s)", statsOnlyMessage, task.Operation),
		}, nil
	}
}

func (s *NodeServer) GetTaskStatus(ctx context.Context, req *proto.TaskStatusRequest) (*proto.TaskStatusResponse, error) {
	_ = ctx
	taskID := ""
	if req != nil {
		taskID = req.TaskId
	}
	return &proto.TaskStatusResponse{
		TaskId:       taskID,
		Status:       "unsupported",
		ErrorMessage: statsOnlyMessage,
	}, nil
}

var gzipReaderPool = sync.Pool{}

func decompressGzipIfNeeded(payload []byte) ([]byte, error) {
	if len(payload) >= 2 && payload[0] == 0x1f && payload[1] == 0x8b {
		reader := bytes.NewReader(payload)
		var gr *gzip.Reader
		if v := gzipReaderPool.Get(); v != nil {
			gr = v.(*gzip.Reader)
			if err := gr.Reset(reader); err != nil {
				gzipReaderPool.Put(gr)
				return nil, err
			}
		} else {
			var err error
			gr, err = gzip.NewReader(reader)
			if err != nil {
				return nil, err
			}
		}
		defer gzipReaderPool.Put(gr)
		return io.ReadAll(gr)
	}
	return payload, nil
}
