package aiven

import (
	"encoding/json"
	"errors"
	"fmt"
)

type (
	ServiceTopic struct {
		CleanupPolicy         string       `json:"cleanup_policy"`
		MinimumInSyncReplicas string       `json:"min_insync_replicas"`
		Partitions            []*Partition `json:"partitions"`
		Replication           int          `json:"replication"`
		RetentionBytes        int          `json:"retention_bytes"`
		RetentionHours        int          `json:"retention_hours"`
		State                 string       `json:"state"`
		TopicName             string       `json:"topic_name"`
	}

	Partition struct {
		ConsumerGroups []*ConsumerGroup `json:"consumer_groups"`
		EarliestOffset int              `json:"earliest_offset"`
		ISR            int              `json:"isr"`
		LatestOffset   int              `json:"latest_offset"`
		Partition      int              `json:"partition"`
		Size           int              `json:"size"`
	}

	ConsumerGroup struct {
		GroupName string `json:"group_name"`
		Offset    int    `json:"offset"`
	}

	ServiceTopicsHandler struct {
		client *Client
	}

	CreateServiceTopicRequest struct {
		CleanupPolicy         string `json:"cleanup_policy"`
		MinimumInSyncReplicas string `json:"min_insync_replicas"`
		Partitions            int    `json:"partitions"`
		Replication           int    `json:"replication"`
		RetentionBytes        int    `json:"retention_bytes"`
		RetentionHours        int    `json:"retention_hours"`
		TopicName             string `json:"topic_name"`
	}

	UpdateServiceTopicRequest struct {
		MinimumInSyncReplicas string `json:"min_insync_replicas"`
		Partitions            int    `json:"partitions"`
		RetentionBytes        int    `json:"retention_bytes"`
		RetentionHours        int    `json:"retention_hours"`
	}

	ServiceTopicResponse struct {
		APIResponse
		Topic *ServiceTopic `json:"topic"`
	}
)

func (h *ServiceTopicsHandler) Create(project, service string, req CreateServiceUserRequest) error {
	bts, err := h.client.doPostRequest(fmt.Sprintf("/project/%s/service/%s/topic", project, service), req)
	if err != nil {
		return err
	}

	var rsp *APIResponse
	if err := json.Unmarshal(bts, &rsp); err != nil {
		return err
	}

	if rsp == nil {
		return ErrNoResponseData
	}

	if rsp.Errors != nil && len(rsp.Errors) != 0 {
		return errors.New(rsp.Message)
	}

	return nil
}

func (h *ServiceTopicsHandler) Get(project, service, topic string) (*ServiceTopic, error) {
	rsp, err := h.client.doGetRequest(fmt.Sprintf("/project/%s/service/%s/topic/%s", project, service, topic), nil)
	if err != nil {
		return nil, err
	}

	var response *ServiceTopicResponse
	if err := json.Unmarshal(rsp, &response); err != nil {
		return nil, err
	}

	if len(response.Errors) != 0 {
		return nil, errors.New(response.Message)
	}

	return response.Topic, nil
}

func (h *ServiceTopicsHandler) Update(project, service, topic string, req UpdateServiceTopicRequest) error {
	bts, err := h.client.doPutRequest(fmt.Sprintf("/project/%s/service/%s/topic/%s", project, service, topic), req)
	if err != nil {
		return err
	}

	var rsp *APIResponse
	if err := json.Unmarshal(bts, &rsp); err != nil {
		return err
	}

	if rsp == nil {
		return ErrNoResponseData
	}

	if rsp.Errors != nil && len(rsp.Errors) != 0 {
		return errors.New(rsp.Message)
	}

	return nil
}

func (h *ServiceTopicsHandler) Delete(project, service, topic string) error {
	bts, err := h.client.doDeleteRequest(fmt.Sprintf("/project/%s/service/%s/topic/%s", project, service, topic), nil)
	if err != nil {
		return err
	}

	return handleDeleteResponse(bts)
}
