package common

import (
	"fmt"
)

type QueuesStats struct {
	Messages int `json:"messages"`
	Volume   int `json:"volume"`
	Waiting  int `json:"waiting"`
	Expired  int `json:"expired"`
	Delayed  int `json:"delayed"`
}

func (qs *QueuesStats) String() string {
	return fmt.Sprintf("Stats: messages=%d, volume=%d, waiting=%d, expired=%d, delayed=%d", qs.Messages, qs.Volume, qs.Waiting, qs.Expired, qs.Delayed)
}

type QueuesChannel struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	LastActivity int         `json:"lastActivity"`
	IsActive     bool        `json:"isActive"`
	Incoming     QueuesStats `json:"incoming"`
	Outgoing     QueuesStats `json:"outgoing"`
}

func (qc *QueuesChannel) String() string {
	return fmt.Sprintf("Channel: name=%s, type=%s, last_activity=%d, is_active=%t, incoming=%v, outgoing=%v", qc.Name, qc.Type, qc.LastActivity, qc.IsActive, qc.Incoming, qc.Outgoing)
}

type PubSubStats struct {
	Messages int `json:"messages"`
	Volume   int `json:"volume"`
}

func (ps *PubSubStats) String() string {
	return fmt.Sprintf("Stats: messages=%d, volume=%d", ps.Messages, ps.Volume)
}

type PubSubChannel struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	LastActivity int         `json:"lastActivity"`
	IsActive     bool        `json:"isActive"`
	Incoming     PubSubStats `json:"incoming"`
	Outgoing     PubSubStats `json:"outgoing"`
}

func (pc *PubSubChannel) String() string {
	return fmt.Sprintf("Channel: name=%s, type=%s, last_activity=%d, is_active=%t, incoming=%v, outgoing=%v", pc.Name, pc.Type, pc.LastActivity, pc.IsActive, pc.Incoming, pc.Outgoing)
}

type CQStats struct {
	Messages  int `json:"messages"`
	Volume    int `json:"volume"`
	Responses int `json:"responses"`
}

func (cs *CQStats) String() string {
	return fmt.Sprintf("Stats: messages=%d, volume=%d, responses=%d", cs.Messages, cs.Volume, cs.Responses)
}

type CQChannel struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	LastActivity int     `json:"lastActivity"`
	IsActive     bool    `json:"isActive"`
	Incoming     CQStats `json:"incoming"`
	Outgoing     CQStats `json:"outgoing"`
}

func (cc *CQChannel) String() string {
	return fmt.Sprintf("Channel: name=%s, type=%s, last_activity=%d, is_active=%t, incoming=%v, outgoing=%v", cc.Name, cc.Type, cc.LastActivity, cc.IsActive, cc.Incoming, cc.Outgoing)
}
