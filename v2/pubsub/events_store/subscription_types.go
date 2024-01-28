package events_store

type StartAtType int32

const (
	StartAtTypeUndefined StartAtType = iota
	StartAtTypeFromNew
	StartAtTypeFromFirst
	StartAtTypeFromLast
	StartAtTypeFromSequence
	StartAtTypeFromTime
)
