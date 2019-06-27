package kubemq

import (
	"time"

	"go.opencensus.io/trace"
)

type Trace struct {
	Name       string
	attributes []trace.Attribute
	annotation trace.Annotation
}

func CreateTrace(name string) *Trace {
	t := &Trace{
		Name:       name,
		attributes: nil,
		annotation: trace.Annotation{},
	}
	return t
}

func (t *Trace) AddStringAttribute(key string, value string) *Trace {
	t.attributes = append(t.attributes, trace.StringAttribute(key, value))
	return t
}
func (t *Trace) AddInt64Attribute(key string, value int64) *Trace {
	t.attributes = append(t.attributes, trace.Int64Attribute(key, value))
	return t
}
func (t *Trace) AddBoolAttribute(key string, value bool) *Trace {
	t.attributes = append(t.attributes, trace.BoolAttribute(key, value))
	return t
}

func (t *Trace) AddAnnotation(timestamp time.Time, message string) *Trace {
	t.annotation = trace.Annotation{
		Time:       timestamp,
		Message:    message,
		Attributes: nil,
	}
	return t
}
