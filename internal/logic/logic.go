package logic

import (
	"github.com/arashbina/streaming-consumer/internal/storage"
	"github.com/arashbina/streaming-consumer/internal/stream"
)

type Logic struct {
	store  storage.Storage
	stream *stream.Client
}

func New(store storage.Storage, stream *stream.Client) *Logic {

	l := Logic{
		store:  store,
		stream: stream,
	}

	return &l
}

func (l *Logic) Consume() {

	l.stream.Consume()
}
