package parser

import "0michalsokolowski0/binlog-parser/internal/parser/conversion"

type RowsEventBuffer struct {
	buffered []conversion.RowsEventData
}

func NewRowsEventBuffer() RowsEventBuffer {
	return RowsEventBuffer{}
}

func (mb *RowsEventBuffer) BufferRowsEventData(d conversion.RowsEventData) {
	mb.buffered = append(mb.buffered, d)
}

func (mb *RowsEventBuffer) Drain() []conversion.RowsEventData {
	ret := mb.buffered
	mb.buffered = nil

	return ret
}
