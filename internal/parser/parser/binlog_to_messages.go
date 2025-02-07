package parser

import (
	"0michalsokolowski0/binlog-parser/internal/database"
	"0michalsokolowski0/binlog-parser/internal/parser/conversion"
	"0michalsokolowski0/binlog-parser/internal/parser/messages"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

type ConsumerFunc func(messages.Message) error

func ParseBinlogToMessages(binlogFilename string, tableMap database.TableMap, consumer ConsumerFunc) error {
	rowRowsEventBuffer := NewRowsEventBuffer()

	p := replication.NewBinlogParser()

	f := func(e *replication.BinlogEvent) error {
		switch e.Header.EventType {
		case replication.QUERY_EVENT:
			queryEvent := e.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)

			if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
				logrus.Info("Starting transaction")
			} else if strings.HasPrefix(strings.ToUpper(strings.Trim(query, " ")), "SAVEPOINT") {
				logrus.Info("Skipping transaction savepoint")
			} else {
				logrus.Info("Query event")

				err := consumer(conversion.ConvertQueryEventToMessage(*e.Header, *queryEvent))

				if err != nil {
					return err
				}
			}

			break

		case replication.XID_EVENT:
			xidEvent := e.Event.(*replication.XIDEvent)
			xId := uint64(xidEvent.XID)

			logrus.Infof("Ending transaction xID %d", xId)

			for _, message := range conversion.ConvertRowsEventsToMessages(xId, rowRowsEventBuffer.Drain()) {
				err := consumer(message)

				if err != nil {
					return err
				}
			}

			break

		case replication.TABLE_MAP_EVENT:
			tableMapEvent := e.Event.(*replication.TableMapEvent)

			schema := string(tableMapEvent.Schema)
			table := string(tableMapEvent.Table)
			tableId := uint64(tableMapEvent.TableID)

			err := tableMap.Add(tableId, schema, table)

			if err != nil {
				logrus.Errorf("Failed to add table information for table %s.%s (id %d)", schema, table, tableId)
				return err
			}

			break

		case replication.WRITE_ROWS_EVENTv1,
			replication.UPDATE_ROWS_EVENTv1,
			replication.DELETE_ROWS_EVENTv1,
			replication.WRITE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv2:
			rowsEvent := e.Event.(*replication.RowsEvent)

			tableId := uint64(rowsEvent.TableID)
			tableMetadata, ok := tableMap.LookupTableMetadata(tableId)

			if !ok {
				logrus.Errorf("Skipping event - no table found for table id %d", tableId)
				break
			}

			rowRowsEventBuffer.BufferRowsEventData(
				conversion.NewRowsEventData(*e.Header, *rowsEvent, tableMetadata),
			)

			break

		default:
			break
		}

		return nil
	}

	return p.ParseFile(binlogFilename, 0, f)
}
