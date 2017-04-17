package parser

import (
	"strings"
	"zalora/binlog-parser/parser/messages"
	"zalora/binlog-parser/parser/conversion"
	"zalora/binlog-parser/parser/database"
	"github.com/siddontang/go-mysql/replication"
	"github.com/golang/glog"
)

func ParseBinlogToMessages(binlogFileName string, tableMap database.TableMap, consumer func(messages.Message)) error {
	rowRowsEventBuffer := NewRowsEventBuffer()

	p := replication.NewBinlogParser()

	f := func(e *replication.BinlogEvent) error {
		switch e.Header.EventType {
		case replication.QUERY_EVENT:
			queryEvent := e.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)

			if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
				glog.Info("Starting transaction")
			} else if strings.HasPrefix(strings.ToUpper(strings.Trim(query, " ")), "SAVEPOINT") {
				glog.Info("Skipping transaction savepoint")
			} else {
				glog.Info("Query event")
				consumer(conversion.ConvertQueryEventToMessage(*e.Header, *queryEvent))
			}

			break

		case replication.XID_EVENT:
			xidEvent := e.Event.(*replication.XIDEvent)
			xId := uint64(xidEvent.XID)

			glog.Infof("Ending transaction xID %d", xId)

			for _, message := range conversion.ConvertRowsEventsToMessages(xId, rowRowsEventBuffer.Drain()) {
				consumer(message)
			}

			break

		case replication.TABLE_MAP_EVENT:
			tableMapEvent := e.Event.(*replication.TableMapEvent)

			schema := string(tableMapEvent.Schema)
			table := string(tableMapEvent.Table)
			tableId := uint64(tableMapEvent.TableID)

			tableMap.Add(tableId, schema, table)

			break

		case replication.WRITE_ROWS_EVENTv2:
			fallthrough
		case replication.UPDATE_ROWS_EVENTv2:
			fallthrough
 		case replication.DELETE_ROWS_EVENTv2:
			rowsEvent := e.Event.(*replication.RowsEvent)

			tableId := uint64(rowsEvent.TableID)
			tableMetadata, ok := tableMap.LookupTableMetadata(tableId)

			if ok == false {
				glog.Errorf("Skipping event - no table found for table id %D", tableId)
				break
			}

			rowRowsEventBuffer.BufferRowsEventData(
				conversion.RowsEventData{
					BinlogEventHeader: *e.Header,
					BinlogEvent: *rowsEvent,
					TableMetadata: tableMetadata,
				},
			)

			break

		default:
			break
		}

		return nil
	}

	return p.ParseFile(binlogFileName, 0, f)
}
