// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

import (
	"bytes"
	"fmt"
	"log"
	"sync"
)

var (
	logIdMutex sync.Mutex
	logId      int
)

func newLogId() int {
	logIdMutex.Lock()
	defer logIdMutex.Unlock()
	logId += 1
	return logId
}

// NewLoggingConn returns logging wrapper around a connection.
func NewLoggingConn(conn Conn) Conn {
	return loggingConn{conn, newLogId()}
}

type loggingConn struct {
	Conn
	id int
}

func (c loggingConn) Close() error {
	err := c.Conn.Close()
	log.Printf("%d.Close() (err: %v)", c.id, err)
	return err
}

func (c loggingConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) error {
	err := c.Conn.Update(namespace, selector, update, options)
	var buf bytes.Buffer
	if options != nil {
		if options.Upsert {
			buf.WriteString(", upsert=true")
		}
		if options.Multi {
			buf.WriteString(", multi=true")
		}
	}
	log.Printf("%d.Update(%+v, %+v, %+v%s) (%v)", c.id, namespace, selector, update, buf.String(), err)
	return err
}

func (c loggingConn) Insert(namespace string, options *InsertOptions, documents ...interface{}) error {
	err := c.Conn.Insert(namespace, options, documents...)
	var buf bytes.Buffer
	if options != nil {
		if options.ContinueOnError {
			buf.WriteString(", continue=true")
		}
	}
	log.Printf("%d.Insert(%s%s, %+v) (%v)", c.id, namespace, buf.String(), documents, err)
	return err
}

func (c loggingConn) Remove(namespace string, selector interface{}, options *RemoveOptions) error {
	err := c.Conn.Remove(namespace, selector, options)
	var buf bytes.Buffer
	if options != nil {
		if options.Single {
			buf.WriteString(", single=true")
		}
	}
	log.Printf("%d.Remove(%s, %+v%s) (%v)", c.id, namespace, selector, buf.String(), err)
	return err
}

func (c loggingConn) Find(namespace string, query interface{}, options *FindOptions) (Cursor, error) {
	r, err := c.Conn.Find(namespace, query, options)
	var id int
	if r != nil {
		id = newLogId()
		r = logCursor{r, id}
	}
	var buf bytes.Buffer
	if options != nil {
		if options.Fields != nil {
			buf.WriteString(", fields:")
			fmt.Fprintf(&buf, "%+v", options.Fields)
		}
		if options.Tailable {
			buf.WriteString(", tailable:true")
		}
		if options.SlaveOk {
			buf.WriteString(", slaveOK:true")
		}
		if options.NoCursorTimeout {
			buf.WriteString(", noCursorTimeout:true")
		}
		if options.AwaitData {
			buf.WriteString(", awaitData:true")
		}
		if options.Exhaust {
			buf.WriteString(", exhaust:true")
		}
		if options.PartialResults {
			buf.WriteString(", partialResults:true")
		}
		if options.Skip != 0 {
			fmt.Fprintf(&buf, ", skip:%d", options.Skip)
		}
		if options.Limit != 0 {
			fmt.Fprintf(&buf, ", limit:%d", options.Limit)
		}
		if options.BatchSize != 0 {
			fmt.Fprintf(&buf, ", batchSize:%d", options.BatchSize)
		}
	}
	log.Printf("%d.Find(%s, %+v%s) (%d, %v)", c.id, namespace, query, buf.String(), id, err)
	return r, err
}

type logCursor struct {
	Cursor
	id int
}

func (r logCursor) Close() error {
	err := r.Cursor.Close()
	log.Printf("%d.Close() (%v)", r.id, err)
	return err
}

func (r logCursor) Next(value interface{}) error {
	var bd BSONData
	err := r.Cursor.Next(&bd)
	var m M
	if err == nil {
		err = Decode(bd.Data, value)
		bd.Decode(&m)
	}
	log.Printf("%d.Next() (%v, %v)", r.id, m, err)
	return err
}
