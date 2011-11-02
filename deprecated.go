// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mongo

// No more data in cursor.
var EOF = Done

// Doc is deprecated. Use D instead.
type Doc []DocItem

func (d *Doc) Append(name string, value interface{}) {
	*d = append(*d, DocItem{name, value})
}

// FindOne is deprecated. Use Collection{conn, namespace}.Find(query).One(result) instead.
func FindOne(conn Conn, namespace string, query interface{}, options *FindOptions, result interface{}) error {
	q := Collection{Conn: conn, Namespace: namespace}.Find(query)
	if options != nil {
		q.Options = *options
	}
	return q.One(result)
}

// RunCommand is deprecated. Use Database{conn, dbname}.Run(cmd, result) instead.
func RunCommand(conn Conn, namespace string, cmd Doc, result interface{}) error {
	dbname, _ := SplitNamespace(namespace)
	return Database{Conn: conn, Name: dbname}.Run(cmd, result)
}

// LastError is deprecated. Use Database{Conn: conn, Name: dbname}.LastError(cmd) instead.
func LastError(conn Conn, namespace string, cmd interface{}) error {
	dbname, _ := SplitNamespace(namespace)
	_, err := Database{Conn: conn, Name: dbname}.LastError(cmd)
	return err
}

// commandNamespace returns the command namespace give a database name or
// namespace.
func commandNamespace(namespace string) string {
	name, _ := SplitNamespace(namespace)
	return name + ".$cmd"
}

// SafeInsert is deprecated.
func SafeInsert(conn Conn, namespace string, errorCmd interface{}, documents ...interface{}) error {
	return SafeConn{conn, errorCmd}.Insert(namespace, documents...)
}

// SafeUpdate is deprecated.
func SafeUpdate(conn Conn, namespace string, errorCmd interface{}, selector, update interface{}, options *UpdateOptions) error {
	return SafeConn{conn, errorCmd}.Update(namespace, selector, update, options)
}

// SafeRemove is deprecated.
func SafeRemove(conn Conn, namespace string, errorCmd interface{}, selector interface{}, options *RemoveOptions) error {
	return SafeConn{conn, errorCmd}.Remove(namespace, selector, options)
}

// SafeConn is deprecated.
type SafeConn struct {
	// The connecion to wrap.
	Conn

	// The command document used to fetch the last error. If cmd is nil, then
	// the command {"getLastError": 1} is used as the command.
	Cmd interface{}
}

func (c SafeConn) checkError(namespace string, err error) error {
	if err != nil {
		return err
	}
	dbname, _ := SplitNamespace(namespace)
	_, err = Database{Conn: c.Conn, Name: dbname}.LastError(c.Cmd)
	return err
}

func (c SafeConn) Update(namespace string, selector, update interface{}, options *UpdateOptions) error {
	return c.checkError(namespace, c.Conn.Update(namespace, selector, update, options))
}

func (c SafeConn) Insert(namespace string, documents ...interface{}) error {
	return c.checkError(namespace, c.Conn.Insert(namespace, nil, documents...))
}

func (c SafeConn) Remove(namespace string, selector interface{}, options *RemoveOptions) error {
	return c.checkError(namespace, c.Conn.Remove(namespace, selector, options))
}

// Count is deprected. Use Collection{Conn: conn, Namespace:namespace}.Find(query).Count() instead.
func Count(conn Conn, namespace string, query interface{}, options *FindOptions) (int64, error) {
	q := Collection{Conn: conn, Namespace: namespace}.Find(query)
	if options != nil {
		q.Options = *options
	}
	return q.Count()
}

// FindAndUpdate is deprecated. Use the Collection FindAndUpdate method instead.
func FindAndUpdate(conn Conn, namespace string, selector, update interface{}, options *FindAndModifyOptions, result interface{}) error {
	_, name := SplitNamespace(namespace)
	return findAndModify(
		conn,
		namespace,
		Doc{
			{"findAndModify", name},
			{"query", selector},
			{"update", update}},
		options,
		result)
}

// FindAndRemove is deprecated. Use the Collection FindAndRemove method instead.
func FindAndRemove(conn Conn, namespace string, selector interface{}, options *FindAndModifyOptions, result interface{}) error {
	_, name := SplitNamespace(namespace)
	return findAndModify(
		conn,
		namespace,
		Doc{
			{"findAndModify", name},
			{"query", selector},
			{"remove", true}},
		options,
		result)
}

func findAndModify(conn Conn, namespace string, cmd Doc, options *FindAndModifyOptions, result interface{}) error {
	if options != nil {
		if options.New {
			cmd.Append("new", true)
		}
		if options.Fields != nil {
			cmd.Append("fields", options.Fields)
		}
		if options.Upsert {
			cmd.Append("upsert", true)
		}
		if options.Sort != nil {
			cmd.Append("sort", options.Sort)
		}
	}
	var r struct {
		CommandResponse
		Value BSONData `bson:"value"`
	}
	if err := FindOne(conn, commandNamespace(namespace), cmd, nil, &r); err != nil {
		return err
	}
	if err := r.Error(); err != nil {
		return err
	}
	return r.Value.Decode(result)
}

// FindAndModifyOptions specifies options for the FindAndUpdate and FindAndRemove methods.
// DEPRECATED. 
type FindAndModifyOptions struct {
	// Set to true if you want to return the modified object rather than the
	// original. Ignored for remove.
	New bool `bson:"new/c"`

	// Specify subset of fields to return.
	Fields interface{} `bson:"fields"`

	// Create object if it doesn't exist. Ignored for remove.
	Upsert bool `bson:"upsert/c"`

	// If multiple docs match, choose the first one in the specified sort order
	// as the object to update. 
	Sort interface{} `bson:"sort"`
}

// FindAndUpdate updates and returns a document specified by selector with
// operator update. FindAndUpdate is a wrapper around the MongoDB findAndModify
// command.
//
// DEPRECATED. Use Query.Update or Query.Upsert instead.
func (c Collection) FindAndUpdate(selector, update interface{}, options *FindAndModifyOptions, result interface{}) error {
	_, name := SplitNamespace(c.Namespace)
	cmd := struct {
		Collection string      `bson:"findAndModify"`
		Selector   interface{} `bson:"query"`
		Update     interface{} `bson:"update"`
		FindAndModifyOptions
	}{
		Collection: name,
		Selector:   selector,
		Update:     update,
	}
	if options != nil {
		cmd.FindAndModifyOptions = *options
	}
	return c.findAndModify(&cmd, result)
}

// FindAndRemove removes and returns a document specified by selector.
// FindAndRemove is a wrapper around the MongoDB findAndModify command.
//
// DEPRECATED. Use Query.Remove instead.
func (c Collection) FindAndRemove(selector interface{}, options *FindAndModifyOptions, result interface{}) error {
	_, name := SplitNamespace(c.Namespace)
	cmd := struct {
		Collection string      `bson:"findAndModify"`
		Selector   interface{} `bson:"query"`
		Remove     bool        `bson:"remove"`
		FindAndModifyOptions
	}{
		Collection: name,
		Selector:   selector,
		Remove:     true,
	}
	if options != nil {
		cmd.FindAndModifyOptions = *options
	}
	return c.findAndModify(&cmd, result)
}

func (c Collection) findAndModify(cmd interface{}, result interface{}) error {
	dbname, _ := SplitNamespace(c.Namespace)
	cursor, err := c.Conn.Find(dbname+".$cmd", cmd, runFindOptions)
	if err != nil {
		return err
	}
	defer cursor.Close()
	var r struct {
		CommandResponse
		Value BSONData `bson:"value"`
	}
	if err := cursor.Next(&r); err != nil {
		return err
	}
	if err := r.Error(); err != nil {
		return err
	}
	return r.Value.Decode(result)
}
