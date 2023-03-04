package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"gopkg.in/guregu/null.v4"
	"net/http"
	"strconv"
	"strings"
)

type DBHandler struct {
	DB          *sql.DB                                 //db connection
	tables      map[string]bool                         //map of table names
	tableFields map[string]map[string]map[string]string //map of tables with field parameters
}

func NewDBHandler(db *sql.DB) DBHandler {
	return DBHandler{DB: db}
}

// InitTables set map of table names
func (db *DBHandler) InitTables() {
	db.tables = make(map[string]bool)
	rows, err := db.DB.Query("SHOW TABLES")

	if err != nil {
		fmt.Println("Reading tables error: ", err)
	}

	defer rows.Close()

	tableName := ""

	for rows.Next() {
		err = rows.Scan(&tableName)

		if err != nil {
			fmt.Println("Reading tables error: ", err)
		}

		db.tables[tableName] = true
	}
}

// InitTableFields  set map of table fields with parameters
func (db *DBHandler) InitTableFields() {

	db.tableFields = make(map[string]map[string]map[string]string)

	for tableName, _ := range db.tables {

		rows, err := db.DB.Query("show columns from " + tableName)

		if err != nil {
			fmt.Println("Reading table fields error: ", err)
		}

		db.tableFields[tableName] = make(map[string]map[string]string)

		Field, Type, Null, Key, Default, Extra := null.String{}, null.String{}, null.String{}, null.String{}, null.String{}, null.String{}

		for rows.Next() {
			err = rows.Scan(&Field, &Type, &Null, &Key, &Default, &Extra)

			if err != nil {
				fmt.Println("Reading table fields error: ", err)
			}

			db.tableFields[tableName][Field.String] = make(map[string]string)
			db.tableFields[tableName][Field.String]["Type"] = Type.String
			db.tableFields[tableName][Field.String]["Null"] = Null.String
			db.tableFields[tableName][Field.String]["Key"] = Key.String
			db.tableFields[tableName][Field.String]["Default"] = Default.String
			db.tableFields[tableName][Field.String]["Extra"] = Extra.String
		}
		rows.Close()
	}

}

// DoesTableExist check for existing table
func (db *DBHandler) DoesTableExist(tableName string) (ok bool) {
	ok = db.tables[tableName]
	return
}

func TypeSwitch(typeName string) (v any) {
	switch typeName {
	case "INT":
		v = new(int)
	case "TEXT":
		v = new(null.String)
	case "VARCHAR":
		v = new(null.String)
	default:
		v = new(interface{})
	}
	return
}

func DefaultValueByType(typeName string) (v any) {
	switch {
	case typeName == "int":
		v = 0
	case typeName == "text" || typeName == "char" || strings.Contains(typeName, "varchar"):
		v = ""
	default:
		v = new(interface{})
	}
	return
}

func (db *DBHandler) SearchPrimaryKey(tableName string) (string, bool) {
	for field, _ := range db.tableFields[tableName] {
		if db.tableFields[tableName][field]["Key"] == "PRI" {
			return field, true
		}
	}
	return "", false
}

// ShowAllTables return json with list of tables
func (db *DBHandler) ShowAllTables(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	tables := make([]string, 0, len(db.tables))

	for key, _ := range db.tables {
		tables = append(tables, key)
	}

	body, err := json.Marshal(tables)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out := bytes.Buffer{}

	out.Write([]byte("{\"response\":{\"tables\":"))
	out.Write(body)
	out.Write([]byte("}}\n"))

	_, err = w.Write(out.Bytes())

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func TypeMatch(typeName string, placeholder any, nullPermisson bool) bool {
	switch {
	case typeName == "int":
		if _, ok := placeholder.(int); !ok {
			return false
		}
	case typeName == "text" || typeName == "char" || strings.Contains(typeName, "varchar"):
		if placeholder == nil && nullPermisson {
			return false
		} else if placeholder != nil {
			if _, ok := placeholder.(string); !ok {
				return false
			}
		}
	default:
		return false
	}
	return true
}

// ShowTablesWithParams returns json records based on parameters
func (db *DBHandler) ShowTablesWithParams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	tableName := vars["table"]

	if !db.DoesTableExist(tableName) {
		http.Error(w, "{\"error\":\"unknown table\"}", http.StatusNotFound)
		return
	}

	//check sql injection
	limit, offset := r.FormValue("limit"), r.FormValue("offset")
	if _, err := strconv.Atoi(limit); err != nil {
		limit = "5" //def value
	}
	if _, err := strconv.Atoi(offset); err != nil {
		offset = "0" //def value
	}

	sqlQuery := fmt.Sprintf("SELECT * FROM %s LIMIT  %s OFFSET %s", tableName, limit, offset)

	rows, err := db.DB.Query(sqlQuery)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer rows.Close()

	var objects []map[string]interface{}

	for rows.Next() {
		columns, _ := rows.ColumnTypes()

		values := make([]interface{}, len(columns))
		object := map[string]interface{}{}
		for i, column := range columns {
			v := TypeSwitch(column.DatabaseTypeName())
			object[column.Name()] = v
			values[i] = v
		}

		rows.Scan(values...)

		objects = append(objects, object)
	}

	body, _ := json.Marshal(objects)

	out := bytes.Buffer{}

	out.Write([]byte("{\"response\":{\"records\":"))
	out.Write(body)
	out.Write([]byte("}}\n"))

	_, err = w.Write(out.Bytes())

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ShowLine return record by primary key
func (db *DBHandler) ShowLine(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	tableName, id := vars["table"], vars["id"]

	if !db.DoesTableExist(tableName) {
		http.Error(w, "{\"error\":\"unknown table\"}", http.StatusNotFound)
		return
	}

	sqlQuery := ""

	keyName, ok := db.SearchPrimaryKey(tableName)

	if !ok {
		http.Error(w, "{\"error\":\"not found primary key\"}", http.StatusNotFound)
		return
	}

	sqlQuery = fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableName, keyName)

	rows, err := db.DB.Query(sqlQuery, id)
	if err != nil {
		fmt.Println(err.Error())
	}

	defer rows.Close()

	object := map[string]interface{}{}

	if rows.Next() {
		columns, _ := rows.ColumnTypes()

		values := make([]interface{}, len(columns))
		for i, column := range columns {
			v := TypeSwitch(column.DatabaseTypeName())
			object[column.Name()] = v
			values[i] = v
		}

		err = rows.Scan(values...)

		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		body, _ := json.Marshal(object)

		out := bytes.Buffer{}

		out.Write([]byte("{\"response\":{\"record\":"))
		out.Write(body)
		out.Write([]byte("}}\n"))

		_, err = w.Write(out.Bytes())

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	} else {
		http.Error(w, "{\"error\":\"record not found\"}", http.StatusNotFound)
		return
	}
}

func (db *DBHandler) PutNewLine(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	tableName := vars["table"]

	if !db.DoesTableExist(tableName) {
		http.Error(w, "{\"error\":\"unknown table\"}", http.StatusNotFound)
		return
	}

	var objects map[string]interface{}

	json.NewDecoder(r.Body).Decode(&objects)

	placeholders := make([]interface{}, 0)
	fields, qmark := "", ""

	for field, _ := range db.tableFields[tableName] {

		//check if field auto_increment
		if db.tableFields[tableName][field]["Extra"] == "auto_increment" {
			continue
		}

		if _, ok := objects[field]; !ok && db.tableFields[tableName][field]["Null"] == "NO" {
			fmt.Println(field)
			objects[field] = DefaultValueByType(db.tableFields[tableName][field]["Type"])
			fmt.Println(objects[field])
		} else if db.tableFields[tableName][field]["Null"] == "YES" {
			continue
		}

		fields += field + ","
		qmark += " ?,"
		placeholders = append(placeholders, objects[field])
	}

	fields = fields[:len(fields)-1]
	qmark = qmark[:len(qmark)-1]

	sqlExec := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, fields, qmark)

	fmt.Println(sqlExec)

	result, err := db.DB.Exec(sqlExec, placeholders...)

	if err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	lastID, err := result.LastInsertId()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	keyName, _ := db.SearchPrimaryKey(tableName)

	out := fmt.Sprintf("{\"response\":{\"%s\":%s}}\n", keyName, strconv.Itoa(int(lastID)))

	_, err = w.Write([]byte(out))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (db *DBHandler) EditLine(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName, id := vars["table"], vars["id"]

	keyName, ok := db.SearchPrimaryKey(tableName)

	if !ok {
		http.Error(w, "{\"error\":\"not found primary key\"}", http.StatusNotFound)
		return
	}

	if !db.DoesTableExist(tableName) {
		http.Error(w, "{\"error\":\"unknown table\"}", http.StatusNotFound)
		return
	}

	var objects map[string]interface{}

	json.NewDecoder(r.Body).Decode(&objects)

	sqlExec := fmt.Sprintf("UPDATE %s SET", tableName)
	placeholders := make([]interface{}, 0)

	for field, placeholder := range objects {

		//check for primary key
		if field == keyName {
			http.Error(w, fmt.Sprintf("{\"error\":\"field %s have invalid type\"}", keyName), http.StatusBadRequest)
			return
		}

		//check for existing field
		if _, ok := db.tableFields[tableName][field]; !ok {
			continue
		}

		//check for type match
		typeName := db.tableFields[tableName][field]["Type"]

		if TypeMatch(typeName, placeholder, db.tableFields[tableName][field]["Null"] == "NO") {
			sqlExec += " " + field + " = ?,"
			placeholders = append(placeholders, placeholder)
		} else {
			http.Error(w, fmt.Sprintf("{\"error\":\"field %s have invalid type\"}", field), http.StatusBadRequest)
			return
		}
	}

	sqlExec = sqlExec[:len(sqlExec)-1]
	sqlExec += fmt.Sprintf(" WHERE %s = ?", keyName)
	placeholders = append(placeholders, id)

	result, err := db.DB.Exec(
		sqlExec,
		placeholders...,
	)

	if err != nil {
		fmt.Println(err.Error())
	}

	count, _ := result.RowsAffected()

	out := fmt.Sprintf("{\"response\":{\"updated\":%s}}\n", strconv.Itoa(int(count)))

	_, err = w.Write([]byte(out))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (db *DBHandler) DeleteLine(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	tableName, id := vars["table"], vars["id"]

	if !db.DoesTableExist(tableName) {
		http.Error(w, "{\"error\":\"unknown table\"}", http.StatusNotFound)
		return
	}

	sqlExec := fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName)

	result, err := db.DB.Exec(sqlExec, id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	count, _ := result.RowsAffected()

	out := fmt.Sprintf("{\"response\":{\"deleted\":%s}}\n", strconv.Itoa(int(count)))

	_, err = w.Write([]byte(out))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {

	dbh := NewDBHandler(db)
	dbh.InitTables()
	dbh.InitTableFields()

	r := mux.NewRouter()
	r.HandleFunc("/", dbh.ShowAllTables).Methods("GET")
	r.HandleFunc("/{table}", dbh.ShowTablesWithParams).Methods("GET")
	r.HandleFunc("/{table}/{id}", dbh.ShowLine).Methods("GET")
	r.HandleFunc("/{table}/", dbh.PutNewLine).Methods("PUT")
	r.HandleFunc("/{table}/{id}", dbh.EditLine).Methods("POST")
	r.HandleFunc("/{table}/{id}", dbh.DeleteLine).Methods("DELETE")

	return http.Handler(r), nil
}
