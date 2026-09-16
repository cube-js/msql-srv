//! This example implements a (simple) proxy that allows connecting to PostgreSQL as though it were
//! a MySQL database. To try this out, start a PostgreSQL database at localhost:5432, and then run
//! this example. Notice that `main` does *not* use PostgreSQL bindings, just MySQL ones!

extern crate msql_srv;
extern crate mysql;
extern crate postgres;
extern crate slab;

use msql_srv::*;
use mysql::prelude::*;
use postgres::types::{ToSql, Type};
use slab::Slab;

use std::io;
use std::net;
use std::thread;

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let client =
                postgres::Client::connect("postgresql://postgres@localhost:5432", postgres::NoTls)
                    .unwrap();
            MysqlIntermediary::run_on_tcp(Postgres::new(client), s).unwrap();
        }
    });

    // we connect using MySQL bindings, but no MySQL server is running!
    let mut db = mysql::Conn::new(format!("mysql://127.0.0.1:{}", port).as_str()).unwrap();
    assert!(db.ping().is_ok());
    {
        let mut results = db
            .query_iter("SELECT INT4(1) AS foo, INT8(1) AS bar, 'BAZ' AS baz")
            .unwrap();
        {
            let cols = results.columns();
            let cols = cols.as_ref();
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0].name_str(), "foo");
            assert_eq!(cols[1].name_str(), "bar");
            assert_eq!(cols[2].name_str(), "baz");
        }
        let one = results.next();
        assert!(one.is_some());
        if let Some(row) = one {
            let row = row.unwrap();
            assert_eq!(row.len(), 3);
            let one = row.get::<i32, _>(0).unwrap();
            assert_eq!(one, 1);
            let one = row.get::<i64, _>(1).unwrap();
            assert_eq!(one, 1);
            let s = row.get::<Vec<u8>, _>(2).unwrap();
            assert_eq!(s, b"BAZ");
        }
        assert_eq!(results.count(), 0);
    }
    {
        let mut results = db
            .exec_iter("SELECT INT4(1) AS foo, INT8(1) AS bar, 'BAZ' AS baz", ())
            .unwrap();
        {
            let cols = results.columns();
            let cols = cols.as_ref();
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0].name_str(), "foo");
            assert_eq!(cols[1].name_str(), "bar");
            assert_eq!(cols[2].name_str(), "baz");
        }
        let one = results.next();
        assert!(one.is_some());
        if let Some(row) = one {
            let row = row.unwrap();
            assert_eq!(row.len(), 3);
            let one = row.get::<i32, _>(0).unwrap();
            assert_eq!(one, 1);
            let one = row.get::<i64, _>(1).unwrap();
            assert_eq!(one, 1);
            let s = row.get::<Vec<u8>, _>(2).unwrap();
            assert_eq!(s, b"BAZ");
        }
        assert_eq!(results.count(), 0);
    }
    drop(db);
    jh.join().unwrap();
}

// this is where the proxy server implementation starts

/// `MysqlShim::Error` must be constructible from `io::Error`, which `postgres::Error` is not.
#[derive(Debug)]
enum Error {
    Io(io::Error),
    Postgres(postgres::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<postgres::Error> for Error {
    fn from(e: postgres::Error) -> Self {
        Error::Postgres(e)
    }
}

struct Prepared {
    stmt: postgres::Statement,
    params: Vec<Column>,
}

struct Postgres {
    client: postgres::Client,
    prepared: Slab<Prepared>,
}

impl Postgres {
    fn new(c: postgres::Client) -> Self {
        Postgres {
            client: c,
            prepared: Slab::new(),
        }
    }
}

impl<W: io::Write> MysqlShim<W> for Postgres {
    type Error = Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        match self.client.prepare(query) {
            Ok(stmt) => {
                // the PostgreSQL server will tell us about the parameter types and output columns
                // of the query we just prepared. we now need to communicate this back to our MySQL
                // client, which requires translating between psql and mysql types.
                let params: Vec<_> = stmt
                    .params()
                    .iter()
                    .map(|t| Column {
                        table: t.schema().to_owned(),
                        column: t.name().to_owned(),
                        coltype: p2mt(t),
                        colflags: ColumnFlags::empty(),
                    })
                    .collect();
                let columns: Vec<_> = stmt
                    .columns()
                    .iter()
                    .map(|c| {
                        let t = c.type_();
                        Column {
                            table: t.schema().to_owned(),
                            column: c.name().to_owned(),
                            coltype: p2mt(t),
                            colflags: ColumnFlags::empty(),
                        }
                    })
                    .collect();

                // keep track of the parameter types so we can decode the values provided by the
                // client when they later execute this statement.
                let id = self.prepared.insert(Prepared { stmt, params });
                let stmt = &self.prepared[id];
                info.reply(id as u32, &stmt.params, &columns)?;
                Ok(())
            }
            Err(e) => {
                if let Some(db) = e.as_db_error() {
                    info.error(ErrorKind::ER_NO, db.message().as_bytes())?;
                    return Ok(());
                }

                Err(e.into())
            }
        }
    }

    fn on_execute(
        &mut self,
        id: u32,
        ps: ParamParser,
        results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        let stmt = match self.prepared.get(id as usize) {
            None => return Ok(results.error(ErrorKind::ER_NO, b"no such prepared statement")?),
            Some(Prepared { stmt, .. }) => stmt.clone(),
        };

        // this is a little nasty because we have to take MySQL-encoded arguments and massage
        // them into &(dyn ToSql + Sync) things, which is what postgres::Client::query takes.
        // we can only do that by first boxing all the values (so they can be kept in a single
        // vec), and then collecting a *second* vec with references to those.
        let args: Vec<Box<dyn ToSql + Sync>> = ps
            .into_iter()
            .map(|p| match p.coltype {
                ColumnType::MYSQL_TYPE_SHORT => Box::new(Into::<i16>::into(p.value)) as Box<_>,
                ColumnType::MYSQL_TYPE_LONG => Box::new(Into::<i32>::into(p.value)) as Box<_>,
                ColumnType::MYSQL_TYPE_LONGLONG => Box::new(Into::<i64>::into(p.value)) as Box<_>,
                ColumnType::MYSQL_TYPE_FLOAT => Box::new(Into::<f32>::into(p.value)) as Box<_>,
                ColumnType::MYSQL_TYPE_DOUBLE => Box::new(Into::<f64>::into(p.value)) as Box<_>,
                ColumnType::MYSQL_TYPE_STRING => {
                    Box::new(Into::<&str>::into(p.value).to_owned()) as Box<_>
                }
                ct => unimplemented!(
                    "don't know how to translate PostgreSQL \
                     argument type {:?} into MySQL value",
                    ct
                ),
            })
            .collect();
        let args: Vec<&(dyn ToSql + Sync)> = args.iter().map(|a| &**a).collect();

        let rows = self.client.query(&stmt, &args[..]);
        answer_rows(results, rows)
    }

    fn on_close(&mut self, id: u32) {
        self.prepared.remove(id as usize);
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let rows = self.client.query(query, &[]);
        answer_rows(results, rows)
    }
}

/// Take a set of rows from PostgreSQL and re-encode them as MySQL rows
fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: Result<Vec<postgres::Row>, postgres::Error>,
) -> Result<(), Error> {
    match rows {
        Ok(rows) => {
            let cols: Vec<_> = rows
                .first()
                .map(|r| {
                    r.columns()
                        .iter()
                        .map(|c| {
                            let t = c.type_();
                            Column {
                                table: t.schema().to_owned(),
                                column: c.name().to_owned(),
                                coltype: p2mt(t),
                                colflags: ColumnFlags::empty(),
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut writer = results.start(&cols)?;
            for row in &rows {
                for (c, col) in cols.iter().enumerate() {
                    match col.coltype {
                        ColumnType::MYSQL_TYPE_SHORT => writer.write_col(row.get::<_, i16>(c))?,
                        ColumnType::MYSQL_TYPE_LONG => writer.write_col(row.get::<_, i32>(c))?,
                        ColumnType::MYSQL_TYPE_LONGLONG => {
                            writer.write_col(row.get::<_, i64>(c))?
                        }
                        ColumnType::MYSQL_TYPE_FLOAT => writer.write_col(row.get::<_, f32>(c))?,
                        ColumnType::MYSQL_TYPE_DOUBLE => writer.write_col(row.get::<_, f64>(c))?,
                        ColumnType::MYSQL_TYPE_STRING => {
                            writer.write_col(row.get::<_, String>(c))?
                        }
                        ct => unimplemented!(
                            "don't know how to translate PostgreSQL \
                             argument type {:?} into MySQL value",
                            ct
                        ),
                    }
                }
                writer.end_row()?;
            }
            writer.finish()?;
        }
        Err(e) => {
            results.error(ErrorKind::ER_BAD_SLAVE, format!("{:?}", e).as_bytes())?;
        }
    }
    Ok(())
}

/// Convert a PostgreSQL data type and translate it into the corresponding MySQL type
fn p2mt(t: &Type) -> msql_srv::ColumnType {
    match *t {
        Type::INT2 => msql_srv::ColumnType::MYSQL_TYPE_SHORT,
        Type::INT4 => msql_srv::ColumnType::MYSQL_TYPE_LONG,
        Type::INT8 => msql_srv::ColumnType::MYSQL_TYPE_LONGLONG,
        Type::FLOAT4 => msql_srv::ColumnType::MYSQL_TYPE_FLOAT,
        Type::FLOAT8 => msql_srv::ColumnType::MYSQL_TYPE_DOUBLE,
        Type::TEXT => msql_srv::ColumnType::MYSQL_TYPE_STRING,
        ref t => unimplemented!(
            "don't know how to translate PostgreSQL type {:?} to a MySQL type",
            t
        ),
    }
}
