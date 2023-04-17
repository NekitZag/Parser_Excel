import sqlite3

class DateBase:
    def __init__(self, file_DB):
        self.con = sqlite3.Connection(file_DB)
        self.cur = self.con.cursor()
        self.cur.execute("""PRAGMA foreign_keys = ON;""")
        self.con.commit()
        self.clear_tables()

    def clear_tables(self):
        for table in self.cur.execute("SELECT name FROM sqlite_master WHERE type='table'"
                                      "LIMIT -1 OFFSET 1 ").fetchall():
            self.cur.execute("DELETE FROM " + table[0])
            self.con.commit()
            self.cur.execute("UPDATE sqlite_sequence set seq=0 where name='"+table[0]+"'"  )
            self.con.commit()


    def create_tables(self):
        self.create_table_result()
        self.create_fact()
        self.create_forecast()
        self.create_QLiq()
        self.create_Qoil()
        self.create_company()

    def create_table_result(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS table_result(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           id_fact INTEGER REFERENCES fact(id) ON DELETE CASCADE,
           id_forcast INTEGER REFERENCES forcast(id) ON DELETE CASCADE
           );
        """)
        self.con.commit()

    def create_fact(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS fact(
           id INTEGER PRIMARY KEY,
           id_company INTEGER REFERENCES Company(id) ON DELETE CASCADE,
           id_Qliq INTEGER REFERENCES Qliq(id) ON DELETE CASCADE,
           id_Qoil INTEGER REFERENCES Qoil(id) ON DELETE CASCADE
           );
        """)
        self.con.commit()

    def create_forecast(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS forcast(
           id INTEGER PRIMARY KEY,
           id_company INTEGER REFERENCES Company(id) ON DELETE CASCADE,
           id_Qliq INTEGER REFERENCES Qliq(id) ON DELETE CASCADE,
           id_Qoil INTEGER REFERENCES Qoil(id) ON DELETE CASCADE
           );
        """)
        self.con.commit()

    def create_QLiq(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS Qliq(
           id INTEGER PRIMARY KEY,
           data1 INTEGER,
           data2 INTEGER
           );
        """)
        self.con.commit()

    def create_Qoil(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS Qoil(
           id INTEGER PRIMARY KEY,
           data1 INTEGER,
           data2 INTEGER
           );
        """)
        self.con.commit()

    def create_company(self):
        self.cur.execute("""CREATE TABLE IF NOT EXISTS Company(
           id INTEGER PRIMARY KEY,
           Company_Name text);
        """)
        self.con.commit()














