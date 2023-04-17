from init_DB import *
import xlrd

class Excel:
    def __init__(self, file_name_excel):
        self.rb = xlrd.open_workbook(file_name_excel)
        self.sheet = self.rb.sheet_by_index(0)

class Parser_excel_DB():
    def __init__(self, excel: Excel, database: DateBase):
        self.sheet = excel.sheet
        self.database = database
        self.list_company = []

    def push_unique_companies(self):
        self.database.cur.execute("delete from company")
        for i in range(3, self.sheet.nrows):
            if self.sheet.row_values(i)[1] not in self.list_company:
                self.list_company.append(self.sheet.row_values(i)[1])
        self.database.cur.executemany("insert into company(id, company_name) values(?, ?)",
                        [[id, elem] for id, elem in enumerate(self.list_company)])
        self.database.con.commit()

    def fill_DB_from_excel(self):
        index = 1
        for i in range(3, self.sheet.nrows):
            self.database.cur.execute("insert into Qliq (id, data1, data2) values (?, ?, ?)",
                        [index, self.sheet.row_values(i)[2], self.sheet.row_values(i)[3]])
            self.database.cur.execute("insert into Qoil (id, data1, data2) values (?, ?, ?)",
                        [index, self.sheet.row_values(i)[4], self.sheet.row_values(i)[5]])
            self.database.cur.execute("insert into fact (id, id_company, id_Qliq, id_Qoil)"
                        "values (?, ?, ?, ?)",
                        [i - 2, self.list_company.index(self.sheet.row_values(i)[1]), index, index])
            index += 1
            self.database.cur.execute("insert into Qliq (id, data1, data2) values (?, ?, ?)",
                        [index, self.sheet.row_values(i)[6], self.sheet.row_values(i)[7]])
            self.database.cur.execute("insert into Qoil (id, data1, data2) values (?, ?, ?)",
                        [index, self.sheet.row_values(i)[8], self.sheet.row_values(i)[9]])
            self.database.cur.execute("insert into forcast (id, id_company, id_Qliq, id_Qoil)"
                        "values (?, ?, ?, ?)",
                        [i - 2, self.list_company.index(self.sheet.row_values(i)[1]), index, index])
            index += 1
            self.database.cur.execute("insert into table_result (id_fact, id_forcast)"
                        "values (?, ?)",
                        [i - 2, i - 2])
        self.database.con.commit()


if __name__ == "__main__":
    BD = DateBase('data.db')
    BD.create_tables()
    Exc = Excel("Приложение_к_заданию_бек_разработчика.xlsx")
    ParserEx = Parser_excel_DB(Exc, BD)
    ParserEx.push_unique_companies()
    ParserEx.fill_DB_from_excel()

# Добавление даты к Qliq
    try:
        BD.cur.execute("""ALTER TABLE Qliq ADD COLUMN date DATETIME""")
    except:
        pass

# Добавление даты к Qoil
    try:
        BD.cur.execute("""ALTER TABLE Qoil ADD COLUMN date DATETIME""")
    except:
        pass


    BD.cur.execute("""update Qliq set date = date(strftime('%s', '2000-01-01 00:00:00') +
     abs(random() % (strftime('%s', '2000-01-31 23:59:59') -
                       strftime('%s', '2000-01-01 00:00:00'))
     ), 
     'unixepoch');""")
    BD.con.commit()


    BD.cur.execute("""update Qoil set date = date(strftime('%s', '2000-01-01 00:00:00') +
         abs(random() % (strftime('%s', '2000-01-31 23:59:59') -
                           strftime('%s', '2000-01-01 00:00:00'))
         ), 
         'unixepoch');""")
    BD.con.commit()

# Тотал для Qliq и Qoil
    BD.cur.execute("""CREATE TABLE IF NOT EXISTS Qliq_Total(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date ,
                        total_Qliq INTEGER
                        )""")
    BD.con.commit()
    BD.cur.execute("""CREATE TABLE IF NOT EXISTS Qoil_Total(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date ,
                        total_Qoil INTEGER
                        )""")
    BD.con.commit()

# Групировка тотал Qliq и Qoil по датам
    BD.cur.execute("""insert into Qliq_Total (date, total_Qliq)
                        SELECT date, (sum(data1)+ sum(data2)) from Qliq
                        group by date""")
    BD.con.commit()

    BD.cur.execute("""insert into Qoil_Total (date, total_Qoil)
                            SELECT date, (sum(data1)+ sum(data2)) from Qoil
                            group by date""")
    BD.con.commit()
