import pandas as pd
import pandasql as ps
import pymssql

Athletes = pd.read_excel('Athletes.xlsx',engine = 'openpyxl')
Coaches = pd.read_excel('Coaches.xlsx',engine = 'openpyxl')
EntriesGender = pd.read_excel('EntriesGender.xlsx',engine = 'openpyxl')
Medals = pd.read_excel('Medals.xlsx',engine = 'openpyxl')
Teams = pd.read_excel('Teams.xlsx',engine = 'openpyxl')


def conection_sql_server():

    DATA_CONNECTION = {

        "SQL_SERVER_IP":"sql-server",
        "USER_SQL_SERVER":"sa",
        "PASSWORD_SQL_SERVER":"Mi_Super_Mega_Clave_Sql_Server",
        "DATABASE_SQL_SERVER":"StatisticsOlympicGames"

    }
    server = DATA_CONNECTION['SQL_SERVER_IP']
    user = DATA_CONNECTION['USER_SQL_SERVER']
    password = DATA_CONNECTION['PASSWORD_SQL_SERVER']
    conn = pymssql.connect(server, user, password, DATA_CONNECTION['DATABASE_SQL_SERVER'])

    return conn



def create_load_data_athles(cursor, conn):

    Athletes['Name'] = Athletes['Name'].str.replace("'", '')
    Athletes['NOC'] = Athletes['NOC'].str.replace("'", '')
    Athletes['Discipline'] = Athletes['Discipline'].str.replace("'", '')


    try:
        cursor.execute("""
        
            DROP TABLE  Athletes
            
        """)

    except:
        pass


    cursor.execute("""
    
        CREATE TABLE Athletes
        (
            Name VARCHAR(100),
            NOC VARCHAR(100),
            Discipline VARCHAR(100)
        )    
    
    """)

    for i in Athletes.values:

        try:
            cursor.execute("""
    
            INSERT INTO Athletes
            (
                Name,
                NOC,
                Discipline
            )    
            VALUES
            (
                '""" + i[0] + """',
                '""" + i[1] + """',
                '""" + i[2] + """'
            )
    
            """)
        except:
            pass

    conn.commit()
    conn.close()


def create_load_data_coaches(cursor, conn):

    Coaches['NOC'] = Coaches['NOC'].str.replace("'", '')
    Coaches['Name'] = Coaches['Name'].str.replace("'", '')
    Coaches['Discipline'] = Coaches['Discipline'].str.replace("'", '')
    Coaches['Event'] = Coaches['Event'].str.replace("'", '')

    try:
        cursor.execute("""

            DROP TABLE  Coaches

        """)

    except:
        pass

    cursor.execute("""

        CREATE TABLE Coaches
        (
            Name VARCHAR(100),
            NOC VARCHAR(100),
            Discipline VARCHAR(100),
            Event VARCHAR(100)
        )    

    """)


    for i in Coaches.values:

        try:
            cursor.execute("""
            INSERT INTO Coaches
            (
                Name,
                NOC,
                Discipline,
                Event
            )    
            VALUES
            (
                '""" + str(i[0]) + """',
                '""" + str(i[1]) + """',
                '""" + str(i[2]) + """',
                '""" + str(i[3]) + """'
            )
    
            """)
        except:
            pass

    conn.commit()
    conn.close()


def create_load_data_EntriesGender(cursor, conn):

    EntriesGender['Discipline'] = EntriesGender['Discipline'].str.replace("'", '')

    try:
        cursor.execute("""

            DROP TABLE EntriesGender

        """)
    except:
        pass

    cursor.execute("""
        CREATE TABLE EntriesGender
        (
            Discipline VARCHAR(100),
            Female INT,
            Male INT,
            Total INT
        )    
    """)

    for i in EntriesGender.values:

        try:
            cursor.execute("""
    
            INSERT INTO EntriesGender
            (
                Discipline,
                Female,
                Male,
                Total
            )    
            VALUES
            (
                '""" + str(i[0]) + """',
                '""" + str(i[1]) + """',
                '""" + str(i[2]) + """',
                '""" + str(i[3]) + """'
            )
    
            """)

        except:

            pass

    conn.commit()
    conn.close()

def create_load_data_Medals(cursor, conn):

    Medals.rename(columns={'Team/NOC': 'NOC'}, inplace=True)
    Medals['NOC'] = Medals['NOC'].str.replace("'", '')

    try:
        cursor.execute("""
            DROP TABLE Medals
        """)
    except:
        pass


    cursor.execute("""
        CREATE TABLE Medals
        (
            Rank INT,
            NOC VARCHAR(500),
            Gold INT,
            Silver INT,
            Bronze INT,
            Total INT, 
            RankTotal INT
        )    
    """)


    for i in Medals.values:

        try:
            cursor.execute("""


            INSERT INTO Medals
            (
                Rank,
                NOC,
                Gold,
                Silver,
                Bronze,
                Total,
                RankTotal
            )    
            VALUES
            (
                '""" + str(i[0]) + """',
                '""" + str(i[1]) + """',
                '""" + str(i[2]) + """',
                '""" + str(i[3]) + """',
                '""" + str(i[4]) + """',
                '""" + str(i[5]) + """',
                '""" + str(i[6]) + """'
            )

            """)
        except:
            pass

    conn.commit()
    conn.close()



def create_load_data_Teams(cursor, conn):

    Teams['Name'] = Teams['NOC'].str.replace("'", '')
    Teams['Discipline'] = Teams['Event'].str.replace("'", '')
    Teams['NOC'] = Teams['NOC'].str.replace("'", '')
    Teams['Event'] = Teams['Event'].str.replace("'", '')

    try:
        cursor.execute("""
            DROP TABLE Teams 
        """)
    except:
        pass

    cursor.execute("""
        CREATE TABLE Teams
        (
            Name VARCHAR(100),
            Discipline VARCHAR(100),
            NOC VARCHAR(100),
            Event VARCHAR(100)
        )    
    """)


    for i in Teams.values:

        try:
            cursor.execute("""
    
    
            INSERT INTO Teams
            (
                Name,
                Discipline,
                NOC,
                Event
    
            )    
            VALUES
            (
                '""" + str(i[0]) + """',
                '""" + str(i[1]) + """',
                '""" + str(i[2]) + """',
                '""" + str(i[3]) + """'
    
            )
    
            """)
        except:
            pass

    conn.commit()
    conn.close()





#ATHELTES
conn = conection_sql_server()
cursor = conn.cursor()
create_load_data_athles(cursor, conn)

#COACHES
conn = conection_sql_server()
cursor = conn.cursor()
create_load_data_coaches(cursor, conn)

#EntriesGender
conn = conection_sql_server()
cursor = conn.cursor()
create_load_data_EntriesGender(cursor, conn)

#Medals
conn = conection_sql_server()
cursor = conn.cursor()
create_load_data_Medals(cursor, conn)

#Teams
conn = conection_sql_server()
cursor = conn.cursor()
create_load_data_Teams(cursor, conn)