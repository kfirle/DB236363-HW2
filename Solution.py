from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

def queryFromResultSet(result : Connector.ResultSet) -> Query:
    values = list(result.__getitem__(0).values())
    answer = Query(*values)
    return answer

def diskFromResultSet(result : Connector.ResultSet) -> Disk:
    values = list(result.__getitem__(0).values())
    answer = Disk(*values)
    return answer

def ramFromResultSet(result : Connector.ResultSet) -> RAM:
    values = list(result.__getitem__(0).values())
    answer = RAM(*values)
    return answer

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "CREATE TABLE Disk("
                     "id INTEGER NOT NULL CHECK (id > 0),"
                     "company TEXT NOT NULL,"
                     "speed INTEGER NOT NULL CHECK (speed > 0),"
                     "space INTEGER NOT NULL CHECK (space >= 0),"
                     "cost INTEGER NOT NULL CHECK (cost > 0),"
                     "PRIMARY KEY (id)"
                     ");"
                     "CREATE TABLE Query("
                     "id INTEGER NOT NULL CHECK (id > 0),"
                     "purpose TEXT NOT NULL,"
                     "size INTEGER NOT NULL CHECK (size >= 0),"
                     "PRIMARY KEY (id)"
                     ");"
                     "CREATE TABLE RAM("
                     "id INTEGER NOT NULL CHECK (id > 0),"
                     "company TEXT NOT NULL,"
                     "size INTEGER NOT NULL CHECK (size > 0),"
                     "PRIMARY KEY (id)"
                     ");"
                     "CREATE TABLE RunningQueries("
                     "query_id INTEGER NOT NULL CHECK (query_id > 0),"
                     "disk_id INTEGER NOT NULL CHECK (disk_id > 0),"
                     "FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (query_id) REFERENCES Query(id) ON DELETE CASCADE,"
                     "UNIQUE (disk_id, query_id)"
                     ");"
                     "CREATE TABLE RamsOnDisk("
                     "ram_id INTEGER NOT NULL CHECK (ram_id > 0),"
                     "disk_id INTEGER NOT NULL CHECK (disk_id > 0),"
                     "FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (ram_id) REFERENCES RAM(id) ON DELETE CASCADE,"
                     "UNIQUE (disk_id, ram_id)"
                     ");"
                     "CREATE VIEW DISKS_AND_AVAILABLE_QUERIES_THAT_CAN_RUN_ON_THEM AS "
                     "SELECT COALESCE(COUNT(DQ.query_id),0) AS count, COALESCE(DQ.speed, 0) AS speed, DQ.id AS id "
                     "FROM ("
                     "(SELECT id, speed "
                     "FROM Disk) AS B "
                     "FULL OUTER JOIN "
                     "(SELECT D.id AS disk_id, Q.id AS query_id "
                     "FROM Disk AS D, Query AS Q "
                     "WHERE D.space - Q.size >= 0 ) AS A ON A.disk_id = B.id"
                     ") AS DQ "
                     "GROUP BY DQ.id, DQ.disk_id, DQ.speed;"
                     
                     # "CREATE MATERIALIZED VIEW DISK_QUERY_RAM AS "
                     # "SELECT D.id AS disk_id, D.company AS disk_company, D.speed AS speed, D.space AS space, D.cost AS cost, "
                     # "Q.id AS query_id, Q.purpose AS purpose, Q.size AS query_size, "
                     # "R.id AS ram_id,  R.id AS ram_size, R.id AS ram_company "
                     # "FROM Disk AS D, Query AS Q, RAM AS R;"
                     # "CREATE MATERIALIZED VIEW DISK_QUERY AS "
                     # "SELECT D.id AS disk_id, D.company AS disk_company, D.speed AS speed, D.space AS space, D.cost AS cost, "
                     # "Q.id AS query_id, Q.purpose AS purpose, Q.size AS query_size "
                     # "FROM Disk AS D, Query AS Q ;"
                     
                     "COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM Query;"
                     "DELETE FROM Disk;"
                     "DELETE FROM RAM;"
                     "DELETE FROM RunningQueries;"
                     "DELETE FROM RamsOnDisk;"
                     "COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Query CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS RAM CASCADE;"
                     "DROP TABLE IF EXISTS RunningQueries CASCADE;"
                     "DROP TABLE IF EXISTS RamsOnDisk CASCADE;"
                     "COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Query(id, purpose, size) VALUES({id}, {purpose}, {size})")\
            .format(id=sql.Literal(query.getQueryID()), purpose=sql.Literal(query.getPurpose()),
                    size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def getQueryProfile(queryID: int) -> Query:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Query WHERE id={queryID}".format(queryID=queryID))
        rows_effected, result = conn.execute(query, printSchema=False)
        answer = queryFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return Query.badQuery()
    finally:
        conn.close()

def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "UPDATE Disk "
                        "SET space = space + {query_size} "
                        "WHERE id IN (SELECT disk_id FROM RunningQueries WHERE query_id = {query_id});"
                        "DELETE FROM Query WHERE id={query_id};"
                        "COMMIT;".format(query_id=query.getQueryID(), query_size=query.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=False)
        return ReturnValue.OK
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(id, company, speed, space, cost) VALUES({id}, {company}, {speed}, "
                        "{space}, {cost})") \
            .format(id=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                    speed=sql.Literal(disk.getSpeed()), space=sql.Literal(disk.getFreeSpace()),
                    cost=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disk WHERE id={diskID}".format(diskID=diskID))
        rows_effected, result = conn.execute(query, printSchema=False)
        answer = diskFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return Disk.badDisk()
    finally:
        conn.close()


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE id={diskID}".format(diskID=diskID))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(id, company, size) VALUES({id}, {company}, {size})") \
            .format(id=sql.Literal(ram.getRamID()), company=sql.Literal(ram.getCompany()),
                    size=sql.Literal(ram.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM RAM WHERE id={ramID}".format(ramID=ramID))
        rows_effected, result = conn.execute(query, printSchema=False)
        answer = ramFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return RAM.badRAM()
    finally:
        conn.close()


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM WHERE id={ramID}".format(ramID=ramID))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO Disk(id, company, speed, space, cost) VALUES({id_disk}, {company}, {speed}, {space}, {cost});"
                        "INSERT INTO Query(id, purpose, size) VALUES({id_query}, {purpose}, {size});"
                        "COMMIT;") \
            .format(id_disk=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                    speed=sql.Literal(disk.getSpeed()), space=sql.Literal(disk.getFreeSpace()),
                    cost=sql.Literal(disk.getCost()), id_query=sql.Literal(query.getQueryID()), purpose=sql.Literal(query.getPurpose()),
                    size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=False)
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO RunningQueries "
                        "VALUES({query_id}, {disk_id});"
                        "UPDATE Disk "
                        "SET space = space - {query_size} "
                        "WHERE id = {disk_id};"
                        "COMMIT;"
                        .format(query_size=query.getSize(), query_id=query.getQueryID(), disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=False)
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "UPDATE Disk "
                        "SET space = space + {query_size} "
                        "WHERE id = {disk_id} AND EXISTS (SELECT disk_id FROM RunningQueries WHERE query_id = {query_id} AND disk_id = {disk_id});"
                        "DELETE FROM RunningQueries "
                        "WHERE query_id = {query_id} AND disk_id = {disk_id};"
                        "COMMIT;"
                        .format(query_size=query.getSize(), query_id=query.getQueryID(), disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=False)
        return ReturnValue.OK
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        conn.close()


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RamsOnDisk "
                        "VALUES({ram_id}, {disk_id});"
                        .format(ram_id=ramID, disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        return ReturnValue.OK
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RamsOnDisk "
                        "WHERE ram_id = {ram_id} AND disk_id = {disk_id};"
                        .format(ram_id=ramID, disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=False)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()


def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(size) AS avg "
                        "FROM Query INNER JOIN (SELECT query_id "
                                               "FROM RunningQueries "
                                               "WHERE disk_id = {disk_id}) AS Q "
                                               "ON id = query_id".format(disk_id=diskID))
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        avg = list(result.__getitem__(0).values())[0]
        if avg is None:
            return 0
        return avg
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()


def diskTotalRAM(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(size) AS sum "
                        "FROM RAM INNER JOIN (SELECT ram_id "
                        "FROM RamsOnDisk "
                        "WHERE disk_id = {disk_id}) AS Q "
                        "ON id = ram_id".format(disk_id=diskID))
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        ram_sum = list(result.__getitem__(0).values())[0]
        if ram_sum is None:
            return 0
        return ram_sum
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()


def getCostForPurpose(purpose: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(cost * size) AS sum "
                        "FROM ( RunningQueries INNER JOIN "
                        "(SELECT id AS queries_id, size "
                        "FROM Query "
                        "WHERE purpose = '{q_purpose}') AS Q ON queries_id = query_id ) INNER JOIN Disk "
                        "ON id = disk_id".format(q_purpose=purpose))
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        cost_sum = list(result.__getitem__(0).values())[0]
        if cost_sum is None:
            return 0
        return cost_sum
    except Exception as e:
        print(e)
        return -1
    finally:
        conn.close()


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    queries_id = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT Q.id "
                        "FROM Query AS Q, Disk AS D "
                        "WHERE D.id = {disk_id} AND ( D.space - Q.size >= 0 ) "
                        "ORDER BY Q.id DESC "
                        "LIMIT 5".format(disk_id=diskID))
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        for i in range(rows_effected):
            queries_id += result.__getitem__(i).values()
        return queries_id
    finally:
        conn.close()
        return queries_id


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    queries_id = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT Q.id "
                        "FROM Disk AS D, Query AS Q "
                        "WHERE D.id = {disk_id} AND ( D.space - Q.size >= 0 ) AND "
                        "( (SELECT COALESCE(SUM(size),0) AS sum FROM RAM INNER JOIN (SELECT ram_id FROM RamsOnDisk WHERE disk_id = {disk_id}) AS Q ON id = ram_id) - Q.size >=0 ) "
                        "ORDER BY Q.id ASC "
                        "LIMIT 5".format(disk_id=diskID))
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        for i in range(rows_effected):
            queries_id += result.__getitem__(i).values()
        return queries_id
    finally:
        conn.close()
        return queries_id

def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT COUNT(company) "
                        "FROM ("
                        "SELECT company "
                        "FROM Disk "
                        "WHERE id = {disk_id} "
                        "UNION "
                        "(SELECT company "
                        "FROM RAM "
                        "WHERE id IN "
                        "(SELECT ram_id "
                        "FROM RamsOnDisk "
                        "WHERE disk_id = {disk_id}))) AS C ".format(disk_id=diskID))
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        companies_num = list(result.__getitem__(0).values())[0]
        if companies_num == 0:
            return False
        if companies_num == 1:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False
    finally:
        conn.close()


def getConflictingDisks() -> List[int]:
    conn = None
    queries_id = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT disk_id "
                        "FROM RunningQueries "
                        "WHERE query_id IN "
                        "(SELECT DISTINCT query_id "
                        "FROM RunningQueries "
                        "GROUP BY query_id "
                        "HAVING COUNT(disk_id) > 1) "
                        "ORDER BY disk_id ASC")
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        for i in range(rows_effected):
            queries_id += result.__getitem__(i).values()
        return queries_id
    finally:
        conn.close()
        return queries_id


def mostAvailableDisks() -> List[int]:
    conn = None
    queries_id = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT R.id "
                        "FROM "
                        "(SELECT * FROM DISKS_AND_AVAILABLE_QUERIES_THAT_CAN_RUN_ON_THEM "
                        "ORDER BY count DESC, speed DESC, id ASC "
                        "LIMIT 5) AS R ")
        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        for i in range(rows_effected):
            queries_id += result.__getitem__(i).values()
        return queries_id
    finally:
        conn.close()
        return queries_id


def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    queries_id = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT QUERIES_RUNNING_ON_QUERY_DISKS.query_id AS query_id "
            "FROM "
            "( "
            "SELECT * "
            "FROM RunningQueries "
            "WHERE disk_id NOT IN "
            "( "
            "SELECT disk_id "
            "FROM RunningQueries "
            "WHERE disk_id NOT IN "
            "( "
            "SELECT disk_id AS disks "
            "FROM RunningQueries "
            "WHERE query_id = {query_id}"
            ") AND EXISTS (SELECT disk_id AS disks FROM RunningQueries WHERE query_id = {query_id})"
            ") "
            ") AS QUERIES_RUNNING_ON_QUERY_DISKS "
            "WHERE query_id != {query_id} "
            "GROUP BY query_id "
            "HAVING COUNT(QUERIES_RUNNING_ON_QUERY_DISKS.disk_id) * 2 >= ( SELECT (COUNT(disk_id)) FROM RunningQueries WHERE query_id = {query_id}) "
            "ORDER BY query_id ASC "
            "LIMIT 10"
            .format(query_id=queryID))

        # query = sql.SQL( "SELECT * FROM("
        # "(SELECT query_id FROM"
        # "(SELECT * FROM RunningQueries"
        # " WHERE query_id != {query_id} AND disk_id IN"
        # "(SELECT disk_id FROM RunningQueries WHERE query_id = {query_id})"
        # ") AS a "
        # "GROUP BY query_id, a.disk_id HAVING COUNT(a.disk_id) * 2 >= "
        # "SELECT (COUNT(disk_id)) FROM RunningQueriesWHERE query_id = {query_id})"
        # ") "
        # "UNION"
        # "(SELECT id as query_id "
        # "FROM Query "
        # "WHERE id != {query_id} AND NOT EXISTS(SELECT disk_id FROM RunningQueries WHERE query_id = {query_id})"
        # ")"
        # " )AS C "
        # "ORDER BY C.query_id ASC "
        # "LIMIT 10".format(query_id=queryID))

        rows_effected, result = conn.execute(query, printSchema=False)
        conn.commit()
        for i in range(rows_effected):
            queries_id += result.__getitem__(i).values()
        return queries_id
    finally:
        conn.close()
        return queries_id
