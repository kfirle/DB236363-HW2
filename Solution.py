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
    answer = Query(*values[0:3])
    return answer

def diskFromResultSet(result : Connector.ResultSet) -> Disk:
    values = list(result.__getitem__(0).values())
    answer = Disk(*values)
    return answer

def ramFromResultSet(result : Connector.ResultSet) -> RAM:
    values = list(result.__getitem__(0).values())
    answer = RAM(*values[0:3])
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
                     "size INTEGER NOT NULL CHECK (size > 0),"
                     "company TEXT NOT NULL,"
                     "PRIMARY KEY (id)"
                     ");"
                     "CREATE TABLE RunningQueries("
                     "query_id INTEGER NOT NULL CHECK (query_id > 0),"
                     "disk_id INTEGER NOT NULL CHECK (disk_id > 0),"
                     "FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (query_id) REFERENCES Query(id) ON DELETE CASCADE,"
                     "PRIMARY KEY (disk_id, query_id)"
                     ");"
                     "CREATE TABLE RamsOnDisk("
                     "ram_id INTEGER NOT NULL CHECK (ram_id > 0),"
                     "disk_id INTEGER NOT NULL CHECK (disk_id > 0),"
                     "FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE,"
                     "FOREIGN KEY (ram_id) REFERENCES RAM(id) ON DELETE CASCADE,"
                     "PRIMARY KEY (disk_id, ram_id)"
                     ");"
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
        # do stuff
        print(e)
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
        conn.rollback()
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Query(id, purpose, size) VALUES({id}, {purpose}, {size})")\
            .format(id=sql.Literal(query.getQueryID()), purpose=sql.Literal(query.getPurpose()),
                    size=sql.Literal(query.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        # return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Query WHERE id={queryID}".format(queryID=queryID))
        rows_effected, result = conn.execute(query, printSchema=True)
        answer = queryFromResultSet(result)
        conn.commit()
        return answer
    except Exception as e:
        print(e)
        return Query.badQuery()
    finally:
        conn.close()

# TODO: do not forget to adjust the free space on disk if the query runs on one. Hint - think about
#  transactions in such cases (there are more in this assignment.
def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Query WHERE id={queryID}".format(queryID=query.getQueryID()))
        rows_effected, _ = conn.execute(query, printSchema=True)
        conn.commit()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()
        # return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(id, company, speed, space, cost) VALUES({id}, {company}, {speed}, "
                        "{space}, {cost})") \
            .format(id=sql.Literal(disk.getDiskID()), company=sql.Literal(disk.getCompany()),
                    speed=sql.Literal(disk.getSpeed()), space=sql.Literal(disk.getFreeSpace()),
                    cost=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        # return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Disk WHERE id={diskID}".format(diskID=diskID))
        rows_effected, result = conn.execute(query, printSchema=True)
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
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        # return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO RAM(id, size, company) VALUES({id}, {size}, {company})") \
            .format(id=sql.Literal(ram.getRamID()), company=sql.Literal(ram.getCompany()),
                    size=sql.Literal(ram.getSize()))
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        # return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM RAM WHERE id={ramID}".format(ramID=ramID))
        rows_effected, result = conn.execute(query, printSchema=True)
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
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        # return ReturnValue.OK


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
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        # return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "UPDATE Disk "
                        "SET space = space - {query_size} "
                        "WHERE id = {disk_id};"
                        "INSERT INTO RunningQueries "
                        "VALUES({query_id}, {disk_id});"
                        "COMMIT;"
                        .format(query_size=query.getSize(), query_id=query.getQueryID(), disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=True)
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
                        "WHERE id = {disk_id};"
                        "DELETE FROM RunningQueries "
                        "WHERE query_id = {query_id} AND disk_id = {disk_id};"
                        "COMMIT;"
                        .format(query_size=query.getSize(), query_id=query.getQueryID(), disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        query = sql.SQL("DELETE FROM RunningQueries "
                        "WHERE ram_id = {ram_id} AND disk_id = {disk_id};"
                        .format(ram_id=ramID, disk_id=diskID))
        rows_effected, _ = conn.execute(query, printSchema=True)
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
        rows_effected, result = conn.execute(query, printSchema=True)
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
        rows_effected, result = conn.execute(query, printSchema=True)
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
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []
