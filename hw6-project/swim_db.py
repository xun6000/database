import psycopg2
import csv
from datetime import date


# issue
# 1. null parameters
# 2. leg of non-relay
def add_test_data(cur, conn):
    # Org
    update_org(cur, conn, "U422 UTAustion true".split())
    # update it
    update_org(cur, conn, "U422 rice true".split())

    update_meet(cur, conn, 'NCAA_Summer 2007-7-7 4 U422'.split())
    update_meet(cur, conn, 'NCAA_Summer 2009-9-9 4 U422'.split())

    update_stroke(cur, conn, "freestyle".split())
    update_stroke(cur, conn, "freestyle".split())
    update_stroke(cur, conn, "butterfly".split())

    update_distance(cur, conn, "100".split())
    update_distance(cur, conn, "100".split())
    update_distance(cur, conn, "200".split())

    update_leg(cur, conn, "1".split())
    update_leg(cur, conn, "1".split())
    update_leg(cur, conn, "2".split())

    update_participant(cur, conn, "P187734 F U422 Mary".split())
    update_participant(cur, conn, "P187734 M U422 Mike".split())

    update_event(cur, conn, "E0107 F 100".split())
    update_event(cur, conn, "E0107 M 200".split())

    update_heat(cur, conn, "1 E0107 NCAA_Summer".split())
    update_heat(cur, conn, "1 E0107 NCAA_Summer".split())

    update_stroke_of(cur, conn, "E0107 1 freestyle".split())
    update_stroke_of(cur, conn, "E0107 1 butterfly".split())

    update_swim(cur, conn, "1 E0107 NCAA_Summer P187734 1 22.22".split())
    update_swim(cur, conn, "1 E0107 NCAA_Summer P187734 2 11.11".split())
    conn.commit()


def switch(name):
    return {
        "Org": update_org,
        "Meet": update_meet,
        "Stroke": update_stroke,
        "Distance": update_distance,
        "Leg": update_leg,
        "Participant": update_participant,
        "Event": update_event,
        "Heat": update_heat,
        "StrokeOf": update_stroke_of,
        "Swim": update_swim
    }[name]


def read_from_file(cur, conn, filename):
    """Parse all date from given csv file and then add them one by one on the server in the sequence of dependency."""

    # collect data
    with open(filename, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        data = {}
        for row in reader:
            if row[0][0] == "*":
                print(row)
                table_name = row[0].replace("*", "").replace(" ", "")
                if table_name not in data:
                    data[table_name] = []
                continue
            else:
                data[table_name].append(row)

    # update table in the seq of dependency.
    for curr_table in ["Org", "Meet", "Stroke", "Distance", "Leg", "Participant",
                       "Event", "Heat", "StrokeOf", "Swim"]:
        print("current table: ", curr_table)
        for row in data[curr_table]:
            print("current row: ", row)
            switch(curr_table)(cur, conn, row)


def str2bool(v):
    if v.lower() in ("yes", "y", "true", "t", "on", "1"):
        return True
    if v.lower() in ("f", "false", 'n', 'no', 'off', '0'):
        return False
    raise ValueError


def str2datetime(inp):
    raw = inp.split('-')
    if len(raw) != 3:
        print("Only accept datetime in format of year-month-day")
        return
    year, month, day = raw
    return date(int(year), int(month), int(day))


def check_leg(inp):
    try:
        leg = int(inp)
    except ValueError:
        print("Invalid leg format")
        return False

    if leg < 0 or leg > 4:
        print("Invalid leg number(Choices: 1,2,3,4 (use 0 if it is not applicable)")  # ???????
        return False
    return True


def check_distance(inp):
    try:
        distance = int(inp)
    except ValueError:
        print("Invalid distance format")
        return False

    if distance not in (100, 200, 400):
        print("Invalid distance(Choices: 100, 200, 400)")
        return False
    return True


def check_gender(gender):
    if gender.upper() not in ('F', 'M'):
        print("Wrong gender (F/M)")
        return False
    return True


def check_stroke(inp):
    stroke = inp.lower()
    if stroke not in ("freestyle", "medley", "butterfly", "breaststroke", "backstroke"):
        print("Invalid stroke")
        return False
    return True


def init_tables(cur, conn):
    """Initialize all tables needed in swim project."""

    cur.callproc("InitTables", )
    conn.commit()
    print("tables are created.")


def update_org(cur, conn, raw):
    """VARCHAR(50), name VARCHAR(50), is_univ BOOLEAN"""

    #  Check length.
    if len(raw) < 3:
        print("Invalid number of parameters.")
        return

    # Check format.
    org_id, name, is_univ = raw[0:3]
    try:
        is_univ = str2bool(is_univ)
    except ValueError:
        print("Invalid input format.")
        return

    # Try to call server.
    try:
        cur.callproc("updateOrg", (org_id, name, is_univ))
        conn.commit()
    except:
        print("Calling updateOrg failed.")
        raise


def update_meet(cur, conn, raw):
    """name VARCHAR(50), start_dat DATE,num_days INT,org_id VARCHAR(50)"""

    #  Check length.
    if len(raw) < 4:
        print("Invalid number of parameters.")
        return

    # Check format.
    name, start_dat, num_days, org_id = raw[0:4]
    try:
        start_dat = str2datetime(start_dat)
        num_days = int(num_days)
    except ValueError:
        print("Invalid input format.")
        return

    # Try to call server.
    try:
        cur.callproc("updateMeet", (name, start_dat, num_days, org_id))
        conn.commit()
    except psycopg2.IntegrityError:
        print("IntegrityError")
    except:
        print("Calling updateMeet failed.")
        raise


def update_stroke(cur, conn, raw):
    """stroke VARCHAR(50)"""

    stroke = raw[0]
    # Check format.
    if not check_stroke(stroke):
        return

    # Try to call server.
    try:
        cur.callproc("updateStroke", (stroke.lower(),))
        conn.commit()
    except:
        print("Calling updateStroke failed")
        raise


def update_distance(cur, conn, raw):
    """stroke VARCHAR(50)"""

    distance = raw[0]
    # Check distance.
    if not check_distance(distance):
        return

    # Try to call server.
    try:
        cur.callproc("updateDistance", (int(distance),))
        conn.commit()
    except:
        print("Calling updateDistance failed....")
        raise


def update_leg(cur, conn, raw):
    """stroke VARCHAR(50)"""

    leg = raw[0]
    # Check format.
    if not check_leg(leg):
        return

    # Try to call server.
    try:
        cur.callproc("updateLeg", (int(leg),))
        conn.commit()
    except:
        print("Calling updateLeg failed....")
        raise


def update_participant(cur, conn, raw):
    """id VARCHAR(50), name VARCHAR(50), gender CHAR(1), org_id VARCHAR(50)"""

    #  Check length.
    if len(raw) < 4:
        print("Invalid number of parameters.")
        return

    # Check format.
    p_id, gender, org_id, name = raw[0:4]
    if not check_gender(gender):
        return

    # Try to call server.
    try:
        cur.callproc("updateParticipant", (p_id, gender.upper(), org_id, name))
        conn.commit()
    except:
        print("Calling updateParticipant failed.")
        raise


def update_event(cur, conn, raw):
    """id VARCHAR(50),gender CHAR(1), distance INT"""

    #  Check length.
    if len(raw) < 3:
        print("Invalid number of parameters.")
        return

    # Check format.
    e_id, gender, distance = raw[0:3]
    if ((not check_gender(gender)) or (not check_distance(distance))):
        return

    # Try to call server.
    try:
        cur.callproc("updateEvent", (e_id, gender.upper(), int(distance)))
        conn.commit()
    except:
        print("Calling updateEvent failed.")
        raise


def update_heat(cur, conn, raw):
    """id VARCHAR(50), event_id VARCHAR(50), meet_name VARCHAR(50)"""

    #  Check length.
    if len(raw) < 3:
        print("Invalid number of parameters.")
        return

    # Check format.
    h_id, event_id, meet_name = raw[0:3]

    # Try to call server.
    try:
        cur.callproc("updateHeat", (h_id, event_id, meet_name))
        conn.commit()
    except:
        print("Calling updateHeat failed.")
        raise


def update_stroke_of(cur, conn, raw):
    """event_id VARCHAR(50),leg INT, stroke VARCHAR(50)"""

    #  Check length.
    if len(raw) < 3:
        print("Invalid number of parameters.")
        return

    # Check format.
    event_id, leg, stroke = raw[0:3]
    if (not check_leg(leg)) or (not check_stroke(stroke)):
        return

    # Try to call server.
    try:
        cur.callproc("updateStrokeOf", (event_id, int(leg), stroke.lower()))
        conn.commit()
    except:
        print("Calling updateStrokeOf failed.")
        raise


def update_swim(cur, conn, raw):
    """heat_id VARCHAR(50), event_id VARCHAR(50), meet_name VARCHAR(50),
    participant_id VARCHAR(50), leg INT, elapsed_time NUMERIC"""

    #  Check length.
    if len(raw) < 6:
        print("Invalid number of parameters.")
        return

    # Check format.
    heat_id, event_id, meet_name, participant_id, leg, elapsed_time = raw[0:6]

    if leg == "":
        update_leg(cur, conn, "0")
        leg = "0"

    # Check leg.
    if not check_leg(leg):
        return

    # check elapsed_time
    try:
        elapsed_time = float(elapsed_time)
    except ValueError:
        print("Invalid elapsed_time fromat")
        return

    # Try to call server.
    try:
        cur.callproc("updateSwim", (heat_id, event_id, meet_name, participant_id, int(leg), elapsed_time))
        conn.commit()
    except:
        print("Calling updateSwim failed.")
        raise


def add_data(cur, conn):
    """ Add a row of data into the specified table."""

    table_name = input("Enter the label of things you want to do: table_name(string) \n "
                       "Choices: Org, Meet, Stroke, Distance, Leg, Participant, Event, Heat, StrokeOf, Swim. \n")

    if table_name == "Org":
        raw = input(" Enter data: id VARCHAR(50), name VARCHAR(50), is_univ BOOLEAN\n")

    elif table_name == "Meet":
        raw = input(" Enter data: name VARCHAR(50), start_dat DATE,num_days INT,org_id VARCHAR(50)\n")

    elif table_name == "Stroke":
        raw = input(" Enter data: stroke VARCHAR(50)\n")

    elif table_name == "Distance":
        raw = input(" Enter data: distance INT\n")

    elif table_name == "Leg":
        raw = input(" Enter data: leg INT\n")

    elif table_name == "Participant":
        raw = input(" Enter data: id VARCHAR(50), name VARCHAR(50), gender CHAR(1), org_id VARCHAR(50)\n")

    elif table_name == "Event":
        raw = input(" Enter data: id VARCHAR(50),gender CHAR(1), stroke varchar(50), distance INT\n")

    elif table_name == "Heat":
        raw = input(" Enter data: id VARCHAR(50), event_id VARCHAR(50), meet_name VARCHAR(50)\n")

    elif table_name == "StrokeOf":
        raw = input(" Enter data: event_id VARCHAR(50),leg INT, stroke VARCHAR(50)\n")

    elif table_name == "Swim":
        raw = input(" Enter data: heat_id VARCHAR(50), event_id VARCHAR(50), "
                    "meet_name VARCHAR(50), participant_id VARCHAR(50), leg INT, e_time NUMERIC\n")

    else:
        print("Invalid table name.")
        return

    print("Your input", raw.split())
    switch(table_name)(cur, conn, raw.split())


def main():
    # Ask for password.
    password = input("Please enter your password: ")

    # Connect to an existing database
    try:
        conn = psycopg2.connect(dbname="postgres", user="ricedb", password=password, host='localhost', port=5432)
        print("Connected to database rice_db.")
    except:
        print("Connection failed.")
        return

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Initialize tables.
    init_tables(cur, conn)

    add_test_data(cur, conn)

    while True:
        print(" =====\n "
              "1: Read data from a file \n "
              " 2: Add a row of data \n"
              " 3: Update a row of data \n "
              " 4: Save tables to a file \n"
              " 5: Quit \n"
              " =====\n")

        choice = input("Enter the label of things you want to do: ")
        if choice == "1":
            # file_name = input("Enter the name of csv file: ")
            file_name = "data_apr10.csv"
            read_from_file(cur, conn, file_name)
        elif choice == "2":
            add_data(cur, conn)
        elif choice == "3":
            add_data(cur, conn)
        elif choice == "4":
            pass
        elif choice == "5":
            print("Bye!")
            break

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()