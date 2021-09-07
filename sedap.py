import sys
import os
import stomp

# ActiveMQ
host = "localhost"
port = 61613
destination = "/queue/event"

conn = stomp.Connection(host_and_ports=[(host, port)])
conn.connect()

# file paths
blah = r"C:\Users\Ayman\Documents\CustomerWatchTime.csv"
blah_2 = r"C:\Users\Ayman\Documents\CustomerWatchTimeEdited.csv"
demo_file_path = r"C:\Users\Ayman\Documents\demofile.txt"

# checking if file exists
if os.path.exists(blah_2):
    print('File exists.')
else:
    sys.exit('File does not exist yet.')

# TODO: read csv after skipping first line
# with open(blah,'r') as f:
#     with open(blah_2,'w') as f1:
#         next(f) # skip header line
#         for line in f:
#             f1.write(line)
# **************************************************

# Reading new file

# for each_row in file_blah2:
#     if len(str(each_row).strip()) > 0:
#         print(each_row.split(','))

file_blah2 = open(blah_2)
demo_file = open(demo_file_path, 'a')


def validate_and_message(file_open):
    e_id = ()
    count = 0
    with open(demo_file, 'a') as file:

        for each_row in file_open:
            if len(str(each_row).strip()) > 0:
                row = each_row.split(',')
                e_id.append(row[0])
                count += 1

                if row[1].isnumeric() and int(row[1]) > 0:    # Customer ID
                    pass
                else:
                    line = 'Error Message Row: {}, Invalid Customer ID '.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

                if row[2].isalpha() and len(row[2]) < 31:    # Customer First Name
                    pass
                else:
                    line = 'Error Message Row: {}, Invalid Customer First Name'.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

                if row[3].isalpha() and len(row[3]) < 31:    # Customer Last Name
                    pass
                else:
                    line = 'Error Message Row: {}, Invalid Customer Last Name'.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

                if row[4].isnumeric():                       # Channel number
                    if int(row[4]) > 99 and int(row[4]) < 1501:
                        pass
                else:
                    line = 'Error Message Row: {}, Invalid Channel Number'.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

                if row[5].isnumeric():                       # Start Watch Time
                    if int(row[5]) // 100 < 24 and abs(int(row[5])) % 100 < 60:
                        pass
                    start_time = int(row[5])
                else:
                    line = 'Error Message Row: {}, Invalid Start Watch Time'.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

                if row[6].isnumeric() and int(row[6]) > start_time:         # End Watch Time
                    if int(row[6]) // 100 < 24 and abs(int(row[6])) % 100 < 60:
                        pass
                else:
                    line = 'Error Message Row: {}, Invalid End Watch Time'.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

                if row[7].isnumeric():                      # Customer Age
                    if int(row[7]) >= 15 and int(row[7]) <=121:
                        pass
                else:
                    line = 'Error Message Row: {}, Invalid Customer Age'.format(row)
                    file.write(line)
                    conn.send(destination="test.queue", body=line, persistent='true')

        e_id_set = set(e_id)
        if len(e_id_set) == len(e_id):
            pass
        else:
            # TODO: Message for invalid Entry ID
            sys.exit('Entry IDs are not unique.')




















