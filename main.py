# project aims to sort data bases with big number of records using Natural Merge (2+1)

import random
import timeit
import sys

NUMBER_OF_RECORDS = 10000 #number of students
BYTES_TO_TAKE = 2048 # normally 4kB=4096
NUMBER_OF_DISK_SAVES = 0
NUMBER_OF_SORTING_PHASES = 0
NUMBER_OF_DISK_READS = -1
GROWING = True

def get_some_records(file):
    with open(file, "r") as f:
        records = f.read(BYTES_TO_TAKE)
        while records:
            yield records
            records = f.read(BYTES_TO_TAKE)

def get_record(file):
    global NUMBER_OF_DISK_READS
    gen = get_some_records(file)
    rest = '' # rest from the records, to be added on the start of the next run
    while True:
        try:
            NUMBER_OF_DISK_READS += 1
            records_from_generator = next(gen)
            if rest != '':
                records_from_generator = rest + records_from_generator
                rest = ''
            while records_from_generator[-1] != ';':
                rest += records_from_generator[-1]
                records_from_generator = records_from_generator[:-1] # delete last char
                if records_from_generator == '':
                    break
            rest = rest[::-1] # reverse string
            list = records_from_generator.split(';')
            for r in list:
                yield r
        except:
            return


# def get_some_records_from_file(file):
#     rest = '' # rest from the records, to be added on the start of the next run
#     with open(file, "r") as f:
#         records = f.read(BYTES_TO_TAKE)
#         while records:
#             if rest != '':
#                 records = rest + records
#                 rest = ''
#             while records[-1] != ';':
#                 rest += records[-1]
#                 records = records[:-1] # delete last char
#                 if records == '':
#                     break
#             rest = rest[::-1] # reverse string
#             list = records.split(';') if records else []
#             yield list
#             records = f.read(BYTES_TO_TAKE)

def write_records_to_file(file_name, records_list):
    if records_list:
        with open(file_name, "a") as file:
            file.write(';'.join(records_list)+';')

def draw_grade():
    return random.choice(['2.0', '3.0', '3.5', '4.0', '4.5', '5.0'])

def create_record_to_file():
    f.write(str(NUMBER_OF_RECORDS)+'/'+draw_grade()+draw_grade()+draw_grade()+';')

def get_record_value(record):
    i = record.find('/')
    value = (int)(record[i+1]) + (int)(record[i+3])/10 + (int)(record[i+4])\
            + (int)(record[i+6])/10 + (int)(record[i+7]) + (int)(record[i+9])/10
    return value

# can be detached to save tape3 and to save tape1 and 2
def add_record_to_tape(tape, record):
    global NUMBER_OF_DISK_SAVES
    tape.append(record)
    if sys.getsizeof(tape1 + tape2 + tape3) >= BYTES_TO_TAKE * 0.97:
        NUMBER_OF_DISK_SAVES += 3
        if tapes_swapped:
            write_records_to_file(tapes[0], tape2)
            write_records_to_file(tapes[1], tape1)
            tape1.clear()
            tape2.clear()
        else:
            write_records_to_file(tapes[0], tape1)
            write_records_to_file(tapes[1], tape2)
            tape1.clear()
            tape2.clear()
        write_records_to_file(tapes[2], tape3)
        tape3.clear()

tapes = ["tape1.txt", "tape2.txt", "tape3.txt"]
# clear tapes
for t in tapes:
    with open(t, "w") as _:
        pass

# change later to binary files .code() and .decode()
with open(tapes[2], "w") as f:
    while NUMBER_OF_RECORDS > 0:
        create_record_to_file()
        NUMBER_OF_RECORDS -= 1

# DEBUGGER FOR EXACT RECORDS
# with open("backup.txt", "r+") as b:
#     with open(tapes[2], "r+") as w:
#         # b.write(w.read())
#         w.write(b.read())
#check sums
# i = 1
# with open("tape3.txt") as tp:
#     list = tp.read().split(';')
#     for r in list:
#         if r != '':
#             print(f'{i}. {get_record_value(r)}')
#             i += 1


# records generated, time to sort

start = timeit.default_timer()
tape1, tape2, tape3 = [], [], []
sorting = True

# SORTING
while sorting:
    tapes_swapped = False
    lval = -1 # last record value
    # clear tapes 1 and 2
    with open(tapes[0], "w") as _:
        pass
    with open(tapes[1], "w") as _:
        pass

    for record in get_record(tapes[2]): # can be done also using next(gen)
        if record != '':
            # INITIAL SORT - disposition from main tape to two tapes
            # count value of record
            val = get_record_value(record)
            # compare with last record in the tape
            # if greater or equal, write to the same tape
            # otherwise, write to another tape
            if val >= lval:
                add_record_to_tape(tape1, record)
            else:
                add_record_to_tape(tape2, record)
                tape1, tape2 = tape2, tape1 # swap tape1 and tape2
                tapes_swapped = not tapes_swapped
            lval = val

    if tapes_swapped:
        tape1, tape2 = tape2, tape1 # unswap tape1 and tape2
        tapes_swapped = False

    # save whats left in tapes 1 and 2
    NUMBER_OF_DISK_SAVES += 2
    write_records_to_file(tapes[0], tape1)
    write_records_to_file(tapes[1], tape2)
    tape1.clear()
    tape2.clear()

    # clear tape3.txt before writing into it
    with open(tapes[2], "w") as _:
        pass

    # MERGING INTO TAPE 3

    lval = -1
    rec1, rec2 = '', ''
    tape1_gen = get_record(tapes[0])
    tape2_gen = get_record(tapes[1])
    xd = 0
    sorting = False # CHANGE TO True IF NOT SORTED (if more than 2 runs to be merged)
    while True:
        if NUMBER_OF_SORTING_PHASES == 1:
            xd += 1
            if xd == 10:
                pass
        # while insted of if because of empty strings because of .split()
        # take new record, only if previous was already used
        while rec1 == '':
            rec1 = next(tape1_gen, -1)
        while rec2 == '':
            rec2 = next(tape2_gen, -1)

        if rec1 == -1 and rec2 == -1:
            break
        elif rec1 == -1:
            add_record_to_tape(tape3, rec2)
            if get_record_value(rec2) < lval:
                sorting = True
            lval = get_record_value(rec2)
            rec2 = ''
            continue
        elif rec2 == -1:
            add_record_to_tape(tape3, rec1)
            if get_record_value(rec1) < lval:
                sorting = True
            lval = get_record_value(rec1)               # lval useless from here actually, can be deleted
            rec1 = ''
            continue

        if get_record_value(rec1) <= get_record_value(rec2):
            lesser, greater = rec1, rec2
        else:
            lesser, greater = rec2, rec1

        if get_record_value(lesser) >= lval:
            add_record_to_tape(tape3, lesser)
            if lesser == rec1:
                lval = get_record_value(rec1)
                rec1 = ''
            else:
                lval = get_record_value(rec2)
                rec2 = ''
        elif get_record_value(greater) >= lval:
            sorting = True
            add_record_to_tape(tape3, greater)
            if greater == rec1:                     # delete used record (put '' instead)
                lval = get_record_value(rec1)
                rec1 = ''
            else:
                lval = get_record_value(rec2)
                rec2 = ''
        else:
            sorting = True
            add_record_to_tape(tape3, lesser)
            if lesser == rec1:
                lval = get_record_value(rec1)
                rec1 = ''
            else:
                lval = get_record_value(rec2)
                rec2 = ''

    # save whats left in tape3
    NUMBER_OF_DISK_SAVES += 1
    write_records_to_file(tapes[2], tape3)
    tape3.clear()
    NUMBER_OF_SORTING_PHASES += 1

stop = timeit.default_timer()
print(f'Time::::::{stop-start}')
print(f'Disk Saves::::::{NUMBER_OF_DISK_SAVES}')
print(f'Disk Reads::::::{NUMBER_OF_DISK_READS}')
print(f'Sorting phases::::::{NUMBER_OF_SORTING_PHASES}')

#check sums
i = 1
old_r = 0
err = 0
with open("tape3.txt") as tp:
    list = tp.read().split(';')
    for r in list:
        if r != '':
            vaa = get_record_value(r)
            #print(f'{i}. {vaa}')
            if vaa < old_r:
                print('           XXXXXX WRONG ERROR !')
                err = 1
            old_r = vaa
            i += 1
pass

#get 2kB to generator and split it to create ready records
#care for last record, since it probably won't be complete
#dispose records to tapes 1 and 2
#merge to 3 and 4
#merge to 1 and 2
#....
#check if finished


#split to split records
